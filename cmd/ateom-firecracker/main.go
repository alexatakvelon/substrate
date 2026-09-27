//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command ateom-firecracker is the Firecracker micro-VM implementation of the
// ateompb.Ateom service, a peer to cmd/ateom-gvisor and cmd/ateom-microvm.
//
// It cold-boots a Firecracker VM with the kata guest (guest kernel + kata rootfs
// image on a virtio-block root device, a tap NIC wired into the interior netns,
// and a virtio-vsock for the kata-agent), then drives the kata-agent directly
// (no kata shim) to configure guest networking and start the actor's containers
// -- each on its own virtio-block ext4 rootfs, since Firecracker has no
// virtio-fs. It implements RunWorkload (cold boot + container start) and
// TerminateWorkload; the remaining Ateom RPCs -- CheckpointWorkload,
// RestoreWorkload, GetWorkloadStats, GetActiveWorkloadStats -- are stubs pending
// later increments (see stubs.go).
//
// Not yet implemented:
//   - suspend/resume via Firecracker snapshot/restore with userfaultfd;
//   - workload stats and actor-container log forwarding.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomcapacity"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/childreap"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	podUID    = flag.String("pod-uid", "", "The UID of the current pod")
	fcBinary  = flag.String("firecracker-binary", "firecracker", "Path to the firecracker binary (fallback when the runtime asset is absent).")
	kataDebug = flag.Bool("kata-debug", false, "Verbose kata guest debugging: raise the guest agent log level and forward the guest console into the pod logs.")

	// guestBootArgs is the kernel command line for the kata guest. It is a flag
	// so it can be tuned to a specific kata rootfs image build (the root device
	// name in particular varies: a whole-disk ext4 image roots at /dev/vda, a
	// partitioned one at /dev/vda1). init=/usr/bin/kata-agent boots the agent as
	// PID 1, which is the guest's entire userspace.
	guestBootArgs = flag.String("guest-boot-args", defaultKataBootArgs, "Kernel command line for the kata guest.")

	vmmMemReserve = flag.Int("vmm-mem-reserve-mib", vmmMemReserveMiB, "Guest RAM (MiB) held back from the pod's memory limit for the firecracker VMM, which runs as a host process in the pod cgroup alongside the guest RAM.")
	showVersion   = flag.Bool("version", false, "Print version and exit.")
	logLevelFlag  = flag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = flag.String("otlp-relay-socket", ateompath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	// Every listen address here is an unspecified wildcard, which Go binds as a
	// dual-stack socket.
	atunnelListenAddress        = flag.String("atunnel-listen-address", ":443", "Address for actor ingress HTTPS")
	atunnelConnectListenAddress = flag.String("atunnel-connect-listen-address", ":8443", "Address for actor ingress mTLS CONNECT")
	workerCredentialBundle      = flag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker Pod credential bundle used by atunnel for inbound serving and outbound mTLS")
	podIdentityTrustBundle      = flag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod identity trust bundle used for router clients and the node-local atelet")
	atunnelClientIdentity       = flag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress  = flag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	egressGatewayTrustBundle    = flag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "Service DNS trust bundle for the remote egress gateway")
	readinessListenAddress      = flag.String("readiness-listen-address", "0.0.0.0:8080", "Address for HTTP readiness checks")
)

// actorHTTPUpstream is the in-guest HTTP endpoint atunnel proxies actor ingress
// to: the guest's eth0 address (see internal/ateomnet).
const actorHTTPUpstream = "http://" + ateomnet.ActorVethIP + ":80"

// reaper collects children orphaned in the pod PID namespace: the detached
// firecracker VMM, and any synchronous subprocess run through it.
var reaper = childreap.New()

func main() {
	flag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()

	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Share one synchronized writer between the runtime logger and the actor-log
	// forwarder so the two streams to the pod's stdout don't interleave-corrupt.
	logWriter := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(logWriter)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		return err
	}
	slog.InfoContext(ctx, "ateom-firecracker booting", slog.String("version", version.Version))

	const serviceName = "ateom-firecracker"
	// Export through atelet's node-local relay when it is there, so telemetry
	// never touches the worker pod's network. A nil conn means it is not, and
	// both providers fall back to dialing the collector directly. A relay that
	// cannot be dialed is logged rather than fatal: what is lost is the
	// node-local export path, not the ateom's ability to run actors.
	relayConn, err := otlprelay.Dial(ctx, *otlpRelaySocket)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to the OTLP relay; exporting telemetry directly over the pod network",
			slog.String("socket", *otlpRelaySocket), slog.Any("err", err))
	}
	if relayConn != nil {
		defer relayConn.Close()
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName:  serviceName,
		Sampling:     serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetricsPushOnlyVia(ctx, serviceName, relayConn)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	// Create ateom dir.
	ateomDir := ateompath.AteomPath(*podUID)
	if err := resources.ValidateAteomUID(*podUID); err != nil {
		return fmt.Errorf("in resources.ValidateAteomUID: %w", err)
	}
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}
	defer func() {
		if err := os.RemoveAll(ateomDir); err != nil {
			slog.ErrorContext(ctx, "Failed to remove the ateom directory on shutdown", slog.Any("err", err))
		}
	}()

	// Reap children reparented to us: the detached firecracker VMM, plus any
	// synchronous subprocess run through reaper.RunCommand.
	go reaper.Run(ctx)
	slog.InfoContext(ctx, "Child process reaper launched")

	// Clean up any old socket.
	sockPath := ateompath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Networking: create a named interior netns. Each activation builds a fresh
	// veth pair into it plus a tap the guest's virtio-net binds to (see net.go);
	// firecracker enters this netns (--netns) to open that tap.
	interiorNetNS, err := ateomnet.CreateNetNSWithoutSwitching(ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating interior netns: %w", err)
	}

	actorLogger := actorlog.NewActorLogger(logWriter, metadata.OnGCE())
	upstream, err := url.Parse(actorHTTPUpstream)
	if err != nil {
		return fmt.Errorf("while parsing atunnel upstream: %w", err)
	}
	atunnelIngress, atunnelEgress, atunnelEgressPort, err := runAtunnel(ctx, upstream)
	if err != nil {
		return err
	}

	ateomService := NewService(*podUID, *fcBinary, *guestBootArgs, *kataDebug, *vmmMemReserve, interiorNetNS, actorLogger, atunnelIngress, atunnelEgress, atunnelEgressPort, *workerCredentialBundle, *podIdentityTrustBundle, *egressGatewayTrustBundle)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}

	// Trap SIGTERM (sent by the kubelet at the start of the pod's termination
	// grace period) and tear the workload down cleanly before the grace period
	// expires. New workload RPCs are rejected with codes.Unavailable while
	// draining (see rejectIfDraining).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		readiness.MarkNotReady()
		ateomService.gracefulShutdown(context.Background())
		svr.GracefulStop()
	}()

	// Report what this worker can supply. Nothing else tells the control plane,
	// which places no Actor here until it lands.
	go func() {
		err := ateomcapacity.Report(ctx, ateomcapacity.ReportConfig{
			SocketPath:           ateompath.AteomSupportSocket,
			CredentialBundlePath: *workerCredentialBundle,
			TrustBundlePath:      *podIdentityTrustBundle,
		})
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to report worker capacity", err)
		}
	}()

	go serverboot.StartReadinessServer(ctx, *readinessListenAddress, readiness)

	slog.InfoContext(ctx, "ateom-firecracker serving", slog.String("socket", sockPath))
	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}
	return nil
}

func runAtunnel(ctx context.Context, upstream *url.URL) (*atunnel.Server, *atunnel.Egress, uint16, error) {
	atunnelIngress, err := atunnel.NewServer(atunnel.Config{
		CredentialBundlePath: *workerCredentialBundle,
		TrustBundlePath:      *podIdentityTrustBundle,
		AllowedClientID:      *atunnelClientIdentity,
		Upstream:             upstream,
	})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while configuring atunnel: %w", err)
	}
	atunnelListener, err := net.Listen("tcp", *atunnelListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.Serve(ctx, atunnelListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel serving", slog.String("address", *atunnelListenAddress))
	atunnelConnectListener, err := net.Listen("tcp", *atunnelConnectListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel CONNECT listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.ServeConnect(ctx, atunnelConnectListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor CONNECT ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel CONNECT serving", slog.String("address", *atunnelConnectListenAddress))

	atunnelEgress, err := atunnel.NewEgress(atunnel.TCPOriginalDestination)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while configuring atunnel egress: %w", err)
	}
	egressListener, err := net.Listen("tcp", *atunnelEgressListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel egress listener: %w", err)
	}
	egressTCPAddr, ok := egressListener.Addr().(*net.TCPAddr)
	if !ok || egressTCPAddr.Port < 1 || egressTCPAddr.Port > 65535 {
		_ = egressListener.Close()
		return nil, nil, 0, fmt.Errorf("atunnel egress listener has invalid address %q", egressListener.Addr())
	}
	atunnelEgressPort := uint16(egressTCPAddr.Port)
	go func() {
		if err := atunnelEgress.Serve(ctx, egressListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor egress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel egress serving", slog.String("address", *atunnelEgressListenAddress))
	return atunnelIngress, atunnelEgress, atunnelEgressPort, nil
}

const (
	rpcRunWorkload     = "RunWorkload"
	rpcRestoreWorkload = "RestoreWorkload"
)

// activeRPCInfo identifies the workload RPC currently holding lock, so graceful
// shutdown can cancel a boot that would otherwise hold it.
type activeRPCInfo struct {
	name   string
	cancel context.CancelFunc
}

// cancelableMutex is a mutex whose acquisition can be abandoned, so graceful
// shutdown does not park forever behind a wedged RPC.
type cancelableMutex struct {
	ch chan struct{}
}

func newCancelableMutex() *cancelableMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &cancelableMutex{ch: ch}
}

func (m *cancelableMutex) Lock()   { <-m.ch }
func (m *cancelableMutex) Unlock() { m.ch <- struct{}{} }

// LockContext acquires the mutex, reporting false if ctx terminates first. On
// false the mutex is NOT held and must not be unlocked.
func (m *cancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// AteomService is the Firecracker implementation of ateompb.AteomServer.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// lock serializes RPCs; like the other ateoms, the run/terminate lifecycle
	// is not safe to drive concurrently.
	lock *cancelableMutex

	// shuttingDown is set once SIGTERM has been received. While true, new
	// workload RPCs are rejected with codes.Unavailable so the control plane
	// reschedules. Atomic so an RPC arriving while gracefulShutdown waits on lock
	// is turned away rather than queued behind it.
	shuttingDown atomic.Bool

	activeRPCMu sync.Mutex
	activeRPC   *activeRPCInfo

	podUID   string
	fcBinary string
	// bootArgs is the guest kernel command line (--guest-boot-args).
	bootArgs  string
	kataDebug bool
	// memReserveMiB is guest RAM (MiB) held back from the pod's memory limit for
	// the firecracker VMM (a host process sharing the pod cgroup with guest RAM).
	memReserveMiB int

	// interiorNetNS hosts the per-activation actor veth peer and the guest tap
	// (see net.go); firecracker is pointed at it.
	interiorNetNS netns.NsHandle

	actorLogger    *actorlog.ActorLogger
	atunnelIngress *atunnel.Server
	atunnelEgress  *atunnel.Egress

	atunnelEgressPort            uint16
	workerCredentialBundlePath   string
	podIdentityTrustBundlePath   string
	egressGatewayTrustBundlePath string

	// running maps actor UID -> the live micro-VM, kept so TerminateWorkload can
	// tear down the same sandbox.
	running map[string]*runningActor

	// activeActor is the actor whose workload this ateom is currently running, or
	// nil when it is "available". Atomic so a stats reader (once implemented) can
	// read it without taking lock, which is held across a whole cold boot.
	activeActor atomic.Pointer[resources.ActorAttribution]
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(podUID, fcBinary, bootArgs string, kataDebug bool, memReserveMiB int, interiorNetNS netns.NsHandle, actorLogger *actorlog.ActorLogger, atunnelIngress *atunnel.Server, atunnelEgress *atunnel.Egress, atunnelEgressPort uint16, workerCredentialBundlePath, podIdentityTrustBundlePath, egressGatewayTrustBundlePath string) *AteomService {
	return &AteomService{
		lock:                         newCancelableMutex(),
		podUID:                       podUID,
		fcBinary:                     fcBinary,
		bootArgs:                     bootArgs,
		kataDebug:                    kataDebug,
		memReserveMiB:                memReserveMiB,
		interiorNetNS:                interiorNetNS,
		actorLogger:                  actorLogger,
		atunnelIngress:               atunnelIngress,
		atunnelEgress:                atunnelEgress,
		atunnelEgressPort:            atunnelEgressPort,
		workerCredentialBundlePath:   workerCredentialBundlePath,
		podIdentityTrustBundlePath:   podIdentityTrustBundlePath,
		egressGatewayTrustBundlePath: egressGatewayTrustBundlePath,
		running:                      map[string]*runningActor{},
	}
}

type actorEgress struct {
	client            *atunnel.Client
	certificateSource *atunnel.BrokerCertificateSource
	expiresAt         time.Time
}

func (s *AteomService) prepareActorEgress(ctx context.Context, actorAtespace, actorName, actorUID string, gateway *ateompb.EgressGateway) (*actorEgress, error) {
	if gateway == nil {
		return nil, nil
	}
	if gateway.GetAddress() == "" {
		return nil, fmt.Errorf("egress gateway address is required")
	}
	serverName, _, err := net.SplitHostPort(gateway.GetAddress())
	if err != nil {
		return nil, fmt.Errorf("invalid egress gateway address %q: %w", gateway.GetAddress(), err)
	}
	certificateSource, err := atunnel.NewBrokerCertificateSource(atunnel.BrokerConfig{
		SocketPath:           ateompath.AteomSupportSocket,
		CredentialBundlePath: s.workerCredentialBundlePath,
		TrustBundlePath:      s.podIdentityTrustBundlePath,
		ActorAtespace:        actorAtespace,
		ActorName:            actorName,
		ActorUID:             actorUID,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor certificate broker: %w", err)
	}
	expiresAt, err := certificateSource.MintAteomCertificate(ctx)
	if err != nil {
		return nil, fmt.Errorf("while obtaining actor certificate: %w", err)
	}
	gatewayClient, err := atunnel.NewClient(atunnel.ClientConfig{
		GatewayAddress:       gateway.GetAddress(),
		ServerName:           serverName,
		GetClientCertificate: certificateSource.GetClientCertificate,
		TrustBundlePath:      s.egressGatewayTrustBundlePath,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor egress client: %w", err)
	}
	return &actorEgress{client: gatewayClient, certificateSource: certificateSource, expiresAt: expiresAt}, nil
}

func (s *AteomService) activateActorNetworking(atespace, actorName string, egress *actorEgress) error {
	if err := s.atunnelIngress.Activate(atespace, actorName); err != nil {
		return fmt.Errorf("while activating actor ingress: %w", err)
	}
	if egress == nil {
		return nil
	}
	if err := s.atunnelEgress.Activate(egress.client, egress.certificateSource, egress.expiresAt); err != nil {
		return fmt.Errorf("while activating actor egress: %w", err)
	}
	return nil
}

func (s *AteomService) deactivateActorNetworking(ctx context.Context) error {
	err := errors.Join(s.atunnelIngress.Deactivate(ctx), s.atunnelEgress.Deactivate(ctx))
	if err != nil {
		return fmt.Errorf("while deactivating actor networking: %w", err)
	}
	return nil
}

// egressRedirectPort returns the local atunnel egress listener port when the
// activation arms tunneled egress, and zero otherwise.
func (s *AteomService) egressRedirectPort(redirectEgress bool) uint16 {
	if !redirectEgress {
		return 0
	}
	return s.atunnelEgressPort
}

// rejectIfDraining returns a codes.Unavailable error if ateom has begun graceful
// shutdown, so the control plane reschedules the actor onto a live worker.
func (s *AteomService) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "worker draining: not accepting new workloads")
	}
	return nil
}

func (s *AteomService) setActiveRPC(name string, cancel context.CancelFunc) {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = &activeRPCInfo{name: name, cancel: cancel}
}

func (s *AteomService) clearActiveRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = nil
}

// cancelActiveRestoreOrRunRPC cancels an in-flight run or restore so it releases
// lock instead of running its boot to completion.
func (s *AteomService) cancelActiveRestoreOrRunRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	if s.activeRPC != nil && (s.activeRPC.name == rpcRestoreWorkload || s.activeRPC.name == rpcRunWorkload) {
		slog.Info("Cancelling in-progress workload startup RPC due to graceful shutdown", slog.String("rpc", s.activeRPC.name))
		s.activeRPC.cancel()
	}
}

package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"

	"go.101.temporal/client/internal/clients/temporal"
	"go.101.temporal/client/internal/handlers"
	"go.101.temporal/common/lifecycle"
	"go.101.temporal/common/temporalx"
	"go.101.temporal/proto/gen/go/com/temporal_101/client"
	sdkclient "go.temporal.io/sdk/client"
	sdkinterceptor "go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Server interface {
	Start() <-chan error
	IsShuttingDown() bool
	Shutdown(ctx context.Context) error
}

type server struct {
	isShuttingDown atomic.Bool
	lifecycle      *lifecycle.Manager
	grpcServer     *grpc.Server
	grpcListener   net.Listener
}

func InitServer(ctx context.Context) (_ Server, err error) {
	s := &server{lifecycle: lifecycle.New()}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.lifecycle.Shutdown(ctx))
		}
	}()

	temporalClient, err := InitTemporalClient(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.lifecycle.AddNamed("temporal client", temporalClient); err != nil {
		return nil, err
	}

	grpcServer, grpcListener, err := InitGrpcServer(temporalClient)
	if err != nil {
		return nil, err
	}
	s.grpcServer = grpcServer
	s.grpcListener = grpcListener
	if err := s.lifecycle.AddNamed("grpc server", grpcServer); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Start() <-chan error {
	errCh := make(chan error, 1)

	go func() {
		slog.Info("Starting GRPC server", "addr", s.grpcListener.Addr().String())
		if err := s.grpcServer.Serve(s.grpcListener); err != nil {
			errCh <- fmt.Errorf("grpc server: %w", err)
		}
	}()

	return errCh
}

func (s *server) IsShuttingDown() bool {
	return s.isShuttingDown.Load()
}

func (s *server) Shutdown(ctx context.Context) error {
	s.isShuttingDown.Store(true)

	if err := s.lifecycle.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}

	return nil
}

func InitTemporalClient(ctx context.Context) (temporal.Client, error) {
	c, err := temporalx.Dial(ctx, sdkclient.Options{
		HostPort: "127.0.0.1:7233",
		Logger:   slog.Default(),
		Interceptors: []sdkinterceptor.ClientInterceptor{
			temporalx.NewLoggingInterceptor(slog.Default()),
		},
	})
	if err != nil {
		return nil, err
	}

	return temporal.NewClient(c), nil
}

func InitGrpcServer(temporalClient temporal.Client) (*grpc.Server, net.Listener, error) {
	addr := net.JoinHostPort("localhost", "8082")
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %w", err)
	}

	healthHandler := health.NewServer()
	handler := handlers.NewHandler(temporalClient)

	grpcServer := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthHandler)
	temporal101client.RegisterClientServiceServer(grpcServer, handler)

	healthHandler.SetServingStatus(
		"com.temporal_101.client.ClientService",
		grpc_health_v1.HealthCheckResponse_SERVING,
	)

	return grpcServer, listener, nil
}

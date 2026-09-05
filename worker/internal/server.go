package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"go.101.temporal/common/lifecycle"
	"go.101.temporal/common/temporalx"
	workflowcontracts "go.101.temporal/contract/workflows"
	"go.101.temporal/worker/internal/handlers"
	"go.101.temporal/worker/internal/workflows"
	sdkclient "go.temporal.io/sdk/client"
)

type Server interface {
	Start() <-chan error
	IsShuttingDown() bool
	Shutdown(ctx context.Context) error
}

type server struct {
	isShuttingDown atomic.Bool
	lifecycle      *lifecycle.Manager
	httpServer     *http.Server
	workerManager  *temporalx.Manager
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

	workerManager, err := InitWorkerManager(ctx, temporalClient)
	if err != nil {
		return nil, err
	}
	s.workerManager = workerManager
	if err := s.lifecycle.AddNamed("worker manager", workerManager); err != nil {
		return nil, err
	}

	httpServer, err := InitHTTPServer(ctx, s.IsShuttingDown)
	if err != nil {
		return nil, err
	}
	s.httpServer = httpServer
	if err := s.lifecycle.AddNamed("http server", httpServer); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Start() <-chan error {
	errCh := make(chan error, 1)

	slog.Info("Starting worker manager")
	if err := s.workerManager.Start(); err != nil {
		errCh <- fmt.Errorf("failed to start worker: %w", err)
		return errCh
	}

	go func() {
		slog.Info("Starting HTTP server", "addr", s.httpServer.Addr)
		err := s.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
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

func InitTemporalClient(ctx context.Context) (sdkclient.Client, error) {
	return temporalx.Dial(ctx, sdkclient.Options{
		HostPort: "127.0.0.1:7233",
		Logger:   slog.Default(),
	})
}

func InitWorkerManager(_ context.Context, client sdkclient.Client) (*temporalx.Manager, error) {
	return temporalx.NewManager(client,
		temporalx.ForQueue(workflowcontracts.HelloQueue,
			// GreetSomeone
			temporalx.BindWorkflow(workflowcontracts.GreetSomeone, workflows.GreetSomeone),

			// Order
			temporalx.BindWorkflow(workflowcontracts.Order, workflows.Order),
			temporalx.BindActivity(workflows.GenerateOrderID),
			temporalx.BindActivity(workflows.ProcessItem),
			temporalx.BindActivity(workflows.SaveOrder),

			// Long running
			temporalx.BindWorkflow(workflowcontracts.LongRunning, workflows.LongRunning),
			temporalx.BindActivity(workflows.PrepareLongRunning),
			temporalx.BindActivity(workflows.FinishLongRunning),
			temporalx.BindActivity(workflows.PrepareLongRunningV2),
			temporalx.BindActivity(workflows.FinishLongRunningV2),
		).WithInterceptors(temporalx.NewLoggingInterceptor(slog.Default())),
	)
}

func InitHTTPServer(_ context.Context, downFunc handlers.IsShuttingDownFunc) (*http.Server, error) {
	httpServer := http.Server{
		Addr:    ":8081",
		Handler: handlers.InitHTTPHandler(downFunc),
	}
	return &httpServer, nil
}

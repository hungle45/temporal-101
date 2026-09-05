package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.101.temporal/common/log"
	"go.101.temporal/worker/internal"
)

func main() {
	os.Exit(run())
}
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Init("worker")

	slog.InfoContext(ctx, "Starting server")

	server, err := internal.InitServer(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Error initializing server", slog.String("error", err.Error()))
		return 1
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.ErrorContext(shutdownCtx, "Failed to shutdown server", slog.String("error", err.Error()))
		}
	}()

	errCh := server.Start()
	slog.InfoContext(ctx, "Server started")

	select {
	case err := <-errCh:
		slog.ErrorContext(ctx, "Server failed", slog.String("error", err.Error()))
		return 1
	case <-ctx.Done():
		slog.Info("Server stopping")
	}

	return 0
}

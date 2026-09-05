package log

import (
	"log/slog"
	"os"
	"strings"
	"time"
)

func Init(serviceName string) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.String(
					slog.TimeKey,
					a.Value.Time().Format(time.RFC3339),
				)
			}
			return a
		},
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts)).
		With(slog.String("pod", strings.ToUpper(serviceName)))
	slog.SetDefault(logger)
}

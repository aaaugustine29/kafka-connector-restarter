package logging

import (
	"log/slog"
	"os"
)

func Configure(level slog.Leveler) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

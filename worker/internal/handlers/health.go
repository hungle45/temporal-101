package handlers

import (
	"net/http"
)

// Health is the liveness probe.
func Health(isShuttingDown func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if isShuttingDown() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

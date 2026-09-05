package handlers

import "net/http"

type IsShuttingDownFunc func() bool

func InitHTTPHandler(downFunc IsShuttingDownFunc) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/healthz", Health(downFunc))

	return mux
}

package httpconfig

import (
	"net/http"
	"time"
)

const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	IdleTimeout       = 60 * time.Second
	MaxHeaderBytes    = 1 << 20
)

func Apply(server *http.Server) {
	server.ReadHeaderTimeout = ReadHeaderTimeout
	server.ReadTimeout = ReadTimeout
	server.WriteTimeout = WriteTimeout
	server.IdleTimeout = IdleTimeout
	server.MaxHeaderBytes = MaxHeaderBytes
}

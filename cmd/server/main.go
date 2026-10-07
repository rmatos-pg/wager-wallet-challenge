package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/fx"

	"wager-wallet-service/internal/app"
	"wager-wallet-service/internal/config"
)

func main() {
	// The distroless image has no curl: "server healthcheck" is the Docker
	// HEALTHCHECK command.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	fx.New(app.Options(cfg)).Run()
}

func healthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/health/ready")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

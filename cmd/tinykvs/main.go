package main

import (
	"os"
	"strings"
)

// Version info - set via ldflags at build time
// go build -ldflags "-X main.Version=1.0.0 -X main.GitCommit=$(git rev-parse --short HEAD)"
var (
	Version   = "dev"
	GitCommit = "unknown"
)

func main() {
	os.Exit(NewCLI().Run(os.Args))
}

func versionString() string {
	if strings.Contains(Version, GitCommit) {
		return Version
	}
	return Version + " (" + GitCommit + ")"
}

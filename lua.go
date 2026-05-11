package gotsem

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/aquire.lua
var acquireScript string

//go:embed scripts/release.lua
var releaseScript string

func LoadAquire() redis.Script {
	return *redis.NewScript(acquireScript)
}

func LoadRelease() redis.Script {
	return *redis.NewScript(releaseScript)
}

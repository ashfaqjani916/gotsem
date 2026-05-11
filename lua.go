package gotsem

import (
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func LoadAquire() (redis.Script, error) {
	luaAcquire, err := os.ReadFile("scripts/aquire.lua")
	if err != nil {
		return redis.Script{}, fmt.Errorf("load acquire script: %w", err)
	}
	return *redis.NewScript(string(luaAcquire)), nil
}

func LoadRelease() (redis.Script, error) {
	luaRelease, err := os.ReadFile("scripts/release.lua")
	if err != nil {
		return redis.Script{}, fmt.Errorf("load release script: %w", err)
	}
	return *redis.NewScript(string(luaRelease)), nil
}

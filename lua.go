package gotsem

import (
	"os"

	"github.com/redis/go-redis/v9"
)

func LoadAquire() (redis.Script, error) {
	luaAcquire, err := os.ReadFile("scripts/aquire.lua")
	if err != nil {
		panic(err)
	}
	return *redis.NewScript(string(luaAcquire)), nil
}

func LoadRelease() (redis.Script, error) {
	luaRelease, err := os.ReadFile("scripts/release.lua")
	if err != nil {
		panic(err)
	}
	return *redis.NewScript(string(luaRelease)), nil
}

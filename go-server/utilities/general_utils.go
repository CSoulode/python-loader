package utilities

import (
	"log"
	"os"
	"strconv"
)

func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	return value
}

func MustGetEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

func ConditionalAssignInt(condition bool, optionTrue int, optionFalse int) int {
	if condition {
		return optionTrue
	}
	return optionFalse
}
func ConditionalAssignString(condition bool, optionTrue string, optionFalse string) string {
	if condition {
		return optionTrue
	}
	return optionFalse
}

func TernaryStr(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func TryFind[T any](arr []T, predicate func(T) bool) (T, bool) {
	var zero T
	for _, v := range arr {
		if predicate(v) {
			return v, true
		}
	}
	return zero, false
}

func Where[T any](arr []T, predicate func(T) bool) []T {
	var result []T
	for _, v := range arr {
		if predicate(v) {
			result = append(result, v)
		}
	}
	return result
}

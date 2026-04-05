package runner

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

type ServerDBEnv struct {
	DBName     string
	DBUser     string
	DBPassword string
	DBHost     string
	DBPort     string
}

func ParseServerDBEnv(databaseURL string) (ServerDBEnv, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return ServerDBEnv{}, err
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return ServerDBEnv{}, err
	}
	password, _ := parsed.User.Password()
	return ServerDBEnv{
		DBName:     strings.TrimPrefix(parsed.Path, "/"),
		DBUser:     parsed.User.Username(),
		DBPassword: password,
		DBHost:     host,
		DBPort:     port,
	}, nil
}

type RuntimePorts struct {
	VectorGRPC int
	VectorHTTP int
	ServerGRPC int
	ServerHTTP int
}

func PortsForDataset(dataset DatasetConfig) RuntimePorts {
	slotOffset := dataset.PortSlot * 10
	return RuntimePorts{
		VectorGRPC: 19000 + slotOffset,
		VectorHTTP: 19100 + slotOffset,
		ServerGRPC: 15051 + slotOffset,
		ServerHTTP: 18080 + slotOffset,
	}
}

func ServerBinaryPath(paths OutputPaths) string {
	return filepath.Join(paths.ScratchDir, "server")
}

func VectorBinaryPath(paths OutputPaths) string {
	return filepath.Join(paths.ScratchDir, "kvserver")
}

func IntString(value int) string {
	return strconv.Itoa(value)
}

func BuildGoServerEnv(
	base map[string]string,
	dbEnv ServerDBEnv,
	ports RuntimePorts,
	vectorAddr string,
	overrides map[string]string,
) []string {
	env := cloneEnvMap(base)
	env["DB_NAME"] = dbEnv.DBName
	env["DB_USER"] = dbEnv.DBUser
	env["DB_PASSWORD"] = dbEnv.DBPassword
	env["DB_HOST"] = dbEnv.DBHost
	env["DB_PORT"] = dbEnv.DBPort
	env["SV_HOST"] = "127.0.0.1"
	env["SV_PORT"] = IntString(ports.ServerGRPC)
	env["HTTP_PORT"] = IntString(ports.ServerHTTP)
	env["VECTORKV_ADDR"] = vectorAddr
	for key, value := range overrides {
		env[key] = value
	}
	return flattenEnvMap(env)
}

func BuildVectorEnv(databaseURL string, ports RuntimePorts, overrides map[string]string) []string {
	env := cloneEnvMap(overrides)
	env["DATABASE_URL"] = databaseURL
	env["KV_ADDR"] = fmt.Sprintf("127.0.0.1:%d", ports.VectorGRPC)
	env["KV_METRICS_ADDR"] = fmt.Sprintf("127.0.0.1:%d", ports.VectorHTTP)
	return flattenEnvMap(env)
}

func cloneEnvMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func flattenEnvMap(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out
}

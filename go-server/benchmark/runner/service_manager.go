package runner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type RuntimeConfig struct {
	Dataset              DatasetConfig
	DatabaseURL          string
	VectorEnv            map[string]string
	GoServerBaseEnv      map[string]string
	GoServerEnvOverrides map[string]string
	Paths                OutputPaths
	RootDir              string
}

type ManagedRuntime struct {
	Ports         RuntimePorts
	VectorLogPath string
	ServerLogPath string
	vectorCmd     *exec.Cmd
	serverCmd     *exec.Cmd
}

func BuildBinaries(ctx context.Context, root string, paths OutputPaths) error {
	if err := EnsureDir(paths.ScratchDir); err != nil {
		return err
	}
	vectorDir := filepath.Join(root, "vectorkv")
	if err := runCommand(ctx, vectorDir, "go", "build", "-o", VectorBinaryPath(paths), "./cmd/kvserver"); err != nil {
		return err
	}
	serverDir := filepath.Join(root, "python-loader", "go-server")
	return runCommand(ctx, serverDir, "go", "build", "-o", ServerBinaryPath(paths), "./server")
}

func StartManagedRuntime(ctx context.Context, cfg RuntimeConfig) (*ManagedRuntime, error) {
	ports := PortsForDataset(cfg.Dataset)
	runtime := &ManagedRuntime{
		Ports:         ports,
		VectorLogPath: filepath.Join(cfg.Paths.LogDir, fmt.Sprintf("%s-vectorkv.log", cfg.Dataset.Label())),
		ServerLogPath: filepath.Join(cfg.Paths.LogDir, fmt.Sprintf("%s-go-server.log", cfg.Dataset.Label())),
	}

	if err := EnsureDir(cfg.Paths.LogDir); err != nil {
		return nil, err
	}
	vectorAddr := fmt.Sprintf("127.0.0.1:%d", ports.VectorGRPC)
	vectorEnv := BuildVectorEnv(cfg.DatabaseURL, ports, cfg.VectorEnv)
	vectorCmd, err := startLoggedProcess(VectorBinaryPath(cfg.Paths), cfg.RootDir, runtime.VectorLogPath, vectorEnv)
	if err != nil {
		return nil, err
	}
	runtime.vectorCmd = vectorCmd
	if err := waitForVectorReady(ctx, vectorAddr); err != nil {
		runtime.Stop()
		return nil, err
	}

	dbEnv, err := ParseServerDBEnv(cfg.DatabaseURL)
	if err != nil {
		runtime.Stop()
		return nil, err
	}
	serverEnv := BuildGoServerEnv(cfg.GoServerBaseEnv, dbEnv, ports, vectorAddr, cfg.GoServerEnvOverrides)
	serverCmd, err := startLoggedProcess(ServerBinaryPath(cfg.Paths), cfg.RootDir, runtime.ServerLogPath, serverEnv)
	if err != nil {
		runtime.Stop()
		return nil, err
	}
	runtime.serverCmd = serverCmd
	if err := waitForHTTPReady(ctx, fmt.Sprintf("http://127.0.0.1:%d/api/vector/models", ports.ServerHTTP)); err != nil {
		runtime.Stop()
		return nil, err
	}
	return runtime, nil
}

func (r *ManagedRuntime) Stop() {
	stopProcess(r.serverCmd)
	stopProcess(r.vectorCmd)
}

func startLoggedProcess(binary string, workdir string, logPath string, env []string) (*exec.Cmd, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(binary)
	cmd.Dir = workdir
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	return cmd, nil
}

func stopProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
}

func runCommand(ctx context.Context, workdir string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = workdir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w\n%s", name, args, err, string(output))
	}
	return nil
}

func waitForVectorReady(ctx context.Context, addr string) error {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	return retry(ctx, 30, 500*time.Millisecond, func() error {
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	})
}

func waitForHTTPReady(ctx context.Context, endpoint string) error {
	return retry(ctx, 30, 500*time.Millisecond, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	})
}

func retry(ctx context.Context, attempts int, interval time.Duration, fn func() error) error {
	var lastErr error
	for index := 0; index < attempts; index++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return lastErr
}

package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run the real main in a child process so flags, signals, and os.Exit stay isolated.
func TestMainProcess(t *testing.T) {
	if os.Getenv("KCH_TEST_MAIN_PROCESS") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child-process argument separator")
	}
	os.Args = append([]string{os.Args[0]}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

func mainProcess(ctx context.Context, arguments ...string) *exec.Cmd {
	arguments = append([]string{"-test.run=^TestMainProcess$", "--"}, arguments...)
	command := exec.CommandContext(ctx, os.Args[0], arguments...)
	command.Env = append(os.Environ(), "KCH_TEST_MAIN_PROCESS=1")
	return command
}

func TestMainStartupAndShutdown(t *testing.T) {
	emptyClusters := filepath.Join(t.TempDir(), "clusters.yaml")
	if err := os.WriteFile(emptyClusters, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		arguments []string
		want      string
	}{
		{name: "defaults", want: "polling started"},
		{name: "explicit empty clusters", arguments: []string{"--connect-clusters=" + emptyClusters}, want: "no Connect clusters configured; polling disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := mainProcess(ctx, test.arguments...)
			output, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			command.Stderr = command.Stdout
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				cancel()
				if !waited {
					command.Wait()
				}
			})
			ready := make(chan struct{}, 1)
			finishedOutput := make(chan struct{})
			var logs strings.Builder
			go func() {
				defer close(finishedOutput)
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					line := scanner.Text()
					logs.WriteString(line + "\n")
					if strings.Contains(line, "starting API server") {
						ready <- struct{}{}
					}
				}
			}()
			select {
			case <-ready:
			case <-finishedOutput:
				t.Fatalf("application exited before API startup: %s", logs.String())
			case <-ctx.Done():
				t.Fatal("application did not start")
			}
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			<-finishedOutput
			err = command.Wait()
			waited = true
			if err != nil {
				t.Fatalf("application did not shut down cleanly: %v\n%s", err, logs.String())
			}
			if !strings.Contains(logs.String(), test.want) || !strings.Contains(logs.String(), "Kafka Connect Healer stopped") {
				t.Fatalf("missing expected startup or shutdown logs: %s", logs.String())
			}
			if test.name == "explicit empty clusters" && strings.Contains(logs.String(), "polling started") {
				t.Fatalf("empty cluster file started a poller: %s", logs.String())
			}
		})
	}
}

func TestMainRejectsInvalidSuppliedFiles(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalid, []byte("unexpectedField: true"), 0600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, arguments := range [][]string{
		{"--config=" + invalid}, {"--config=" + missing},
		{"--connect-clusters=" + invalid}, {"--connect-clusters=" + missing},
	} {
		t.Run(arguments[0], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			output, err := mainProcess(ctx, arguments...).CombinedOutput()
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
				t.Fatalf("startup error = %v, want exit code 1; logs: %s", err, output)
			}
			if !strings.Contains(string(output), "configuration loading failed") || strings.Contains(string(output), "starting API server") || strings.Contains(string(output), "polling started") {
				t.Fatalf("invalid supplied file did not fail before starting workers: %s", output)
			}
		})
	}
}

# Contributing

Kafka Connect Healer is maintained by Entropic Works, Inc. Bug reports and focused
pull requests are welcome at https://github.com/Entropic-Works/kafka-connect-healer.
Discuss larger behavior or API changes in an issue before implementing them.

For bugs, include the version/image tag, relevant configuration with credentials
removed, Kafka Connect version, reproduction steps, and sanitized logs. Do not post
security vulnerabilities publicly; follow [SECURITY.md](SECURITY.md).

## Development

Use the Go version in `go.mod`. Application code lives under `internal/`, and the
entrypoint is `cmd/kafka-connect-healer`. Follow the existing style, use `gofmt`,
keep changes focused, and add tests for changed behavior. Update the README and
examples when configuration or API behavior changes.

```sh
gofmt -w cmd internal
go mod verify
go build ./...
go vet ./...
go test -race -shuffle=on -count=3 -timeout=5m ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
docker build -t kafka-connect-healer:local .
bash scripts/docker-smoke-test.sh kafka-connect-healer:local
```

The container smoke test requires Docker, Bash, curl, jq, tar, and diff. Go tests
start local HTTP servers. Vendored dependencies are checked in; after intentionally
changing dependencies, run `go mod tidy` and `go mod vendor`. Do not edit `vendor/`
by hand. The vulnerability check downloads its tool and accesses the Go vulnerability
database, even though application dependencies are vendored.

Contributions are submitted under the project's Apache-2.0 license. Contributors
retain ownership of their contributions; submitting a pull request does not assign
copyright to Entropic Works. Only submit code you have the right to contribute, and
preserve third-party license notices.

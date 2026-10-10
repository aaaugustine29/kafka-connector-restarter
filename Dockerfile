# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod ./
COPY vendor/ ./vendor/
COPY cmd/ ./cmd/
COPY internal/ ./internal/

ARG TARGETOS
ARG TARGETARCH
RUN --network=none --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=vendor -trimpath -buildvcs=false -ldflags="-s -w" \
    -o /out/kafka-connect-healer ./cmd/kafka-connect-healer

FROM scratch

COPY --from=build /out/kafka-connect-healer /kafka-connect-healer
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/go/LICENSE /licenses/go-LICENSE
COPY LICENSE /licenses/kafka-connect-healer-LICENSE
COPY vendor/go.yaml.in/yaml/v3/LICENSE vendor/go.yaml.in/yaml/v3/NOTICE /licenses/yaml/

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/kafka-connect-healer"]

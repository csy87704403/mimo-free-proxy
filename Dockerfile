FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY main.go ./

RUN apk add --no-cache ca-certificates
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mimo-free-proxy .

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/mimo-free-proxy /mimo-free-proxy

ENV HOST=0.0.0.0
ENV PORT=39173
ENV UPSTREAM_BASE=https://api.xiaomimimo.com
ENV MAX_429_WAIT_MS=180000
ENV DEFAULT_MODEL=mimo-auto
ENV CLIENT_FILE=/data/client
ENV GOMEMLIMIT=24MiB
ENV GOGC=50

EXPOSE 39173
VOLUME ["/data"]

ENTRYPOINT ["/mimo-free-proxy"]

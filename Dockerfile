FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -tags legacy -trimpath -ldflags="-s -w" -o /out/mimo-free-proxy main.go

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

ENV HOST=0.0.0.0
ENV PORT=39173
ENV UPSTREAM_BASE=https://api.xiaomimimo.com
ENV MAX_429_WAIT_MS=180000
ENV DEFAULT_MODEL=mimo-auto
ENV CLIENT_FILE=/data/client
ENV JWT_FILE=/data/jwt
ENV GOMEMLIMIT=24MiB
ENV GOGC=50

WORKDIR /app
COPY --from=build /out/mimo-free-proxy /app/mimo-free-proxy

EXPOSE 39173
VOLUME ["/data"]

ENTRYPOINT ["/app/mimo-free-proxy"]

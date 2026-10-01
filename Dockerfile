FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raft-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raft-client ./cmd/client
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raft-bench ./cmd/bench

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /bin/raft-server /usr/local/bin/raft-server
COPY --from=builder /bin/raft-client /usr/local/bin/raft-client
COPY --from=builder /bin/raft-bench /usr/local/bin/raft-bench

RUN mkdir -p /var/lib/raft

ENTRYPOINT ["raft-server"]

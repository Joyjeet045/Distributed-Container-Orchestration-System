FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/apiserver ./cmd/apiserver

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/apiserver /app/apiserver
EXPOSE 8080 9090
ENTRYPOINT ["/app/apiserver"]

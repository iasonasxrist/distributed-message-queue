# Build the Go producer, then ship only the binary
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /producer .

FROM gcr.io/distroless/static-debian12
COPY --from=build /producer /producer

# Same port inside the container as the Python producer
EXPOSE 5000

ENTRYPOINT ["/producer"]

# Build the Go broker, then ship only the binary
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /broker .

FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=build /broker /broker

# Same port inside the container as the Python broker
EXPOSE 5000

ENTRYPOINT ["/broker"]

# Use the official Golang image as a builder
FROM golang:1.19 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the application code and config directory
COPY . .

# Build the Go application with static linking
RUN CGO_ENABLED=0 GOOS=linux go build -o app .

# Use a minimal runtime image
FROM alpine:latest
WORKDIR /root/

# Copy the built binary and configuration files from the builder stage
COPY --from=builder /app/app .
COPY --from=builder /app/config ./config


# Set the entry point to the compiled Go binary
ENTRYPOINT ["./app"]

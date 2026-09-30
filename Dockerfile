# Build stage. Dependencies are downloaded before the source is copied, so an
# edit to the code does not invalidate the module cache.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/anchora ./cmd/anchora

# Runtime stage. The binary is static and reads no files but its configuration,
# so it needs nothing from a distribution but CA certificates for the provider
# it calls over HTTPS.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/anchora /usr/local/bin/anchora
COPY config.yaml /app/config.yaml
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/anchora"]
CMD ["-config", "/app/config.yaml"]

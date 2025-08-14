FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/brokerd ./cmd/brokerd

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /out/brokerd /app/brokerd
COPY --chown=nonroot:nonroot config/broker.yaml /app/config/broker.yaml
COPY --chown=nonroot:nonroot schemas /app/schemas

USER nonroot:nonroot

EXPOSE 8080

VOLUME ["/app/data"]

ENTRYPOINT ["/app/brokerd"]
CMD ["-config", "/app/config/broker.yaml"]

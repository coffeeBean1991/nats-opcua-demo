FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /out/opcua-connector .

FROM alpine:3.20
COPY --from=build /out/opcua-connector /usr/local/bin/opcua-connector
ENTRYPOINT ["/usr/local/bin/opcua-connector"]

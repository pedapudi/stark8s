FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/controller ./cmd/controller \
 && CGO_ENABLED=0 go build -o /out/exchange ./cmd/exchange \
 && CGO_ENABLED=0 go build -o /out/wordcount ./examples/wordcount \
 && CGO_ENABLED=0 go build -o /out/pagerank ./examples/pagerank

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /
ENTRYPOINT ["/controller"]

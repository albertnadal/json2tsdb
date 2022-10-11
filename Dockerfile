FROM golang:1.19.2 AS builder
WORKDIR /go/src/github.com/albertnadal/json2tsdb/ 
COPY main.go go.mod go.sum entrypoint.sh ./
COPY tsdb ./tsdb/
RUN CGO_ENABLED=0 go build -a -installsuffix cgo -o json2tsdb .

FROM prom/prometheus
WORKDIR /app
COPY --from=builder /go/src/github.com/albertnadal/json2tsdb/json2tsdb /app
COPY entrypoint.sh /app
USER       nobody
EXPOSE     9090
VOLUME     [ "/prometheus" ]
ENTRYPOINT ["./entrypoint.sh"]
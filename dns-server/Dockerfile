FROM golang:1.21 AS builder

WORKDIR /go/src/dns-server/
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on \
    go build -o /opt/dns-server

FROM alpine
COPY --from=builder /opt/dns-server /dns-server
COPY dns-records.txt /dns-records.txt
EXPOSE 53

CMD ["/dns-server"]

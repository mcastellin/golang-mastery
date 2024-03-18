FROM golang:1.22 AS builder

WORKDIR /go/src/app/
COPY . .

RUN rm -f go.work \
    && CGO_ENABLED=0 GOOS=linux GO111MODULE=on \
    go build -o /opt/distributed-queue

FROM scratch
COPY --from=builder /opt/distributed-queue /distributed-queue

EXPOSE 8080

CMD ["/distributed-queue"]

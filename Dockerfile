FROM golang:1.17 as builder
WORKDIR /go/src/github.com/fhmq/hmq
COPY . .
RUN CGO_ENABLED=0 go build -o hmq -a -ldflags '-extldflags "-static"' .


FROM alpine
WORKDIR /
COPY --from=builder /go/src/github.com/fhmq/hmq/hmq .
EXPOSE 1883

CMD ["/hmq"]
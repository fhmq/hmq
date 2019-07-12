FROM golang:1.12
WORKDIR /go/src/github.com/fhmq/hmq
COPY . .
COPY ./vendor .
RUN CGO_ENABLED=0 go build -o thing_model -a -ldflags '-extldflags "-static"' .


FROM alpine:3.8
WORKDIR /
COPY --from=builder /go/src/github.com/fhmq/hmq/hmq .
EXPOSE 50010

CMD ["/hmq"]
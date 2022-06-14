FROM golang:1.18 as builder
ENV WD=/go/src/hmq
WORKDIR ${WD}
COPY . .
RUN CGO_ENABLED=0 go build -o hmq -a -ldflags '-extldflags "-static"' .


FROM alpine
WORKDIR /
COPY --from=builder ${WD} .
EXPOSE 1883

ENTRYPOINT ["/hmq"]

FROM golang:1.16 AS builder
WORKDIR /build
COPY . /build
RUN go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct && \
  go env -w GOSUMDB=off && go env
RUN CGO_ENABLED=0 go build -o ./dist/hmq main.go

FROM alpine AS hmq
COPY --from=builder /build/dist/hmq /usr/bin/hmq
RUN echo "http://mirrors.aliyun.com/alpine/v3.10/main/" > /etc/apk/repositories \
    && apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
CMD ["hmq", "-c", "/etc/hmq/hmq.config"]
FROM golang:1.27.3 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -buildvcs=false -ldflags='-s -w' -o /out/hmq .
RUN mkdir -p /out/rootfs/data /out/rootfs/tmp && chmod 1777 /out/rootfs/tmp

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chown=65532:65532 /out/rootfs/ /
COPY --from=builder /out/hmq /hmq
WORKDIR /data
USER 65532:65532
EXPOSE 1883
ENTRYPOINT ["/hmq"]
CMD ["--host", "0.0.0.0", "--port", "1883"]

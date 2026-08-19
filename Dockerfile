FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/s3warm ./cmd/fakebee

FROM alpine:3.21
RUN adduser -D -H s3warm && mkdir /data && chown s3warm /data
COPY --from=build /out/s3warm /out/fakebee /usr/local/bin/
USER s3warm
ENV S3WARM_DB=/data/s3warm.db
VOLUME /data
EXPOSE 8333
HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
  CMD wget -q -O- http://127.0.0.1:8333/_s3warm/ready || exit 1
ENTRYPOINT ["s3warm"]

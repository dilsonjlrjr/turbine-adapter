# Build context: the adapter-src root (needs every module plus go.work).
#   docker build -f examples/lab/deploy/Dockerfile .
FROM golang:1.26 AS build
ENV GONOSUMDB=github.com/dilsonjlrjr \
    GOPROXY=direct,https://proxy.golang.org \
    CGO_ENABLED=0
WORKDIR /src
COPY . .
WORKDIR /src/examples
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/lab ./lab

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 app \
    && mkdir /pb_data && chown app /pb_data
COPY --from=build /out/lab /usr/local/bin/lab
USER app
VOLUME /pb_data
EXPOSE 8090
ENV TURBINE_DATA_DIR=/pb_data
ENTRYPOINT ["lab"]
CMD ["serve", "--http=0.0.0.0:8090"]

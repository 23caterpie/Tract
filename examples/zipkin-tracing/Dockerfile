FROM --platform=$BUILDPLATFORM golang:1.18beta1-stretch AS stage1

WORKDIR /var/build/go
ARG TARGETARCH
ENV GOARCH=$TARGETARCH
ADD ./ ./
RUN cd examples/zipkin-tracing && \
    go mod vendor && \
    go build -v -mod=vendor -o /var/build/bin/ ./...

# STAGE 2
# Prepare the base image
FROM debian:stretch AS stage2 

RUN apt-get update -qq --fix-missing && \
    apt-get install -yqq ca-certificates curl tzdata && \
    apt-get clean

# STAGE 3
# Construct the final image
FROM stage2 AS stage3

COPY --from=stage1 /var/build/bin/* /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/zipkin-tracing"]

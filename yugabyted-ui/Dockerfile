# Build stage
FROM --platform=linux/amd64 golang:1.23.6 AS builder

ENV PATH="/usr/local/go/bin:${PATH}"
ENV API_SERVER_DIR=/build/yugabyted-ui/apiserver/cmd/server
ENV OUTPUT_FILE=/yugabyted-ui

WORKDIR /build/yugabyted-ui

COPY apiserver/ apiserver/

# Build the API server
RUN mkdir -p ${OUTPUT_FILE}
RUN cd ${API_SERVER_DIR} && \
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${OUTPUT_FILE}

########################################################
# TODO: use alpine?
FROM --platform=linux/amd64 debian:bullseye-slim

ENV TINI_VERSION=v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

COPY --from=builder ${OUTPUT_FILE} /usr/local/bin/
RUN chmod +x /usr/local/bin/yugabyted-ui/server

EXPOSE 15433

ENTRYPOINT ["/tini", "--"]

FROM ghcr.io/apache/hadoop-runner:jdk11-u2204

ARG HADOOP_VERSION=3.4.3
ARG BASE_URL=https://dlcdn.apache.org/hadoop/common
ARG TARGETPLATFORM

WORKDIR /opt/hadoop

RUN set -eux; \
    case "${TARGETPLATFORM}" in \
        linux/amd64) HADOOP_ARCH='' ;; \
        linux/arm64) HADOOP_ARCH='-aarch64' ;; \
        *) HADOOP_ARCH='' ;; \
    esac; \
    export HADOOP_URL="${BASE_URL}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}${HADOOP_ARCH}.tar.gz"; \
    curl -LSs "$HADOOP_URL" | tar -x -z --strip-components 1 && rm -rf /opt/hadoop/share/doc

ADD log4j.properties /opt/hadoop/etc/hadoop/log4j.properties
RUN sudo chown -R hadoop:users /opt/hadoop/etc/hadoop/*

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

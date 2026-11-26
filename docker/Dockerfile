# Base images pinned by digest (multi-arch index of the tag named before @), so a rebuild
# gets the same bytes. Bump both digests on purpose, e.g. for security updates.
FROM eclipse-temurin:11-jre@sha256:7db5776af2396377c8aae2aa124712da7b72a5b6b014c17d77ae37eb5276f37b AS jre

FROM python:3.11-slim-bookworm@sha256:ef56ae98435cc40d8a27020b0b32a9941515acd5840f0fba34858bb34dd362d1 AS hadoop
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG HADOOP_VERSION=3.3.6
ARG HADOOP_SHA512=de3eaca2e0517e4b569a88b63c89fae19cb8ac6c01ff990f1ff8f0cc0f3128c8e8a23db01577ca562a0e0bb1b4a3889f8c74384e609cd55e537aada3dcaa9f8a
COPY docker/fetch.py /usr/local/bin/fetch.py
RUN python /usr/local/bin/fetch.py \
        "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        "${HADOOP_SHA512}" /tmp/hadoop.tgz \
    && mkdir -p /opt/hadoop \
    && tar -xzf /tmp/hadoop.tgz -C /opt/hadoop --strip-components=1 \
    && rm -rf /tmp/hadoop.tgz /opt/hadoop/share/doc \
        /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar \
        /opt/hadoop/share/hadoop/tools/lib/azure-*.jar \
        /opt/hadoop/share/hadoop/tools/lib/aliyun-*.jar \
        /opt/hadoop/share/hadoop/tools/lib/cos_api-bundle-*.jar

FROM python:3.11-slim-bookworm@sha256:ef56ae98435cc40d8a27020b0b32a9941515acd5840f0fba34858bb34dd362d1 AS spark
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG SPARK_VERSION=3.5.3
ARG SPARK_SHA512=76d6edc1050629c0be0f82543d7da36f7c27ef2e7978e28c20bb5b9e7677c7c8b485af3c6a34319f402706592a1b381ad8ec96458104b23814c871e2b4870eab
COPY docker/fetch.py /usr/local/bin/fetch.py
RUN python /usr/local/bin/fetch.py \
        "${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" \
        "${SPARK_SHA512}" /tmp/spark.tgz \
    && mkdir -p /opt/spark \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm -rf /tmp/spark.tgz /opt/spark/examples /opt/spark/data /opt/spark/R /opt/spark/kubernetes

FROM python:3.11-slim-bookworm@sha256:ef56ae98435cc40d8a27020b0b32a9941515acd5840f0fba34858bb34dd362d1
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/var/log/hadoop \
    SPARK_HOME=/opt/spark \
    SPARK_CONF_DIR=/opt/spark/conf \
    SPARK_LOG_DIR=/var/log/spark \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYTHONDONTWRITEBYTECODE=1 \
    HADOOP_STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    GTDP_IN_IMAGE=1
ENV PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${PATH}

# Spark's launch scripts call ps. Debian security updates replace pinned versions, so no pin.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=hadoop /opt/hadoop /opt/hadoop
COPY --from=spark /opt/spark /opt/spark

# Same users and groups on every node: HDFS resolves groups with `id` on the NameNode.
RUN groupadd -g 1000 hadoop \
    && groupadd -g 1001 datalake \
    && groupadd -g 1002 analysts \
    && groupadd -g 1003 ops \
    && useradd -u 1000 -g hadoop -M -s /bin/bash hadoop \
    && useradd -u 1001 -g datalake -M -s /bin/bash etl \
    && useradd -u 1002 -g analysts -M -s /bin/bash analyst \
    && useradd -u 1003 -g hadoop -M -s /bin/bash spark \
    && mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/yarn \
    && chown -R hadoop:hadoop /hadoop \
    && mkdir -p "${HADOOP_LOG_DIR}" "${SPARK_LOG_DIR}" \
    && chmod 1777 "${HADOOP_LOG_DIR}" "${SPARK_LOG_DIR}"

COPY docker/conf/hadoop/ /opt/hadoop/etc/hadoop/
COPY docker/conf/spark/ /opt/spark/conf/
COPY --chmod=755 docker/bin/entrypoint.sh /usr/local/bin/gtdp-entrypoint
COPY --chmod=755 docker/bin/healthcheck.py /usr/local/bin/gtdp-healthcheck

ENTRYPOINT ["gtdp-entrypoint"]
CMD ["bash"]

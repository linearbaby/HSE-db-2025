FROM eclipse-temurin:11-jre

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH \
    JAVA_HOME=/opt/java/openjdk \
    HADOOP_USER_NAME=hdfs \
    UV_PYTHON_VENV_PATH=/opt/python-runtime/.venv/bin \
    UV_MANAGED_VENV=/opt/python-runtime/.venv \
    LC_ALL=C.UTF-8 LANG=C.UTF-8

WORKDIR /opt
ADD https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz /opt/
RUN tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

COPY conf/core-site.xml  ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hdfs-site.xml  ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY conf/yarn-site.xml  ${HADOOP_CONF_DIR}/yarn-site.xml
COPY conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY conf/capacity-scheduler.xml ${HADOOP_CONF_DIR}/capacity-scheduler.xml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

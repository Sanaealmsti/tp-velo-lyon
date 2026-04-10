FROM apache/hadoop:3.3.6

USER root

COPY core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml /opt/hadoop/etc/hadoop/
COPY start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh && chown hadoop:hadoop /start-hadoop.sh

RUN mkdir -p /tmp/hadoop-data/namenode /tmp/hadoop-data/datanode && \
    chown -R hadoop:hadoop /tmp/hadoop-data

USER hadoop
WORKDIR /opt/hadoop

EXPOSE 9000 9870 8088 8042

CMD ["/start-hadoop.sh"]

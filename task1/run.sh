sudo docker build -t hw-hadoop-ref:3.3.6 .
sudo docker run --rm --hostname any -e HADOOP_USER_NAME=hdfs hw-hadoop-ref:3.3.6
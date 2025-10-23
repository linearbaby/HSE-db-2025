#!/usr/bin/env bash

# конфиг локалхоста
echo "127.0.0.1 namenode" >> /etc/hosts
echo "127.0.0.1 resourcemanager" >> /etc/hosts

# Формат NN, без него не запускается
if [ ! -d /opt/hadoop-tmp/dfs/name/current ]; then
  hdfs namenode -format -force -nonInteractive
fi

# HDFS стартуем
hdfs --daemon start namenode
hdfs --daemon start datanode
for _ in {1..60}; do hdfs dfs -ls / >/dev/null 2>&1 && break || sleep 1; done

yarn --daemon start resourcemanager
yarn --daemon start nodemanager

# === ЗАДАНИЕ ===
# 1) /createme
hdfs dfs -mkdir -p /createme
# 2) удалить /delme
hdfs dfs -rm -r -f /delme || true
# 3) /nonnull.txt
echo "non empty" | hdfs dfs -put -f - /nonnull.txt

# /shadow.txt
cat > /opt/shadow.txt <<'TXT'
Innsmouth dreams and whispers.
Shadows over Innsmouth
Dunwich and Arkham, not Innsmouth
TXT
hdfs dfs -put -f /opt/shadow.txt /shadow.txt

# 4) wordcount
OUT=/tmp/wordcount-test
EX_JAR="$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"
yarn jar "$EX_JAR" wordcount /shadow.txt "$OUT"

# 5) Innsmouth
CNT=$(hdfs dfs -cat "$OUT"/part-* | awk -F'\t' '$1=="Innsmouth"{s+=$2}END{if(s==""){s=0}print s}')
printf "%s\n" "$CNT" | hdfs dfs -put -f - /whataboutinsmouth.txt

# Валидация результата
hdfs dfs -ls /
hdfs dfs -cat /whataboutinsmouth.txt
hdfs dfs -cat "$OUT"/part-* | head -n 50
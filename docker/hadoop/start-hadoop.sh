#!/bin/bash

HADOOP_HOME=/opt/hadoop
NAMENODE_DIR=/tmp/hadoop-data/namenode

if [ ! -d "$NAMENODE_DIR/current" ]; then
  echo "🔧 Formatage du NameNode (première fois)..."
  $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
fi

echo "🚀 Démarrage NameNode..."
$HADOOP_HOME/bin/hdfs --daemon start namenode
sleep 3

echo "🚀 Démarrage DataNode..."
$HADOOP_HOME/bin/hdfs --daemon start datanode
sleep 3

echo "🚀 Démarrage ResourceManager..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
sleep 3

echo "🚀 Démarrage NodeManager..."
$HADOOP_HOME/bin/yarn --daemon start nodemanager
sleep 3

echo "✅ Hadoop démarré. Processus Java actifs :"
ps -ef | grep -E "NameNode|DataNode|ResourceManager|NodeManager" | grep -v grep

echo "📡 NameNode UI : http://localhost:9870"
echo "📡 YARN UI     : http://localhost:8088"

# Garder le container vivant — on boucle en suivant les logs
mkdir -p $HADOOP_HOME/logs
while true; do
  tail -F $HADOOP_HOME/logs/*.log 2>/dev/null || sleep 10
done

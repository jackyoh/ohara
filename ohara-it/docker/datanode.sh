#!/bin/bash
mkdir -p $HADOOP_DATANODE_FOLDER
chmod 755 -R $HADOOP_DATANODE_FOLDER

# Build config xml file for the HDFS

if [[ -z "$HADOOP_NAMENODE" ]];then
  echo "HADOOP_NAMENODE environment variable is required!!!"
  exit 2
fi
echo "HADOOP_NAMENODE: ${HADOOP_NAMENODE}"
bash $HADOOP_HOME/bin/hdfs-site.sh > $HADOOP_CONF_DIR/hdfs-site.xml
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode
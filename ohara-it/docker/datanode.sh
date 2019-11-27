#!/bin/bash
mkdir -p $HADOOP_DATANODE_FOLDER
chmod 755 -R $HADOOP_DATANODE_FOLDER

# Build config xml file for the HDFS
echo "NAMENOST_HOST: ${HADOOP_NAMENODE_HOST}"
bash $HADOOP_HOME/bin/hdfs-site.sh > $HADOOP_CONF_DIR/hdfs-site.xml
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode
#!/bin/bash
mkdir -p $HADOOP_NAMENODE_FOLDER
chmod 755 -R $HADOOP_NAMENODE_FOLDER

# Build config xml file for the HDFS
bash $HADOOP_HOME/bin/core-site.sh > $HADOOP_CONF_DIR/core-site.xml
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format
$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode
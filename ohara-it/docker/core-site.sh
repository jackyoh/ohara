#!/bin/bash

echo '<?xml version="1.0" encoding="UTF-8"?>'
echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
echo '<configuration>'
echo '  <property>'
echo '    <name>fs.defaultFS</name>'
echo "    <value>hdfs://${HOSTNAME}:9000</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>hadoop.tmp.dir</name>'
echo "    <value>${HADOOP_NAMENODE_FOLDER}</value>"
echo '  </property>'
echo '</configuration>'
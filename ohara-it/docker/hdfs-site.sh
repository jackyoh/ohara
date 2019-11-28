#!/bin/bash

echo '<?xml version="1.0" encoding="UTF-8"?>'
echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
echo '<configuration>'
echo '  <property>'
echo '    <name>dfs.replication</name>'
echo "    <value>1</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.permissions</name>'
echo '    <value>false</value>'
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.data.dir</name>'
echo "    <value>${HADOOP_DATANODE_FOLDER}</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.namenode.rpc-address</name>'
echo "    <value>${HADOOP_NAMENODE}</value>"
echo '  </property>'
echo '</configuration>'
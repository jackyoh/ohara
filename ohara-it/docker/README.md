### How to build a docker image of HDFS?

* Please follow below command to build namenode and data docker image:

```
# docker build -f namenode.dockerfile -t oharastream/ohara:hdfs-namenode .
# docker build -f datanode.dockerfile -t oharastream/ohara:hdfs-datanode .
```

### How to running the namenode docker container?
* You must set all datanode IP and hostname mapping to the /etc/hosts file or you have the
DNS Server to resolve datanode hostname

* You must confirm namenode port firewalld is close

* Please follow below command to run namenode container:

```
# docker run --net host -it oharastream/ohara:hdfs-namenode
```

### How to running the datanode docker container?
* You must set namenode IP and hostname mapping to the /etc/hosts file or you have the
DNS Server toresolve namenode hostname

* You must confirm datanode port firewalld is close

* Please follow below command to run datanode container:

```
# docker run -it --env HADOOP_NAMENODE=${NAMENODE_HOST_NAME}:9000 --net host oharastream/ohara:hdfs-datanode
```

### How to use the shell script to run the HDFS container?
* You can follow below command to run HDFS container:

```
$ bash hdfs-container.sh [start-all|start-namenode|start-datanode|stop-all|stop-namenode|stop-datanode] arg1 ...
```

### How to use the shell script to run the Oracle database container?
* You can follow below command to run oracle database container:

```
$ bash oracle-container.sh start --user ${USERNAME} --password ${PASSWORD}
```

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

### How to build the ftp server docker image?
* You can follow below command to build the ftp server docker image

```
$ docker build -f ftp.dockerfile -t oharastream/ohara:ftp .
```
### How to run the fpt server docker container?
* You can follow below command to run the ftp docker container

```
$ docker run -d --env FTP_USER_NAME=${FTP_LOGIN_USER_NAME} --env FTP_USER_PASS=${FTP_LOGIN_PASSWORD} --env FORCE_PASSIVE_IP=${YOUR_HOST_IP} -p 21:21 -p 30000-30009:30000-30009 oharastream/ohara:ftp
```

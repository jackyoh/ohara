### How to build a docker image of HDFS?

* Please follow below command to build namenode and data docker image:

```
# docker build -f namenode.dockerfile -t oharastream/ohara:hadoop-namenode .
# docker build -f datanode.dockerfile -t oharastream/ohara:hadoop-datanode .
```

### How to running the namenode docker container?

* Please follow below command to run namenode container:

```
# docker run --net host -it oharastream/ohara:hadoop-namenode
```

### How to running the datanode docker container?

* Please follow below command to run datanode container:

```
# docker run -it --env HADOOP_NAMENODE=${NAMENODE_HOST_NAME}:9000 --net host oharastream/ohara:hadoop-datanode
```
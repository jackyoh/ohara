# Test case: Ohara installation

- [Use the hardware specification](#use-the-hardware-specification)
- [Use version information](#use-version-information)
- [Install the Kubernetes](#install-the-kubernetes)
- [Running the Ohara Configuration service](#running-the-ohara-configuration-service)
- [Running the Ohara Manager service](#running-the-ohara-manager-service)

## Use the hardware specification

- Preparation 5 nodes

  - K8S Master: **1 個 Node**, **CPU 4 Core**, **Memory 4 GB**, **Disk 100 GB**

  - K8S Slave: **3 個 Node**, **CPU 4 Core**, **Memory 4 GB**, **Disk 100 GB**

  - Ohara Configurator 和 Ohara Manager 在同一台 Node, **CPU 4 Core**, **Memory 4 GB**, **Disk 100 GB**

- HostName and IP Information

  - k8s-m-test 10.100.0.140

  - k8s-s-test-0 10.100.0.169

  - k8s-s-test-1 10.100.0.167

  - k8s-s-test-2 10.100.0.114

  - ohara-configurator 10.2.0.32

  - ohara-manager 10.2.0.32

## Environment

- Operating System: **CentOS 7.6.1810**

- Docker: **18.09.6**

- Kubernetes: **1.18.1**

- Ohara: **\${ohara version name}** 這裡以 0.7.0-SNAPSHOT 當做 example

## Install Kubernetes

- Please refer to the [document](https://ohara.readthedocs.io/en/latest/user_guide.html#kubernetes)

- 需要在每台 Kubernetes 的 Slave Node Pull zookeeper, broker, connector-worker 和 stream 的 docker image，指令如下：

```sh
$ docker pull oharastream/zookeeper:${ohara version name}
$ docker pull oharastream/broker:${ohara version name}
$ docker pull oharastream/connect-worker:${ohara version name}
$ docker pull oharastream/stream:${ohara version name}
```

Example

```sh
$ docker pull oharastream/zookeeper:0.7.0-SNAPSHOT
$ docker pull oharastream/broker:0.7.0-SNAPSHOT
$ docker pull oharastream/connect-worker:0.7.0-SNAPSHOT
$ docker pull oharastream/stream:0.7.0-SNAPSHOT
```

## Running Ohara Configuration service

1. 使用 ssh 登入到要啟動 Ohara Configuration Service 的 node 裡

2. Pull Ohara Configurator docker image

```sh
$ docker pull oharastream/configurator:${ohara version name}
```

Example:

```sh
$ docker pull oharastream/configurator:0.7.0-SNAPSHOT
```

3. 執行 Ohara Configurator service Docker container

```sh
$ docker run --rm \
           -p 5000:5000 \
           --add-host ${K8S_WORKER01_HOSTNAME}:${K8S_WORKER01_IP} \
           --add-host ${K8S_WORKER02_HOSTNAME}:${K8S_WORKER02_IP} \
           oharastream/configurator:0.7.0-SNAPSHOT \
           --port 5000 \
           --hostname ${Start Configurator Host Name} \
           --k8s http://${Your_K8S_Master_Host_IP}:8080/api/v1
```

Example:

```sh
$ docker run -d --rm \
           -p 5000:5000 \
           --add-host k8s-m-test:10.100.0.140 \
           --add-host k8s-s-test-0:10.100.0.169 \
           --add-host k8s-s-test-1:10.100.0.167 \
           --add-host k8s-s-test-2:10.100.0.114 \
           oharastream/configurator:0.7.0-SNAPSHOT \
           --port 5000 \
           --hostname 10.2.0.32 \
           --k8s http://10.100.0.140:8080/api/v1
```

4. 使用 docker 指令確認 Ohara Configurator 的 container 是否有啟動

```sh
$ docker ps -a

CONTAINER ID        IMAGE                                     COMMAND                  CREATED             STATUS              PORTS                    NAMES
d6127177b18f        oharastream/configurator:0.7.0-SNAPSHOT   "/tini -- configurat…"   About an hour ago   Up About an hour    0.0.0.0:5000->5000/tcp   adoring_bartik
```

5. 使用以下指令確認 Ohara Configurator 的 Restful API 是否能顯示版本資訊

```sh
$ curl -X GET http://${Your Configurator Node name}:5000/v0/info
```

Example:

```sh
$ curl -X GET http://10.2.0.32:5000/v0/info

{"versionInfo":{"branch":"master","revision":"1b38aaff103c7fa84440c2bbdfc7699eafb0716f","version":"0.7.0-SNAPSHOT","date":"2019-08-17 17:09:26","user":"root"},"mode":"K8S"}
```

6. 以上步驟不能正常執行，請使用 docker logs 指令查看 log，並且回報到 ohara GitHub 的 [Ohara issues page](https://github.com/oharastream/ohara/issues)

```sh
$ docker logs ${CONFIGURATOR CONTAINER ID}
```

## Running the Ohara Manager service

1. 使用 ssh 登入到要啟動 Ohara Manager Service 的 node 裡

2. Pull Ohara Manager docker image

```sh
$ docker pull oharastream/manager:${ohara version name}
```

Example

```sh
$ docker pull oharastream/manager:0.7.0-SNAPSHOT
```

3.  執行 Ohara Manager service Docker container

```sh
$ docker run -d --rm \
                       -p ${ohara-manager-port}:5050 \
                       oharastream/manager:${ohara version name} \
                      --port ${ohara-manager-port} \
                      --configurator http://${ohara-configurator-host}:${ohara-configurator-port}/v0
```

Example

```sh
$ docker run -d --rm \
                           -p 5050:5050 \
                          oharastream/manager:0.7.0-SNAPSHOT \
                          --port 5050 \
                         --configurator http://10.2.0.32:5000/v0
```

4. 使用 docker ps 指令查看 ohara manager service 的 container 是否有正常執行

```sh
$ docker ps -a

# Output
CONTAINER ID        IMAGE                                COMMAND                  CREATED             STATUS              PORTS                    NAMES
1f1b2cfac5d8        oharastream/manager:0.7.0-SNAPSHOT   "/tini -- manager.sh…"   4 seconds ago       Up 3 seconds        0.0.0.0:5050->5050/tcp   sleepy_nightingale
```

5. 使用 docker logs 指令查看 ohara manager service container 的 log

```sh
$ docker logs ${CONTAINER ID}
```

Example

```sh
$ docker logs -f 1f1b2cfac5d8

# Manager's log
[HPM] Proxy created: /  ->  http://10.2.0.32:5000/v0
[HPM] Proxy rewrite rule created: "/api" ~> ""
Ohara manager is running at port: 5050
Successfully connected to the configurator: http://10.2.0.32:5000/v0
```

6.  開啟 ohara manager 的 web 畫面，在 browser 的 URL 輸入以下網址：

```sh
http://10.2.0.32:5050
```

就可以看到 Ohara manager 的畫面

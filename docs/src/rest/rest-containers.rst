..
.. Copyright 2019 is-land
..
.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.
..


Container
=========

Each processes managed by Ohara is based on docker container. In most
cases, user don’t need to know the details of containers since the
management of containers is on Ohara’s shoulder. However, Ohara
understand that we all have curious brain so Ohara supports to display
the container’s details of a running cluster. Noted that the context may
be changed between different release of Ohara. And the distinct
implementations of container manager possibly provide different context
of containers.


retrieve the container details of a running cluster
---------------------------------------------------

*GET /v0/containers/$clusterName?group=$clusterGroup*

Example Response
  The **cluster name** may be mapped to different services (of course, it
  would be better to avoid using same name on different services), hence,
  the returned JSON is in array type. The details of elements are shown
  below.

  #. clusterKey (**Object**) — cluster key
  #. clusterType (**string**) — cluster type
  #. containers (**array(object)**) — the container in this cluster

     - environments (**object**) — the environment variables of container
     - name (**string**) — the name of container
     - hostname (**string**) — hostname of container
     - size (**string**) — the disk size used by this container
     - state (**option(string)**) — the state of container
     - portMappings (**array(object)**) — the exported ports of this container

       - portMappings[i].hostIp (**string**) — the network interface of container host
       - portMappings[i].hostPort (**int**) — host port
       - portMappings[i].containerPort (**int**) — container port

     - nodeName (**string**) — the node which host this container
     - imageName (**string**) — the image used to create this container
     - id (**string**) — container id
     - kind (**string**) — Ohara supports the DOCKER and K8S mode

  .. code-block:: json

    [
      {
        "clusterKey": {
          "group": "default",
          "name": "wk00"
        },
        "clusterType": "worker",
        "containers": [
          {
            "environments": {
              "KAFKA_JMX_OPTS": "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=41484 -Dcom.sun.management.jmxremote.rmi.port=41484 -Djava.rmi.server.hostname=ohara-release-test-00",
              "KAFKA_HEAP_OPTS": "-Xms1024M -Xmx1024M",
              "WORKER_PLUGIN_URLS": "",
              "WORKER_SHARED_JAR_URLS": ""
            },
            "name": "default-wk00-worker-3b8c71a",
            "hostname": "wk00-worker-5739cbd",
            "size": -1,
            "state": "RUNNING",
            "portMappings": [
              {
                "hostIp": "10.2.10.30",
                "hostPort": 36789,
                "containerPort": 36789
              },
              {
                "hostIp": "10.2.10.30",
                "hostPort": 41484,
                "containerPort": 41484
              }
            ],
            "nodeName": "ohara-release-test-00",
            "imageName": "oharastream/connect-worker:$|version|",
            "id": "2a3b3872-35ab-11ea-8a18-a29736512df3",
            "kind": "K8S"
          }
        ]
      }
    ]
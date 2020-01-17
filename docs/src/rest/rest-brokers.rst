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

.. _rest-brokers:

Broker
======

`Broker <https://kafka.apache.org/intro>`__ is core of data transmission
in ohara. The topic, which is a part our data lake, is hosted by broker
cluster. The number of brokers impacts the performance of transferring
data and data durability. But it is ok to setup broker cluster in single
node when testing. As with :ref:`zookeeper <rest-zookeepers>`, broker has many
configs also. Ohara still provide default to most configs and then
enable user to overwrite them.

Broker is based on :ref:`zookeeper <rest-zookeepers>`, hence you have to create
zookeeper cluster first. Noted that a zookeeper cluster can be used by
only a broker cluster. It will fail if you try to multi broker cluster
on same zookeeper cluster.

The properties which can be set by user are shown below.

#. name (**string**) — cluster name
#. group (**string**) — cluster group
#. imageName (**string**) — docker image
#. clientPort (**int**) — broker client port.
#. jmxPort (**int**) — port used by jmx service
#. zookeeperClusterKey (**object**) — key of zookeeper cluster used to store metadata of broker cluster

   - zookeeperClusterKey.group(**option(string)**) — the group of zookeeper cluster
   - zookeeperClusterKey.name(**string**) — the name of zookeeper cluster

    .. note::
      the following forms are legal as well. 1) {"name": "n"} and 2) "n". Both forms are converted to
      {"group": "default", "name": "n"}

#. nodeNames (**array(string)**) — the nodes running the zookeeper process
#. tags (**object**) — the user defined parameters

The following information are updated by Ohara.

#. aliveNodes (**array(string)**) — the nodes that host the running containers of broker cluster
#. state (**option(string)**) — only started/failed broker has state (RUNNING or DEAD)
#. error (**option(string)**) — the error message from a failed broker. If broker is fine or un-started, you won't get this field.
#. lastModified (**long**) — last modified this jar time

.. _rest-brokers-create:

create a broker cluster
-----------------------

*POST /v0/brokers*

Example Request
  .. code-block:: json

    {
      "name": "bk",
      "nodeNames": ["node00"],
      "zookeeperClusterKey": "zk"
    }

Example Response
  .. code-block:: json

    {
      "name": "bk00",
      "offsets.topic.replication.factor": 1,
      "xms": 1024,
      "routes": {},
      "num.partitions": 1,
      "lastModified": 1578903360246,
      "num.network.threads": 1,
      "tags": {},
      "xmx": 1024,
      "imageName": "oharastream/broker:$|version|",
      "log.dirs": "/home/ohara/default/data",
      "aliveNodes": [],
      "jmxPort": 42020,
      "num.io.threads": 1,
      "clientPort": 39614,
      "zookeeperClusterKey": {
        "group": "default",
        "name": "zk00"
      },
      "group": "default",
      "nodeNames": [
        "node00"
      ]
    }

list all broker clusters
------------------------

*GET /v0/brokers*

Example Response
  .. code-block:: json

    [
      {
        "name": "bk00",
        "offsets.topic.replication.factor": 1,
        "xms": 1024,
        "routes": {},
        "num.partitions": 1,
        "lastModified": 1578903360246,
        "num.network.threads": 1,
        "tags": {},
        "xmx": 1024,
        "imageName": "oharastream/broker:$|version|",
        "log.dirs": "/home/ohara/default/data",
        "aliveNodes": [],
        "jmxPort": 42020,
        "num.io.threads": 1,
        "clientPort": 39614,
        "zookeeperClusterKey": {
          "group": "default",
          "name": "zk00"
        },
        "group": "default",
        "nodeNames": [
          "node00"
        ]
      }
    ]

update broker cluster properties
--------------------------------

*PUT /v0/brokers/$name?group=$group*

.. note::
   If the required broker (group, name) was not exists, we will try to use this request as POST

  .. code-block:: json

    {
      "xmx": 2048
    }

Example Response
  .. code-block:: json

    {
      "name": "bk00",
      "offsets.topic.replication.factor": 1,
      "xms": 1024,
      "routes": {},
      "num.partitions": 1,
      "lastModified": 1578903494681,
      "num.network.threads": 1,
      "tags": {},
      "xmx": 2048,
      "imageName": "oharastream/broker:$|version|",
      "log.dirs": "/home/ohara/default/data",
      "aliveNodes": [],
      "jmxPort": 42020,
      "num.io.threads": 1,
      "clientPort": 39614,
      "zookeeperClusterKey": {
        "group": "default",
        "name": "zk00"
      },
      "group": "default",
      "nodeNames": [
        "node00"
      ]
    }

delete a broker properties
--------------------------

*DELETE /v0/brokers/$name?group=$group*

You cannot delete properties of an non-stopped broker cluster.
We will use the default value as the query parameter "?group=" if you don't specify it.

Example Response
  ::

     204 NoContent

  .. note::
     It is ok to delete an nonexistent broker cluster, and the response is
     204 NoContent.


.. _rest-brokers-get:

get a broker cluster
--------------------

*GET /v0/brokers/$name?group=$group*
We will use the default value as the query parameter "?group=" if you don't specify it.

Example Response
  .. code-block:: json

    {
      "name": "bk00",
      "offsets.topic.replication.factor": 1,
      "xms": 1024,
      "routes": {},
      "num.partitions": 1,
      "lastModified": 1578903494681,
      "num.network.threads": 1,
      "tags": {},
      "xmx": 2048,
      "imageName": "oharastream/broker:$|version|",
      "log.dirs": "/home/ohara/default/data",
      "aliveNodes": [],
      "jmxPort": 42020,
      "num.io.threads": 1,
      "clientPort": 39614,
      "zookeeperClusterKey": {
        "group": "default",
        "name": "zk00"
      },
      "group": "default",
      "nodeNames": [
        "node00"
      ]
    }


start a broker cluster
----------------------

*PUT /v0/brokers/$name/start?group=$group*
We will use the default value as the query parameter "?group=" if you don't specify it.

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get broker cluster <rest-brokers-get>` to fetch up-to-date status


stop a broker cluster
---------------------

Gracefully stopping a running broker cluster. It is disallowed to
stop a broker cluster used by a running :ref:`worker cluster <rest-workers>`.

*PUT /v0/brokers/$name/stop?group=$group[&force=true]*
We will use the default value as the query parameter "?group=" if you don't specify it.

Query Parameters
  #. force (**boolean**) — true if you don’t want to wait the graceful shutdown
     (it can save your time but may damage your data).

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get broker cluster <rest-brokers-get>` to fetch up-to-date status


add a new node to a running broker cluster
------------------------------------------

*PUT /v0/brokers/$name/$nodeName?group=$group*

If you want to extend a running broker cluster, you can add a node to
share the heavy loading of a running broker cluster. However, the
balance is not triggered at once.

We will use the default value as the query parameter "?group=" if you don't specify it.

Example Response
  ::

    202 Accepted

  .. note::
    Although it's a rare case, you should not use the "API keyword" as the nodeName.
    For example, the following APIs are invalid and will trigger different behavior!

    - /v0/brokers/$name/start
    - /v0/brokers/$name/stop

remove a node from a running broker cluster
-------------------------------------------

*DELETE /v0/brokers/$name/$nodeName?group=$group*

If your budget is limited, you can decrease the number of nodes running
broker cluster. BUT, removing a node from a running broker cluster
invoke a lot of data move. The loading may burn out the remaining nodes.

We will use the default value as the query parameter "?group=" if you don't specify it.

Example Response
  ::

     204 NoContent

  .. note::
     It is ok to delete an nonexistent broker node, and the response is
     204 NoContent.


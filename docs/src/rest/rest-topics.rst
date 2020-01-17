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

.. _rest-topics:

Topic
=====

Ohara topic is based on kafka topic. It means the creation of topic on
ohara will invoke a creation of kafka also. Also, the delete to ohara
topic also invoke a delete request to kafka. The common properties in
topic are shown below.

#. group (**string**) — topic group.
#. name (**string**) — topic name
#. brokerClusterKey (**Object**) — target broker cluster.

   - brokerClusterKey.group (**option(string)**) — the group of cluster
   - brokerClusterKey.name (**string**) — the name of cluster

   .. note::
      the following forms are legal as well. 1) {"name": "n"} and 2) "n". Both forms are converted to
      {"group": "default", "name": "n"}

#. numberOfReplications (**option(int)**) — the number of replications
#. numberOfPartitions (**option(int)**)— the number of partitions for this topic
#. tags (**option(object)**) — the extra description to this object

   .. note::
      #. The name must be unique in a broker cluster.
      #. There are many other available configs which are useful in creating topic. Please ref :ref:`broker clusters <rest-brokers>` to see how to retrieve the available configs for specific broker cluster.

The following information are tagged by ohara.

#. state (**option(string)**) — state of a running topic. nothing if the topic is not running.
#. partitionInfos (**Array(object)**) — the details of partitions.

   - index (**int**) — the index of partition
   - leaderNode (**String**) — the leader (node) of this partition
   - replicaNodes (**Array(String)**) — the nodes hosting the replica for this partition
   - inSyncReplicaNodes (**Array(String)**) — the nodes which have fetched the newest data from leader
   - beginningOffset (**long**) — the beginning offset
   - endOffset (**endOffset**) — the latest offset (Normally, it is the latest commit data)

#. group (**string**) — the group value is always "default"
#. :ref:`metrics <connector-metrics>` (**object**) — the metrics number of a running topic
#. lastModified (**long**) — the last time to update this ftp
   information


store a topic properties
------------------------

*POST /v0/topics*

.. note::
  #. the name you pass to ohara is used to build topic on kafka, and it is restricted by Kafka ([a-zA-Z0-9\._\-])
  #. the ignored fields will be auto-completed by Ohara Configurator. Also, you could update/replace it by UPDATE request later.
  #. this API does NOT create a topic on broker cluster. Instead, you should sent START request to run a topic on broker cluster actually
  #. There are many other available configs which are useful in creating topic. Please ref :ref:`broker clusters <rest-brokers>` to see how to retrieve the available configs for specific broker cluster.

Example Request
  .. code-block:: json

     {
       "brokerClusterKey": "bk",
       "name": "topic0",
       "numberOfReplications": 1,
       "numberOfPartitions": 1
     }


Example Response
  .. code-block:: json

    {
      "brokerClusterKey": {
        "group": "default",
        "name": "bk"
      },
      "name": "topic0",
      "partitionInfos": [],
      "lastModified": 1578537142950,
      "tags": {},
      "numberOfReplications": 1,
      "metrics": {
        "meters": []
      },
      "group":"default",
      "numberOfPartitions": 1
    }


  .. note::
     The topic, which is just created, does not have any metrics.


update a topic properties
-------------------------

*PUT /v0/topics/${name}?group=${group}*

Example Request
  .. code-block:: json

     {
       "numberOfPartitions": 3
     }


Example Response
  .. code-block:: json

    {
      "brokerClusterKey": {
        "group": "default",
        "name": "bk"
      },
      "name": "topic0",
      "partitionInfos": [],
      "lastModified": 1578537915735,
      "tags": {},
      "numberOfReplications": 1,
      "metrics": {
        "meters": []
      },
      "group": "default",
      "numberOfPartitions": 3
    }



list all topics properties
--------------------------

*GET /v0/topics?${key}=${value}*

the accepted query keys are listed below.
#. group
#. name
#. state
#. lastModified
#. tags
#. tag - this field is similar to tags but it addresses the "contain" behavior.
#. key

  .. note::
    Using "NONE" represents the nonexistence of state.

Example Response
  .. code-block:: json

    [
      {
        "brokerClusterKey": {
          "group": "default",
          "name": "bk"
        },
        "name": "topic1",
        "partitionInfos": [],
        "lastModified": 1578537915735,
        "tags": {},
        "numberOfReplications": 1,
        "metrics": {
          "meters": []
        },
        "group": "default",
        "numberOfPartitions": 3
      }
    ]

delete a topic properties
-------------------------

*DELETE /v0/topics/${name}?group=${group}*

Example Response

  ::

     204 NoContent

  .. note::
    It is ok to delete an nonexistent topic, and the response is 204 NoContent.
    You must be stopped the delete topic.

.. _rest-topics-get:

get a topic properties
----------------------

*GET /v0/topics/${name}*

Example Response
  .. code-block:: json

    {
      "brokerClusterKey": {
        "group": "default",
        "name": "bk"
      },
      "name": "topic1",
      "partitionInfos": [],
      "lastModified": 1578537915735,
      "tags": {},
      "numberOfReplications": 1,
      "metrics": {
        "meters": []
      },
      "group": "default",
      "numberOfPartitions": 3
    }

start a topic on remote broker cluster
--------------------------------------

*PUT /v0/topics/${name}/start*


Example Response
  ::

     202 Accepted

  .. note::
    You should use :ref:`Get Topic info <rest-topics-get>` to fetch up-to-date status

stop a topic from remote broker cluster
---------------------------------------

*PUT /v0/topics/${name}/stop*

.. note::
  the topic will lose all data after stopping.

Example Response
  ::

     202 Accepted

  .. note::
    You should use :ref:`Get Topic info <rest-topics-get>` to fetch up-to-date status

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


Inspect
=======

Inspect APIs is a powerful tool that it enable you to "see" what in the target via Configurator. For example, you can
get the definitions from specific image, or you can see what connectors or stream app in the specific file.

get Ohara Configurator info
---------------------------

*GET /v0/inspect/configurator*

the format of response of Ohara Configurator is shown below.

#. versionInfo (**object**) — version details of Ohara Configurator

   - branch (**string**) — the branch name of Ohara Configurator
   - version (**string**) — the release version of Ohara Configurator
   - revision (**string**) — commit hash of Ohara Configurator. You can trace the hash code via `Github <https://github.com/oharastream/ohara/commits/master>`__
   - user (**string**) — the release manager of Ohara Configurator.
   - date (**string**) — the date of releasing Ohara Configurator.

#. mode (**string**) — the mode of this configurator. There are three modes now:

   - K8S: k8s mode is for the production.
   - SSH: ssh is useful to simple env.
   - FAKE: fake mode is used to test APIs.

Example Response
  .. code-block:: json

    {
      "versionInfo": {
        "branch": "$|branch|",
        "revision": "b303f3c2e52647ee5e79e55f9d74a5e51238a92c",
        "version": "$|version|",
        "date": "2020-01-08 06:05:47",
        "user": "root"
      },
      "mode": "K8S"
    }


get zookeeper/broker/worker/stream info
---------------------------------------

*GET /v0/inspect/$service*

This API used to fetch the definitions for specific cluster service. The following fields are returned.

#. imageName (**string**) — the image name of service
#. settingDefinitions (**array(object)**) — the available settings for this service (see :ref:`setting <setting-definition>`)

the available variables for $service are shown below.

#. zookeeper
#. broker
#. worker
#. stream

Example Response
  .. code-block:: json

    {
      "imageName": "oharastream/zookeeper:$|version|",
      "settingDefinitions": [
        {
           "blacklist": [],
           "reference": "NONE",
           "displayName": "peerPort",
           "regex": null,
           "internal": false,
           "permission": "EDITABLE",
           "documentation": "the port exposed to each quorum",
           "necessary": "OPTIONAL_WITH_RANDOM_DEFAULT",
           "valueType": "BINDING_PORT",
           "tableKeys": [],
           "orderInGroup": 10,
           "key": "peerPort",
           "defaultValue": null,
           "recommendedValues": [],
           "group": "core"
        },
      ],
      "classInfos": []
    }


get running zookeeper/broker/worker/stream info
-----------------------------------------------

*GET /v0/inspect/$service/$name?group=$group*

This API used to fetch the definitions for specific cluster service and the definitions of available classes in the service.
The following fields are returned.

#. imageName (**string**) — the image name of service
#. settingDefinitions (**array(object)**) — the available settings for this service (see :ref:`setting <setting-definition>`)
#. classInfos (**array(object)**) — the information available classes in this service

  - classInfos[i].className — the name of this class
  - classInfos[i].classType — the type of this class. for example, topic, source connector, sink connector or stream app
  - classInfos[i].settingDefinitions — the definitions of this class

the available variables for $service are shown below.

#. zookeeper
#. broker
#. worker
#. stream

Example Response
  .. code-block:: json

    {
      "imageName": "oharastream/broker:$|version|",
      "settingDefinitions": [
        {
          "blacklist": [],
          "reference": "NONE",
          "displayName": "xmx",
          "regex": null,
          "internal": false,
          "permission": "EDITABLE",
          "documentation": "maximum memory allocation (in MB)",
          "necessary": "OPTIONAL_WITH_DEFAULT",
          "valueType": "POSITIVE_LONG",
          "tableKeys": [],
          "orderInGroup": 8,
          "key": "xmx",
          "defaultValue": 1024,
          "recommendedValues": [],
          "group": "core"
        }
      ],
      "classInfos": [
        {
          "classType": "topic",
          "className": "N/A",
          "settingDefinitions": [
            {
              "blacklist": [],
              "reference": "NONE",
              "displayName": "numberOfPartitions",
              "regex": null,
              "internal": false,
              "permission": "EDITABLE",
              "documentation": "the number of partitions",
              "necessary": "OPTIONAL_WITH_DEFAULT",
              "valueType": "POSITIVE_INT",
              "tableKeys": [],
              "orderInGroup": 4,
              "key": "numberOfPartitions",
              "defaultValue": 1,
              "recommendedValues": [],
              "group": "core"
            }
          ]
        }
      ]
    }

Query Database
--------------

*POST /v0/inspect/rdb*

This API returns the table details of a relational database. This API
invokes a running connector on worker cluster to fetch database
information and return to Ohara Configurator. You should deploy suitable
jdbc driver on worker cluster before using this API. Otherwise, you will
get a exception returned by Ohara Configurator. The query consists of
following fields.

#. url (**string**) — jdbc url
#. user (**string**) — user who can access target database
#. password (**string**) — password which can access target database
#. workerClusterKey (**Object**) — target worker cluster.

   - workerClusterKey.group (**option(string)**) — the group of cluster
   - workerClusterKey.name (**string**) — the name of cluster

  .. note::
    the following forms are legal as well. 1) {"name": "n"} and 2) "n". Both forms are converted to
    {"group": "default", "name": "n"}

#. catalogPattern (**option(string)**) — filter returned tables according to catalog
#. schemaPattern (**option(string)**) — filter returned tables according to schema
#. tableName (**option(string)**) — filter returned tables according to name

Example Request
  .. code-block:: json

    {
      "url": "jdbc:postgresql://localhost:5432/postgres",
      "user": "ohara",
      "password": "123456",
      "workerClusterKey": "wk00"
    }

Example Response
  #. name (**string**) — database name
  #. tables (**array(object)**)

     - tables[i].catalogPattern (**option(object)**) — table’s catalog pattern
     - tables[i].schemaPattern (**option(object)**) — table’s schema pattern
     - tables[i].name (**option(object)**) — table’s name
     - tables[i].columns (**array(object)**) — table’s columns

       - tables[i].columns[j].name (**string**) — column’s columns
       - tables[i].columns[j].dataType (**string**) — column’s data type
       - tables[i].columns[j].pk (**boolean**) — true if this column is pk. otherwise false

  .. code-block:: json

    {
      "name": "postgresql",
      "tables": [
        {
          "schemaPattern": "public",
          "name": "table1",
          "columns": [
            {
              "name": "column1",
              "dataType": "timestamp",
              "pk": false
            },
            {
              "name": "column2",
              "dataType": "varchar",
              "pk": true
            }
          ]
        }
      ]
    }


Query Topic
--------------

*POST /v0/inspect/topic/$name?group=$group&timeout=$timeout&$limit=$limit*

Fetch the latest data from a topic. the query arguments are shown below.

#. timeout (**long**) — break the fetch if this timeout is reached
#. limit (**long**) — the number of messages in topic

the response includes following items.

#. messages (**Array(Object)**) — messages

  - messages[i].partition (**int**) — the index of partition
  - messages[i].offset (**Long**) — the offset of this message
  - messages[i].sourceClass (**Option(String)**) — class name of the component which generate this data
  - messages[i].sourceKey (**Option(Object)**) — object key of the component which generate this data
  - messages[i].value (**Option(Object)**) — the value of this message
  - messages[i].error (**Option(String)**) — error message happen in failing to parse value

Example Response
  .. code-block:: json

    {
      "messages": [
        {
          "sourceKey": {
            "group": "default",
            "name": "perf"
          },
          "sourceClass": "com.island.ohara.connector.perf.PerfSourceTask",
          "partition": 0,
          "offset": 0,
          "value": {
            "a": "c54e2f3477",
            "b": "32ae422fb5",
            "c": "53e448ab80",
            "tags": []
          }
        }
      ]
    }

Query File
-----------

#. name (**string**) — the file name without extension
#. group (**string**) — the group name (we use this field to separate different workspaces)
#. size (**long**) — file size
#. tags (**object**) — the extra description to this object
#. lastModified (**long**) — the time of uploading this file
#. classInfos (**array(object)**) — the information of available classes in this file

  - classInfos[i].className — the name of this class
  - classInfos[i].classType — the type of this class. for example, topic, source connector, sink connector or stream app
  - classInfos[i].settingDefinitions — the definitions of this class


*POST /v0/inspect/files*

Example Request
  .. code-block:: text

     Content-Type: multipart/form-data
     file="ohara-it-sink.jar"
     group="default"


Example Response
  .. code-block:: json

    {
      "name": "ohara-it-sink.jar",
      "size": 7902,
      "url": null,
      "lastModified": 1579055900202,
      "tags": {},
      "classInfos": [
        {
          "classType": "sink",
          "className": "com.island.ohara.it.connector.DumbSinkConnector",
          "settingDefinitions": [
            {
              "blacklist": [],
              "reference": "NONE",
              "displayName": "kind",
              "regex": null,
              "internal": false,
              "permission": "READ_ONLY",
              "documentation": "kind of connector",
              "necessary": "OPTIONAL_WITH_DEFAULT",
              "valueType": "STRING",
              "tableKeys": [],
              "orderInGroup": 13,
              "key": "kind",
              "defaultValue": "sink",
              "recommendedValues": [],
              "group": "core"
            }
          ]
        }
      ],
      "group": "default"
    }

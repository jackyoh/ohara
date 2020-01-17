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

.. _rest-connectors:

Connector
=========

Connector is core of application in ohara :ref:`pipeline <rest-pipelines>`.
Connector has two type - source and sink. Source connector pulls data
from another system and then push to topic. By contrast, Sink connector
pulls data from topic and then push to another system. In order to use
connector in :ref:`pipeline <rest-pipelines>`, you have to set up a connector
settings in ohara and then add it to :ref:`pipeline <rest-pipelines>`. Of
course, the connector settings must belong to a existent connector in
target worker cluster. By default, worker cluster hosts only the
official connectors. If you have more custom requirement for connector,
please follow :ref:`custom connector guideline <connector>` to
write your connector.

Apart from custom settings, common settings are required by all
connectors. The common settings are shown below.

#. group (**string**) — the value of group is always "default"
#. name (**string**) — the name of this connector
#. connector.class (**class**) — class name of connector implementation
#. topicKeys(**array(object)**) — the source topics or target topics for this connector
#. columns (**array(object)**) — the schema of data for this connector

   - columns[i].name (**string**) — origin name of column
   - columns[i].newName (**string**) — new name of column
   - columns[i].dataType (**string**) — the type used to convert data
   - columns[i].order (**int**) — the order of this column

#. numberOfTasks (**int**) — the number of tasks
#. workerClusterKey (**Object**) — target worker cluster.

   - workerClusterKey.group (**option(string)**) — the group of cluster
   - workerClusterKey.name (**string**) — the name of cluster

   .. note::
      the following forms are legal as well. 1) {"name": "n"} and 2) "n". Both forms are converted to
      {"group": "default", "name": "n"}

#. tags (**object**) — the extra description to this object

The following information are updated by Ohara.

#. group (**string**) — connector’s group
#. name (**string**) — connector’s name
#. lastModified (**long**) — the last time to update this connector
#. state (**option(string)**) — the state of a started connector
#. nodeName (**option(string)**) — the node hosting this connector
#. error (**option(string)**) — the error message from a failed connector. If the connector is fine or un-started, you won’t get this field.
#. tasksStatus (**Array(object)**) — the tasks status of this connector

    - tasksStatus[i].state (**string**) — the state of a started task.
    - tasksStatus[i].nodeName (**string**) — the node hosting this task
    - tasksStatus[i].error (**option(string)**) — the error message from a failed task. If the task is fine or un-started, you won’t get this field.

#. :ref:`metrics <connector-metrics>` (**object**) — the metrics from a running connector

  - meters (**array(object)**) — the metrics in meter type

    - meters[i].name (**string**) — the number of this meter (normally, it is unique)
    - meters[i].value (**double**) — the value in double
    - meters[i].unit (**string**) — the unit of value
    - meters[i].document (**string**) — human-readable description to this meter
    - meters[i].queryTime (**Long**) — the time we query this meter from remote nodes
    - meters[i].startTime (**Option(Long)**) — the time to start this meter (not all services offer this record)

The following keys are internal and protected so you can't define them in creating/updating connector.

#. connectorKey — It points to the really (group, name) for the connector running in kafka.
#. topics —  It points to the really topic names in kafka for the connector running in kafka.



.. _rest-connectors-create-settings:

create the settings of connector
--------------------------------

*POST /v0/connectors*

It is ok to lack some common settings when creating settings for a
connector. However, it is illegal to start a connector with incomplete
settings. For example, storing the settings consisting of only
**connector.name** is ok. But stating a connector with above incomplete
settings will introduce a error.

Example Request
  .. code-block:: json

    {
      "name":"perf",
      "topicKeys": ["t0"],
      "workerClusterKey": "wk",
      "connector.class":"com.island.ohara.connector.perf.PerfSource"
    }

Example Response
  .. code-block:: json

    {
      "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "author": "root",
      "topicKeys": [
        {
          "group": "default",
          "name": "t0"
        }
      ],
      "name": "perf",
      "check.rule": "NONE",
      "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "lastModified": 1577282907085,
      "tags": {},
      "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "perf.cell.length": 10,
      "tasks.max": 1,
      "perf.batch": 10,
      "perf.frequency": "1000 milliseconds",
      "connector.class": "com.island.ohara.connector.perf.PerfSource",
      "revision": "baafe4a3d875e5e5028b686c4f74f26cfd8b1b66",
      "version": "$|version|",
      "columns": [],
      "metrics": {
        "meters": [],
      },
      "workerClusterKey": {
        "group": "default",
        "name": "wk"
      },
      "tasksStatus": [],
      "kind": "source",
      "group": "default"
    }

update the settings of connector
--------------------------------

*PUT /v0/connectors/${name}?group=${group}*

  .. note::
    you cannot update a non-stopped connector.

Example Request

  .. code-block:: json

    {
      "topicKeys": [
        "t1"
      ]
    }

Example Response
  .. code-block:: json

    {
      "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "author": "root",
      "topicKeys": [
        {
          "group": "default",
          "name": "t1"
        }
      ],
      "name": "perf",
      "check.rule": "NONE",
      "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "lastModified": 1577283010533,
      "tags": {},
      "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "perf.cell.length": 10,
      "tasks.max": 1,
      "perf.batch": 10,
      "perf.frequency": "1000 milliseconds",
      "connector.class": "com.island.ohara.connector.perf.PerfSource",
      "revision": "baafe4a3d875e5e5028b686c4f74f26cfd8b1b66",
      "version": "$|version|",
      "columns": [],
      "metrics": {
        "meters": [],
      },
      "workerClusterKey": {
        "group": "default",
        "name": "wk"
      },
      "tasksStatus": [],
      "kind": "source",
      "group": "default"
    }


list information of all connectors
----------------------------------

*GET /v0/connectors*

the accepted query keys are listed below.
#. group
#. name
#. lastModified
#. tags
#. tag - this field is similar to tags but it addresses the "contain" behavior.
#. key

Example Response
  .. code-block:: json

    [
      {
        "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "author": "root",
        "topicKeys": [
          {
            "group": "default",
            "name": "t1"
          }
        ],
        "name": "perf",
        "check.rule": "NONE",
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "lastModified": 1577283010533,
        "tags": {},
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "perf.cell.length": 10,
        "tasks.max": 1,
        "perf.batch": 10,
        "perf.frequency": "1000 milliseconds",
        "connector.class": "com.island.ohara.connector.perf.PerfSource",
        "revision": "baafe4a3d875e5e5028b686c4f74f26cfd8b1b66",
        "version": "$|version|",
        "columns": [],
        "metrics": {
          "meters": [],
        },
        "workerClusterKey": {
          "group": "default",
          "name": "wk"
        },
        "tasksStatus": [],
        "kind": "source",
        "group": "default"
      }
    ]

.. _rest-connectors-delete:

delete a connector
------------------

*DELETE /v0/connectors/${name}?group=${group}*

Deleting the settings used by a running connector is not allowed. You
should :ref:`stop <rest-connectors-stop>` connector before deleting it.

Example Response
  ::

     204 NoContent

  .. note::
     It is ok to delete an jar from an nonexistent connector or a running
     connector, and the response is 204 NoContent.


.. _rest-connectors-get-info:

get information of connector
----------------------------

*GET /v0/connectors/${name}?group=${group}*

Example Response
  .. code-block:: json

    {
      "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "author": "root",
      "topicKeys": [
        {
          "group": "default",
          "name": "t1"
        }
      ],
      "name": "perf",
      "check.rule": "NONE",
      "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "lastModified": 1577283010533,
      "tags": {},
      "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "perf.cell.length": 10,
      "tasks.max": 1,
      "perf.batch": 10,
      "perf.frequency": "1000 milliseconds",
      "connector.class": "com.island.ohara.connector.perf.PerfSource",
      "revision": "baafe4a3d875e5e5028b686c4f74f26cfd8b1b66",
      "version": "$|version|",
      "columns": [],
      "metrics": {
        "meters": [],
      },
      "workerClusterKey": {
        "group": "default",
        "name": "wk"
      },
      "tasksStatus": [],
      "kind": "source",
      "group": "default"
    }

start a connector
-----------------

*PUT /v0/connectors/${name}/start?group=${group}*

Ohara will send a start request to specific worker cluster to start the
connector with stored settings, and then make a response to called. The
connector is executed async so the connector may be still in starting
after you retrieve the response. You can send
:ref:`GET request <rest-connectors-get-info>` to see the state of
connector. This request is idempotent so it is safe to retry this
command repeatedly.

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get Connector info <rest-connectors-get-info>` to fetch up-to-date status

.. _rest-connectors-stop:

stop a connector
----------------

*PUT /v0/connectors/${name}/stop?group=${group}*

Ohara will send a stop request to specific worker cluster to stop the
connector. The stopped connector will be removed from worker cluster.
The settings of connector is still kept by ohara so you can start the
connector with same settings again in the future. If you want to delete
the connector totally, you should stop the connector and then
:ref:`delete <rest-connectors-delete>` it. This request is idempotent so it is
safe to send this request repeatedly.

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get Connector info <rest-connectors-get-info>` to fetch up-to-date status


pause a connector
-----------------

*PUT /v0/connectors/${name}/pause?group=${group}*

Pausing a connector is to disable connector to pull/push data from/to
source/sink. The connector is still alive in kafka. This request is
idempotent so it is safe to send this request repeatedly.

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get Connector info <rest-connectors-get-info>` to fetch up-to-date status

resume a connector
------------------

*PUT /v0/connectors/${name}/resume?group=${group}*

Resuming a connector is to enable connector to pull/push data from/to
source/sink. This request is idempotent so it is safe to retry this
command repeatedly.

Example Response
  ::

    202 Accepted

  .. note::
    You should use :ref:`Get Connector info <rest-connectors-get-info>` to fetch up-to-date status


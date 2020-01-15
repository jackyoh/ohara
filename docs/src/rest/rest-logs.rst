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

.. _rest-logs:

Logs
====

This world is beautiful but not safe. Even though ohara shoulders the
blame for simplifying your life, there is a slim chance that something
don’t work well in ohara. The Logs APIs, which are engineers’ best
friend, open a door to observe the logs of running cluster.

The available query parameters are shown below.

#. sinceSeconds (**long**) — show the logs since relative time

It collect output from all containers’ of a cluster and then format them
to JSON representation which has following elements.

#. clusterKey (**object**) — cluster key

   - clusterKey.group (**string**) — cluster group

   - clusterKey.name (**string**) — cluster name

#. logs (**array(object)**) — log of each container

   - logs[i].hostname — hostname

   - logs[i].value — total log of a container

get the log of a running cluster
--------------------------------

*GET /v0/logs/$clusterType/$clusterName?group=$group*

- clusterType (**string**)

  - zookeepers
  - brokers
  - workers
  - streams

Example Request 1
  * GET /v0/logs/zookeepers/zk?group=default

Example Response 1
  .. code-block:: json

    {
      "clusterKey": {
        "group": "default",
        "name": "zk"
      },
      "logs": [
        {
          "hostname": "node00",
          "value": "2020-01-14 10:15:42,146 [myid:] - INFO [main:QuorumPeerConfig@136"
        }
      ]
    }

Example Request 2
  * GET /v0/logs/zookeepers/zk?group=default&sinceSeconds=10000

Example Response 2
  .. code-block:: json

    {
      "clusterKey": {
        "group": "default",
        "name": "zk"
      },
      "logs": [
        {
          "hostname": "ohara-release-test-00",
          "value": "2020-01-15 02:00:43,090 [myid:] - INFO  [ProcessThread(sid:0 cport:2181)::PrepRequestProcessor@653] - Got user-level KeeperException when processing sessionid:0x100000761180000 type:setData cxid:0x11a zxid:0x9e txntype:-1 reqpath:n/a Error Path:/config/topics/default-topic0 Error:KeeperErrorCode = NoNode for /config/topics/default-topic0\n"
        }
      ]
    }

get the log of Configurator
---------------------------

*GET /v0/logs/configurator*

Example Response
  .. code-block:: json

    {
      "clusterKey": {
        "group": "N/A",
        "name": "node00"
      },
      "logs": [
        {
          "hostname": "node00",
          "value": "2020-01-10 09:43:02,669 INFO  [main] configurator.Configurator$(391): start a configurator built on hostname:ohara-release-test-00 and port:5000\n2020-01-10 09:43:02,676 INFO  [main] configurator.Configurator$(393): enter ctrl+c to terminate the configurator"
        }
      ]
    }

  .. note::
    the Configurator MUST run on docker container and the node hosting Configurator MUST be added to Configurator via
    :ref:`Node APIs <rest-nodes>`
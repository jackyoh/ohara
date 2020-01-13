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

.. _rest-nodes:

Node
====

Node is the basic unit of running service. It can be either physical
machine or vm. In section :ref:`zookeeper <rest-zookeepers>`,
:ref:`Broker <rest-brokers>` and :ref:`Worker <rest-workers>`, you will see many
requests demanding you to fill the node name to build the services.
Currently, ohara requires the node added to ohara should pre-install
following services.

#. docker (18.09+)
#. ssh server
#. k8s (only if you want to k8s to host containers)
#. official ohara images

   - `oharastream/zookeeper`_
   - `oharastream/broker`_
   - `oharastream/connect-worker`_
   - `oharastream/stream`_

The version (tag) depends on which ohara you used. It would be better to
use the same version to Ohara. For example, the version of Ohara
configurator you are running is $|version|, then the official images you should
download is oharastream/xxxx:$|version|.

The properties used by describing a node are shown below.

#. hostname (**string**) — hostname of node.

   This hostname must be available on you DNS.
   It will cause a lot of troubles if Ohara Configurator is unable to
   connect to remote node via this hostname.

#. port (**int**) — ssh port of node
#. user (**string**) — ssh account
#. password (**string**) — ssh password
#. tags (**object**) — the extra description to this object

The following information are updated at run-time.

#. services (**array(object)**) — the services hosted by this node

   - services[i].name (**string**) — service name (configurator, zookeeper, broker, connect-worker and _stream)
   - services[i].clusterKeys (**array(object)**) — the keys of this service

#. resources (**array(object)**) — the available resources of this node

   - resources[i].name (**string**) — the resource name
   - resources[i].value (**number**) — the "pure" number of resource
   - resources[i].used (**option(double)**) — the used "value" in percentage. Noted: this value may be null if the impl is unable to calculate the used resource.
   - resources[i].unit (**string**) — the description of the "value" unit

#. state (**String**) — "available" means this node works well. otherwise, "unavailable" is returned
#. error (**option(String)**) — the description to the unavailable node
#. lastModified (**long**) — the last time to update this node

store a node
------------

*POST /v0/nodes*

#. hostname (**string**) — hostname of node
#. port (**int**) — ssh port of node
#. user (**string**) — ssh account
#. password (**string**) — ssh password

Example Request
  .. code-block:: json

    {
      "hostname": "node00",
      "port": 22,
      "user": "abc",
      "password": "pwd"
    }

Example Response
  .. code-block:: json

    {
      "services": [
        {
          "name": "zookeeper",
          "clusterKeys": []
        },
        {
          "name": "broker",
          "clusterKeys": []
        },
        {
          "name": "connect-worker",
          "clusterKeys": []
        },
        {
          "name": "stream",
          "clusterKeys": []
        },
        {
          "name": "configurator",
          "clusterKeys": [
            {
                "group": "N/A",
                "name": "node00"
            }
          ]
        }
      ],
      "hostname": "node00",
      "state": "AVAILABLE",
      "lastModified": 1578627668686,
      "tags": {},
      "port": 22,
      "resources": [
        {
          "name": "CPU",
          "value": 6.0,
          "unit": "cores"
        },
        {
          "name": "Memory",
          "value": 10.496479034423828,
          "unit": "GB"
        }
      ],
      "user": "abc",
      "password": "pwd"
    }


update a node
-------------

*PUT /v0/nodes/${name}*

#. hostname (**string**) — hostname of node
#. port (**int**) — ssh port of node
#. user (**string**) — ssh account
#. password (**string**) — ssh password

Example Request
  .. code-block:: json

    {
      "port": 9999
    }

  .. note::
     An new node will be created if your input name does not exist

  .. note::
     the update request will clear the validation report attached to this node

Example Response
  .. code-block:: json

    {
      "services": [
        {
          "name": "zookeeper",
          "clusterKeys": []
        },
        {
          "name": "broker",
          "clusterKeys": []
        },
        {
          "name": "connect-worker",
          "clusterKeys": []
        },
        {
          "name": "stream",
          "clusterKeys": []
        },
        {
          "name": "configurator",
          "clusterKeys": [
            {
                "group": "N/A",
                "name": "node00"
            }
          ]
        }
      ],
      "hostname": "node00",
      "state": "AVAILABLE",
      "lastModified": 1578627668686,
      "tags": {},
      "port": 9999,
      "resources": [
        {
          "name": "CPU",
          "value": 6.0,
          "unit": "cores"
        },
        {
          "name": "Memory",
          "value": 10.496479034423828,
          "unit": "GB"
        }
      ],
      "user": "abc",
      "password": "pwd"
    }


list all nodes stored in Ohara
------------------------------

*GET /v0/nodes*

Example Response
  .. code-block:: json

    [
      {
        "services": [
          {
            "name": "zookeeper",
            "clusterKeys": []
          },
          {
            "name": "broker",
            "clusterKeys": []
          },
          {
            "name": "connect-worker",
            "clusterKeys": []
          },
          {
            "name": "stream",
            "clusterKeys": []
          },
          {
            "name": "configurator",
            "clusterKeys": [
              {
                  "group": "N/A",
                  "name": "node00"
              }
            ]
          }
        ],
        "hostname": "node00",
        "state": "AVAILABLE",
        "lastModified": 1578627668686,
        "tags": {},
        "port": 22,
        "resources": [
          {
            "name": "CPU",
            "value": 6.0,
            "unit": "cores"
          },
          {
            "name": "Memory",
            "value": 10.496479034423828,
            "unit": "GB"
          }
        ],
        "user": "abc",
        "password": "pwd"
      }
    ]


delete a node
-------------

*DELETE /v0/nodes/${name}*

Example Response
  ::

     204 NoContent

  .. note::
     It is ok to delete an an nonexistent pipeline, and the response is
     204 NoContent. However, it is disallowed to remove a node which is
     running service. If you do want to delete the node from ohara, please
     stop all services from the node.

get a node
----------

*GET /v0/nodes/${name}*

Example Response
  .. code-block:: json

    {
      "services": [
        {
          "name": "zookeeper",
          "clusterKeys": []
        },
        {
          "name": "broker",
          "clusterKeys": []
        },
        {
          "name": "connect-worker",
          "clusterKeys": []
        },
        {
          "name": "stream",
          "clusterKeys": []
        },
        {
          "name": "configurator",
          "clusterKeys": [
            {
                "group": "N/A",
                "name": "node00"
            }
          ]
        }
      ],
      "hostname": "node00",
      "state": "AVAILABLE",
      "lastModified": 1578627668686,
      "tags": {},
      "port": 22,
      "resources": [
        {
          "name": "CPU",
          "value": 6.0,
          "unit": "cores"
        },
        {
          "name": "Memory",
          "value": 10.496479034423828,
          "unit": "GB"
        }
      ],
      "user": "abc",
      "password": "pwd"
    }

.. _oharastream/zookeeper: https://cloud.docker.com/u/oharastream/repository/docker/oharastream/zookeeper
.. _oharastream/broker: https://cloud.docker.com/u/oharastream/repository/docker/oharastream/broker
.. _oharastream/connect-worker: https://cloud.docker.com/u/oharastream/repository/docker/oharastream/connect-worker
.. _oharastream/stream: https://cloud.docker.com/u/oharastream/repository/docker/oharastream/stream
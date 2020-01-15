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

.. _rest-objects:

Object
======

Object APIs offer a way to store anything to Configurator. It is useful when you have something temporary and you have
no other way to store them.

Similar to other APIs, the required fields are "name" and "group".

#. name (**string**) — name of object.
#. group (**string**) — group of object
#. tags (**option(object)**) — the extra description to this object

The following information are updated at run-time.

#. lastModified (**long**) — the last time to update this node


store a object
--------------

*POST /v0/objects*

Example Request
  .. code-block:: json

     {
       "name": "n0",
       "k": "v"
     }

Example Response
  .. code-block:: json

    {
      "name": "n0",
      "lastModified": 1579071742763,
      "tags": {},
      "k": "v",
      "group": "default"
    }

update a object
---------------

*PUT /v0/objects/${name}*

Example Request
  .. code-block:: json

     {
       "k0": "v0"
     }

Example Response
  .. code-block:: json

    {
      "name": "n0",
      "k0": "v0",
      "lastModified": 1579072298657,
      "tags": {},
      "k": "v",
      "group": "default"
    }

list all objects
----------------

*GET /v0/objects*

Example Response
  .. code-block:: json

    [
      {
        "name": "n0",
        "k0": "v1000000",
        "lastModified": 1579072345437,
        "tags": {},
        "k": "v",
        "group": "default"
      }
    ]

delete a node
-------------

*DELETE /v0/objects/${name}*

Example Response
  ::

     204 NoContent

get a object
------------

*GET /v0/objects/${name}*

Example Response
  .. code-block:: json

    {
      "name": "n0",
      "k0": "v0",
      "lastModified": 1579072345437,
      "tags": {},
      "k": "v",
      "group": "default"
    }
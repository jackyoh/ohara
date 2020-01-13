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

.. _rest-validation:

Validation
==========

Notwithstanding we have read a lot of document and guideline, there is a
chance to input incorrect request or settings when operating ohara.
Hence, ohara provides a serial APIs used to validate request/settings
before you do use them to start service. Noted that not all
request/settings are validated by Ohara configurator. If the
request/settings is used by other system (for example, kafka), ohara
automatically bypass the validation request to target system and then
wrap the result to JSON representation.

Validate the connector settings
-------------------------------

*PUT /v0/validate/connector*

Before starting a connector, you can send the settings to test whether
all settings are available for specific connector. Ohara is not in
charge of settings validation. Connector MUST define its setting via
:ref:`setting definitions <setting-definition>`.
Ohara configurator only repackage the request to kafka format and then
collect the validation result from kafka.

Example Request
  The request format is same as :ref:`connector request <rest-connectors-create-settings>`

Example Response
  If target connector has defined the settings correctly, kafka is doable
  to validate each setting of request. Ohara configurator collect the
  result and then generate the following report.

  .. code-block:: json

     {
       "errorCount": 0,
       "settings": [
         {
           "definition": {
             "displayName": "Connector class",
             "group": "core",
             "orderInGroup": 3,
             "key": "connector.class",
             "valueType": "CLASS",
             "necessary": "REQUIRED",
             "defaultValue": null,
             "documentation": "the class name of connector",
             "reference": "NONE",
             "regex": null,
             "internal": false,
             "permission": "EDITABLE",
             "tableKeys": [],
             "recommendedValues": [],
             "blacklist": []
           },
           "value": {
             "key": "connector.class",
             "value": "com.island.ohara.connector.perf.PerfSource",
             "errors": []
           }
         }
       ]
     }

The above example only show a part of report. The element **definition**
is equal to :ref:`connector’s setting definition <rest-workers>`. The definition
is what connector must define. If you don’t write any definitions for
you connector, the validation will do nothing for you. The element
**value** is what you request to validate.

#. key (**string**) — the property key. It is equal to key in **definition**
#. value (**string**) — the value you request to validate
#. errors (**array(string)**) — error message when the input value is illegal to connector



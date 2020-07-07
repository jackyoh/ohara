/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ObjectKey } from './basicInterface';
import { ServiceBody } from './clusterInterface';

export enum WORKSPACE_FLAGS {
  CREATING = 'creating',
  CREATED = 'created',
  RESTARTING = 'restarting',
  RESTARTED = 'restarted',
  DELETING = 'deleting',
  DELETED = 'deleted',
}

export interface Workspace extends ObjectKey {
  broker?: ServiceBody;
  flag?: WORKSPACE_FLAGS;
  nodeNames: string[];
  worker?: ServiceBody;
  zookeeper?: ServiceBody;
}
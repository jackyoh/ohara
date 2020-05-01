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

import { merge } from 'lodash';
import { normalize } from 'normalizr';
import { ofType } from 'redux-observable';
import { defer, from } from 'rxjs';
import {
  catchError,
  distinctUntilChanged,
  map,
  mergeMap,
  startWith,
} from 'rxjs/operators';

import { LOG_LEVEL } from 'const';
import * as zookeeperApi from 'api/zookeeperApi';
import * as actions from 'store/actions';
import * as schema from 'store/schema';
import { getId } from 'utils/object';

export const createZookeeper$ = values => {
  const zookeeperId = getId(values);
  return defer(() => zookeeperApi.create(values)).pipe(
    map(res => res.data),
    map(data => normalize(data, schema.zookeeper)),
    map(normalizedData => merge(normalizedData, { zookeeperId })),
    map(normalizedData => actions.createZookeeper.success(normalizedData)),
    startWith(actions.createZookeeper.request({ zookeeperId })),
    catchError(err =>
      from([
        actions.createZookeeper.failure(merge(err, { zookeeperId })),
        actions.createEventLog.trigger({ ...err, type: LOG_LEVEL.error }),
      ]),
    ),
  );
};

export default action$ =>
  action$.pipe(
    ofType(actions.createZookeeper.TRIGGER),
    map(action => action.payload),
    distinctUntilChanged(),
    mergeMap(values => createZookeeper$(values)),
  );

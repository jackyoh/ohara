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

import { normalize } from 'normalizr';
import { merge } from 'lodash';
import { ofType } from 'redux-observable';
import { defer, interval, of } from 'rxjs';
import {
  catchError,
  debounce,
  map,
  switchMap,
  startWith,
} from 'rxjs/operators';

import * as workerApi from 'api/workerApi';
import * as actions from 'store/actions';
import * as schema from 'store/schema';
import { getId } from 'utils/object';

export const updateWorker$ = values => {
  const workerId = getId(values);
  return defer(() => workerApi.update(values)).pipe(
    map(res => res.data),
    map(data => normalize(data, schema.worker)),
    map(normalizedData => merge(normalizedData, { workerId })),
    map(normalizedData => actions.updateWorker.success(normalizedData)),
    startWith(actions.updateWorker.request({ workerId })),
    catchError(error =>
      of(actions.updateWorker.failure(merge(error, { workerId }))),
    ),
  );
};

export default action$ =>
  action$.pipe(
    ofType(actions.updateWorker.TRIGGER),
    map(action => action.payload),
    debounce(() => interval(1000)),
    switchMap(values => updateWorker$(values)),
  );
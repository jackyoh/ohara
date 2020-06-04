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

import { omit } from 'lodash';
import { RESOURCE, API, COMMAND } from 'api/utils/apiUtils';
import { ObjectKey, BasicResponse } from './apiInterface/basicInterface';
import {
  PipelineResponse,
  PipelineResponseList,
  PipelineRequest,
} from './apiInterface/pipelineInterface';

const pipelineApi = new API(RESOURCE.PIPELINE);

export const create = (params: PipelineRequest) => {
  return pipelineApi.post<PipelineResponse>({
    name: params.name,
    body: params,
  });
};

// For manager test
export const update = (params: PipelineRequest) => {
  return pipelineApi.put<PipelineResponse>({
    name: params.name,
    group: params.group,
    body: omit(params, ['name', 'group']),
  });
};

export const remove = (objectKey: ObjectKey) => {
  return pipelineApi.delete<BasicResponse>(objectKey);
};

export const get = (objectKey: ObjectKey) => {
  return pipelineApi.get<PipelineResponse>({
    name: objectKey.name,
    queryParams: { group: objectKey.group },
  });
};

export const getAll = (queryParams?: object) => {
  return pipelineApi.get<PipelineResponseList>({ queryParams });
};

export const refresh = (objectKey: ObjectKey) => {
  return pipelineApi.execute<BasicResponse>({
    objectKey,
    action: COMMAND.REFRESH,
  });
};

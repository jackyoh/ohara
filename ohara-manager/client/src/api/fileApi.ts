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

// Note: Do not change the usage of absolute path
// unless you have a solution to resolve TypeScript + Coverage
// For the QA test
import { omit } from 'lodash';
import { RESOURCE, API } from '../api/utils/apiUtils';
import { ObjectKey, BasicResponse } from './apiInterface/basicInterface';
import {
  FileRequest,
  FileResponse,
  FileResponseList,
} from './apiInterface/fileInterface';

const fileApi = new API(RESOURCE.FILE);

export const create = (file: FileRequest) => {
  return fileApi.upload<FileResponse>({ params: file });
};

export const update = (params: { [k: string]: any }) => {
  return fileApi.put<FileResponse>({
    name: params.name,
    group: params.group,
    body: omit(params, ['name', 'group']),
  });
};

export const remove = (objectKey: ObjectKey) => {
  return fileApi.delete<BasicResponse>(objectKey);
};

export const get = (objectKey: ObjectKey) => {
  return fileApi.get<FileResponse>({
    name: objectKey.name,
    queryParams: { group: objectKey.group },
  });
};

export const getAll = (queryParams?: object) => {
  return fileApi.get<FileResponseList>({ queryParams });
};

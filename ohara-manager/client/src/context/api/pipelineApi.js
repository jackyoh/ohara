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

import { isEmpty } from 'lodash';
import { hashWith } from 'utils/sha';
import * as pipelineApi from 'api/pipelineApi';
import { validate } from './utils';
import { WORKSPACE } from './index';

export const createApi = context => {
  const { workspaceName, showMessage } = context;
  if (!workspaceName) return;

  const group = hashWith(WORKSPACE, workspaceName);
  return {
    fetchAll: async () => {
      const params = { group };
      const res = await pipelineApi.getAll(params);
      if (!isEmpty(res.errors)) {
        throw new Error(res.title);
      }
      return res.data;
    },
    fetch: async name => {
      const params = { name, group };
      const res = await pipelineApi.get(params);
      if (!isEmpty(res.errors)) {
        throw new Error(res.title);
      }
      return res.data;
    },
    create: async values => {
      try {
        validate(values);
        const params = { ...values, group };
        const res = await pipelineApi.create(params);
        if (!isEmpty(res.errors)) {
          throw new Error(res.title);
        }
        showMessage(res.title);
        return res.data;
      } catch (e) {
        showMessage(e.message);
        throw e;
      }
    },
    update: async values => {
      try {
        validate(values);
        const params = { ...values, group };
        const res = await pipelineApi.update(params);
        if (!isEmpty(res.errors)) {
          throw new Error(res.title);
        }
        showMessage(res.title);
        return res.data;
      } catch (e) {
        showMessage(e.message);
        throw e;
      }
    },
    delete: async name => {
      try {
        const params = { name, group };
        const res = await pipelineApi.remove(params);
        if (!isEmpty(res.errors)) {
          throw new Error(res.title);
        }
        showMessage(res.title);
      } catch (e) {
        showMessage(e.message);
        throw e;
      }
    },
    refresh: async name => {
      try {
        const params = { name, group };
        const res = await pipelineApi.refresh(params);
        if (!isEmpty(res.errors)) {
          throw new Error(res.title);
        }
        showMessage(res.title);
      } catch (e) {
        showMessage(e.message);
        throw e;
      }
    },
  };
};
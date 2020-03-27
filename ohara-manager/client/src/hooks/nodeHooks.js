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

import { useCallback, useMemo } from 'react';
import { useDispatch, useSelector } from 'react-redux';

import * as actions from 'store/actions';
import * as selectors from 'store/selectors';

export const useAllNodes = () => {
  const getAllNodes = useMemo(selectors.makeGetAllNodes, []);
  const nodes = useSelector(
    useCallback(state => getAllNodes(state), [getAllNodes]),
  );
  return nodes;
};

export const useCreateNodeAction = () => {
  const dispatch = useDispatch();
  return function(values) {
    dispatch(actions.createNode.trigger(values));
  };
};

export const useUpdateNodeAction = () => {
  const dispatch = useDispatch();
  return function(values) {
    dispatch(actions.updateNode.trigger(values));
  };
};

export const useFetchNodesAction = () => {
  const dispatch = useDispatch();
  return function() {
    dispatch(actions.fetchNodes.trigger());
  };
};

export const useDeleteNodeAction = () => {
  const dispatch = useDispatch();
  return function(values) {
    dispatch(actions.deleteNode.trigger(values));
  };
};
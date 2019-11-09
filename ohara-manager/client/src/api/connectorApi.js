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

import * as connector from './body/connectorBody';
import { requestUtil, responseUtil, axiosInstance } from './utils/apiUtils';
import * as URL from './utils/url';
import wait from './waitApi';
import * as waitUtil from './utils/waitUtils';
import * as workerApi from './workerApi';

const url = URL.CONNECTOR_URL;

export const connectorSources = {
  jdbc: 'com.island.ohara.connector.jdbc.source.JDBCSourceConnector',
  json: 'com.island.ohara.connector.jio.JsonIn',
  ftp: 'com.island.ohara.connector.ftp.FtpSource',
  smb: 'com.island.ohara.connector.smb.SmbSource',
  perf: 'com.island.ohara.connector.perf.PerfSource',
};

export const connectorSinks = {
  json: 'com.island.ohara.connector.jio.JsonOut',
  ftp: 'com.island.ohara.connector.ftp.FtpSink',
  hdfs: 'com.island.ohara.connector.hdfs.sink.HDFSSink',
  smb: 'com.island.ohara.connector.smb.SmbSink',
  console: 'com.island.ohara.connector.console.ConsoleSink',
};

export const create = async (params, body) => {
  body = body ? body : await workerApi.get(params.workerClusterKey);
  // the "connector.class" is key of each connector
  // we can use it to filter the correct definition
  // the connector__class here is meant to be "connector.class" of definitions
  const newBody = { ...body, className: params.connector__class };
  const requestBody = requestUtil(params, connector, newBody);
  const res = await axiosInstance.post(url, requestBody);
  return responseUtil(res, connector);
};

export const start = async params => {
  const { name, group } = params;
  await axiosInstance.put(`${url}/${name}/start?group=${group}`);
  const res = await wait({
    url: `${url}/${name}?group=${group}`,
    checkFn: waitUtil.waitForConnectRunning,
  });
  return responseUtil(res, connector);
};

export const update = async params => {
  const { name, group } = params;
  delete params[name];
  delete params[group];
  const body = params;
  const res = await axiosInstance.put(`${url}/${name}?group=${group}`, body);
  return responseUtil(res, connector);
};

export const stop = async params => {
  const { name, group } = params;
  await axiosInstance.put(`${url}/${name}/stop?group=${group}`);
  const res = await wait({
    url: `${url}/${name}?group=${group}`,
    checkFn: waitUtil.waitForConnectStop,
  });
  return responseUtil(res, connector);
};

export const remove = async params => {
  const { name, group } = params;
  await axiosInstance.delete(`${url}/${name}?group=${group}`);
  const res = await wait({
    url,
    checkFn: waitUtil.waitForClusterNonexistent,
    paramRes: params,
  });
  return responseUtil(res, connector);
};

export const get = async params => {
  const { name, group } = params;
  const res = await axiosInstance.get(`${url}/${name}?group=${group}`);
  return responseUtil(res, connector);
};

export const getAll = async (params = {}) => {
  const res = await axiosInstance.get(url + URL.toQueryParameters(params));
  return res ? responseUtil(res, connector) : [];
};
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

import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { useHistory } from 'react-router-dom';
import { get, toNumber, size, find, isEqual } from 'lodash';
import moment from 'moment';
import Grid from '@material-ui/core/Grid';
import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';
import Avatar from '@material-ui/core/Avatar';
import Button from '@material-ui/core/Button';
import InputIcon from '@material-ui/icons/Input';
import Typography from '@material-ui/core/Typography';

import * as topicApi from 'api/topicApi';
import { useListWorkspacesDialog, useWorkspace } from 'context';
import { Dialog } from 'components/common/Dialog';
import { Wrapper } from './WorkspaceListStyles';

const Statistic = ({ value, label }) => (
  <>
    <Typography variant="h4" component="h2" align="center">
      {value}
    </Typography>
    <Typography
      variant="caption"
      component="h6"
      color="textSecondary"
      gutterBottom
      align="center"
    >
      {label}
    </Typography>
  </>
);

Statistic.propTypes = {
  value: PropTypes.number.isRequired,
  label: PropTypes.string.isRequired,
};

function WorkspaceList() {
  const history = useHistory();
  const { isOpen, close } = useListWorkspacesDialog();
  const { workspaces, currentWorkspace } = useWorkspace();
  const currWorkspaceName = get(currentWorkspace, 'settings.name');
  const [topics, setTopics] = useState(null);

  useEffect(() => {
    const fetchTopics = async () => {
      const result = await topicApi.getAll();
      setTopics(result.errors ? [] : result.data);
    };
    if (topics !== null) return;
    fetchTopics();
  }, [topics]);

  const handleClick = name => () => {
    history.push(`/${name}`);
    close();
  };

  const pickBrokerKey = object => {
    return {
      name: get(object, 'settings.brokerClusterKey.name'),
      group: get(object, 'settings.brokerClusterKey.group'),
    };
  };

  return (
    <>
      <Dialog
        open={isOpen}
        handleClose={close}
        title={`Showing ${size(workspaces)} workspaces`}
        showActions={false}
        maxWidth="md"
      >
        <Wrapper>
          <Grid container spacing={2}>
            {workspaces.map(workspace => {
              const name = get(workspace, 'settings.name');
              const nodeNames = get(workspace, 'settings.nodeNames');
              const lastModified = get(workspace, 'lastModified');

              const avatarText = name.substring(0, 2).toUpperCase();
              const updatedText = moment(toNumber(lastModified)).fromNow();
              const isActive = name === currWorkspaceName;
              const brokerKey = pickBrokerKey(workspace);
              const count = {
                nodes: size(nodeNames),
                pipelines: 0, // TODO: See the issue (https://github.com/oharastream/ohara/issues/3506)
                topics: size(
                  find(topics, topic =>
                    isEqual(pickBrokerKey(topic), brokerKey),
                  ),
                ),
              };

              return (
                <Grid item xs={4} key={name}>
                  <Card className={isActive ? 'active-workspace' : ''}>
                    <CardHeader
                      avatar={
                        <Avatar className="workspace-icon">{avatarText}</Avatar>
                      }
                      title={name}
                      subheader={`Updated: ${updatedText}`}
                    />
                    <CardContent>
                      <Grid container spacing={2}>
                        <Grid item xs={4}>
                          <Statistic value={count.nodes} label="Nodes" />
                        </Grid>
                        <Grid item xs={4}>
                          <Statistic
                            value={count.pipelines}
                            label="Pipelines"
                          />
                        </Grid>
                        <Grid item xs={4}>
                          <Statistic value={count.topics} label="Topics" />
                        </Grid>
                      </Grid>
                    </CardContent>
                    <CardActions>
                      <Button
                        size="large"
                        startIcon={<InputIcon />}
                        className="call-to-action"
                        onClick={handleClick(name)}
                        disabled={isActive}
                      >
                        Into Workspace
                      </Button>
                    </CardActions>
                  </Card>
                </Grid>
              );
            })}
          </Grid>
        </Wrapper>
      </Dialog>
    </>
  );
}

export default WorkspaceList;
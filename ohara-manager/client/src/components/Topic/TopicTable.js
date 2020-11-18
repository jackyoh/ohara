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

import { useState } from 'react';
import PropTypes from 'prop-types';
import { join, map, includes, isEmpty, isFunction, toUpper } from 'lodash';

import Link from '@material-ui/core/Link';
import Tooltip from '@material-ui/core/Tooltip';

import { State } from 'api/apiInterface/topicInterface';
import { Actions, MuiTable as Table } from 'components/common/Table';
import TopicCreateDialog from './TopicCreateDialog';
import TopicDeleteDialog from './TopicDeleteDialog';
import TopicDetailDialog from './TopicDetailDialog';

const defaultOptions = {
  onCreateIconClick: null,
  onDeleteIconClick: null,
  onDetailIconClick: null,
  showCreateIcon: true,
  showDeleteIcon: true,
  showDetailIcon: true,
  showTitle: true,
};

function TopicTable(props) {
  const {
    broker,
    topics: data,
    onCreate,
    onDelete,
    onLinkClick,
    title,
  } = props;

  const options = { ...defaultOptions, ...props?.options };

  const [activeTopic, setActiveTopic] = useState();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [isDetailDialogOpen, setIsDetailDialogOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);

  const handleCreateIconClick = () => {
    if (isFunction(options?.onCreateIconClick)) {
      options.onCreateIconClick();
    } else {
      setIsCreateDialogOpen(true);
    }
  };

  const handleDeleteIconClick = (topic) => {
    if (isFunction(options?.onDeleteIconClick)) {
      options.onDeleteIconClick(topic);
    } else {
      setIsDeleteDialogOpen(true);
      setActiveTopic(topic);
    }
  };

  const handleDetailIconClick = (topic) => {
    if (isFunction(options?.onDetailIconClick)) {
      options.onDetailIconClick(topic);
    } else {
      setIsDetailDialogOpen(true);
      setActiveTopic(topic);
    }
  };

  const renderActionColumn = () => {
    const isShow = options?.showDeleteIcon || options?.showDetailIcon;

    return {
      cellStyle: { textAlign: 'right' },
      headerStyle: { textAlign: 'right' },
      hidden: !isShow,
      render: (rowData) => (
        <Actions
          actions={[
            {
              hidden: !options?.showDetailIcon,
              name: 'view',
              onClick: handleDetailIconClick,
              tooltip: 'View topic',
            },
            {
              disabled: !isEmpty(rowData.pipelines),
              hidden: !options?.showDeleteIcon,
              name: 'delete',
              onClick: handleDeleteIconClick,
              tooltip: !isEmpty(rowData.pipelines)
                ? 'Cannot delete topics that are in use'
                : 'Delete topic',
            },
          ]}
          data={rowData}
        />
      ),
      sorting: false,
      title: 'Actions',
    };
  };

  const sort = (a, b) => {
    if (a !== b) {
      // to find nulls
      if (!a) return -1;
      if (!b) return 1;
    }
    return a < b ? -1 : a > b ? 1 : 0;
  };

  return (
    <>
      <Table
        actions={[
          {
            disabled: broker?.state !== State.RUNNING,
            hidden: !options?.showCreateIcon,
            icon: 'add',
            isFreeAction: true,
            onClick: handleCreateIconClick,
            tooltip: 'Create Topic',
          },
        ]}
        columns={[
          {
            title: 'Name',
            customFilterAndSearch: (filterValue, topic) => {
              return includes(toUpper(topic.displayName), toUpper(filterValue));
            },
            customSort: (topic, anotherTopic) => {
              return sort(topic?.displayName, anotherTopic?.displayName);
            },
            render: (topic) => {
              return topic.displayName;
            },
          },
          {
            title: 'Partitions',
            field: 'numberOfPartitions',
            type: 'numeric',
          },
          {
            title: 'Replications',
            field: 'numberOfReplications',
            type: 'numeric',
          },
          {
            title: 'Pipelines',
            customFilterAndSearch: (filterValue, topic) => {
              const value = join(
                map(topic?.pipelines, (pipeline) => pipeline.name),
              );
              return includes(toUpper(value), toUpper(filterValue));
            },
            render: (topic) => {
              return (
                <>
                  {map(topic?.pipelines, (pipeline) => (
                    <div key={pipeline.name}>
                      <Tooltip title="Click the link to switch to that pipeline">
                        <Link
                          component="button"
                          onClick={() => onLinkClick(pipeline)}
                          variant="body2"
                        >
                          {pipeline.name}
                        </Link>
                      </Tooltip>
                    </div>
                  ))}
                </>
              );
            },
            sorting: false,
          },
          {
            title: 'State',
            field: 'state',
          },
          renderActionColumn(),
        ]}
        data={data}
        options={{
          prompt: options?.prompt,
          rowStyle: null,
          selection: false,
          showTitle: options?.showTitle,
        }}
        title={title}
      />
      <TopicCreateDialog
        broker={broker}
        isOpen={isCreateDialogOpen}
        onClose={() => setIsCreateDialogOpen(false)}
        onConfirm={onCreate}
        topics={data}
      />
      <TopicDeleteDialog
        isOpen={isDeleteDialogOpen}
        onClose={() => setIsDeleteDialogOpen(false)}
        onConfirm={onDelete}
        topic={activeTopic}
      />
      <TopicDetailDialog
        isOpen={isDetailDialogOpen}
        onClose={() => setIsDetailDialogOpen(false)}
        topic={activeTopic}
      />
    </>
  );
}

TopicTable.propTypes = {
  broker: PropTypes.shape({
    state: PropTypes.string,
  }),
  topics: PropTypes.arrayOf(
    PropTypes.shape({
      name: PropTypes.string,
      numberOfPartitions: PropTypes.number,
      numberOfReplications: PropTypes.number,
      pipelines: PropTypes.arrayOf(
        PropTypes.shape({
          name: PropTypes.string,
        }),
      ),
      state: PropTypes.string,
      tags: PropTypes.shape({
        displayName: PropTypes.string,
        isShared: PropTypes.bool,
      }),
    }),
  ),
  onCreate: PropTypes.func,
  onDelete: PropTypes.func,
  onLinkClick: PropTypes.func,
  options: PropTypes.shape({
    onCreateIconClick: PropTypes.func,
    onDeleteIconClick: PropTypes.func,
    onDetailIconClick: PropTypes.func,
    prompt: PropTypes.string,
    showCreateIcon: PropTypes.bool,
    showDeleteIcon: PropTypes.bool,
    showDetailIcon: PropTypes.bool,
    showTitle: PropTypes.bool,
  }),
  title: PropTypes.string,
};

TopicTable.defaultProps = {
  broker: null,
  topics: [],
  onCreate: () => {},
  onDelete: () => {},
  onLinkClick: () => {},
  options: defaultOptions,
  title: 'Topics',
};

export default TopicTable;

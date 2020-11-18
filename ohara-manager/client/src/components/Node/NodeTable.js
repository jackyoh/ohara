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
import {
  flatMap,
  filter,
  find,
  isEmpty,
  isFunction,
  map,
  round,
  sortBy,
  size,
  unionBy,
  uniq,
} from 'lodash';

import LinearProgress from '@material-ui/core/LinearProgress';
import Link from '@material-ui/core/Link';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';

import { Actions, MuiTable as Table } from 'components/common/Table';
import { KIND } from 'const';
import NodeCreateDialog from './NodeCreateDialog';
import NodeDeleteDialog from './NodeDeleteDialog';
import NodeDetailDialog from './NodeDetailDialog';
import NodeEditorDialog from './NodeEditorDialog';
import NodeStateChip from './NodeStateChip';

const defaultOptions = {
  comparison: false,
  comparedNodes: [],
  customColumns: [],
  disabledEditIcon: false,
  disabledDeleteIcon: false,
  disabledRemoveIcon: false,
  onCreateIconClick: null,
  onDeleteIconClick: null,
  onDetailIconClick: null,
  onEditorIconClick: null,
  onUndoIconClick: null,
  onRefreshIconClick: null,
  selection: false,
  selectedNodes: [],
  disabledNodes: [],
  showAddIcon: false,
  showCreateIcon: true,
  showDeleteIcon: true,
  showDetailIcon: true,
  showEditorIcon: true,
  showUndoIcon: false,
  showRefreshIcon: false,
  showRemoveIcon: false,
  showTitle: true,
  showServicesColumn: true,
};

const getUnionResourceNames = (nodes) =>
  uniq(
    flatMap(nodes, (node) => map(node.resources, (resource) => resource.name)),
  );

function NodeTable(props) {
  const {
    nodes,
    onCreate,
    onDelete,
    onUpdate,
    onSelectionChange,
    title,
  } = props;

  const options = { ...defaultOptions, ...props?.options };

  const [activeNode, setActiveNode] = useState();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [isDetailDialogOpen, setIsDetailDialogOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);
  const [isEditorDialogOpen, setIsEditorDialogOpen] = useState(false);

  const data = options?.comparison
    ? sortBy(unionBy(options?.comparedNodes, nodes, 'hostname'), ['hostname'])
    : nodes;

  const resourceNames = getUnionResourceNames(data);

  const willBeRemoved = (node) =>
    !find(nodes, (n) => n.hostname === node.hostname);

  const willBeAdded = (node) =>
    !isEmpty(options?.comparedNodes) &&
    !find(options?.comparedNodes, (n) => n.hostname === node.hostname);

  const handleAddIconClick = () => {
    if (isFunction(options?.onAddIconClick)) {
      options.onAddIconClick();
    }
  };

  const handleCreateIconClick = () => {
    if (isFunction(options?.onCreateIconClick)) {
      options.onCreateIconClick();
    } else {
      setIsCreateDialogOpen(true);
    }
  };

  const handleDeleteIconClick = (node) => {
    if (isFunction(options?.onDeleteIconClick)) {
      options.onDeleteIconClick(node);
    } else {
      setIsDeleteDialogOpen(true);
      setActiveNode(node);
    }
  };

  const handleDetailIconClick = (node) => {
    if (isFunction(options?.onDetailIconClick)) {
      options.onDetailIconClick(node);
    } else {
      setIsDetailDialogOpen(true);
      setActiveNode(node);
    }
  };

  const handleEditorIconClick = (node) => {
    if (isFunction(options?.onEditorIconClick)) {
      options.onEditorIconClick(node);
    } else {
      setIsEditorDialogOpen(true);
      setActiveNode(node);
    }
  };

  const handleUndoIconClick = (node) => {
    if (isFunction(options?.onUndoIconClick)) {
      options.onUndoIconClick(node);
    }
  };

  const handleRefreshIconClick = () => {
    if (isFunction(options?.onRefreshIconClick)) {
      options.onRefreshIconClick();
    }
  };

  const handleEditorDialogConfirm = (nodeToUpdate) => {
    onUpdate(nodeToUpdate);
    setIsEditorDialogOpen(false);
  };

  const renderResourceColumns = () => {
    return map(resourceNames, (resourceName) => ({
      title: resourceName,
      render: (node) => {
        const resource = find(node.resources, (r) => r.name === resourceName);
        if (!resource) return;
        const { unit, used, value } = resource;
        return (
          <>
            <Typography variant="subtitle2">
              {round(value, 1)} {unit}
            </Typography>
            <Tooltip title={`${round(used * 100, 1)} %`}>
              <LinearProgress
                color={used > 0.8 ? 'secondary' : 'primary'}
                value={used * 100}
                variant="determinate"
              />
            </Tooltip>
          </>
        );
      },
    }));
  };

  const renderServiceColumn = () => {
    return {
      title: 'Services',
      hidden: !options?.showServicesColumn,
      render: (node) => {
        const services = filter(
          node.services,
          (service) => service.name !== KIND.configurator,
        );
        const clusters = flatMap(services, (service) => service.clusterKeys);
        const count = size(clusters);
        if (count > 0) {
          return (
            <Typography>
              {}
              <Link
                component="button"
                onClick={(event) => {
                  handleDetailIconClick(node);
                  event.stopPropagation();
                }}
                variant="h6"
              >
                {count}
              </Link>
            </Typography>
          );
        } else {
          return <Typography>{count}</Typography>;
        }
      },
    };
  };

  const renderRowActions = () => {
    const isShow =
      options?.showDeleteIcon ||
      options?.showDetailIcon ||
      options?.showEditorIcon ||
      options?.showRemoveIcon;

    const render = (node) => {
      const getUndoTooltipTitle = (node) => {
        if (willBeAdded(node)) {
          return 'Undo add node';
        } else if (willBeRemoved(node)) {
          return 'Undo remove node';
        }
        return 'Undo';
      };

      const showUndoIcon = (node) =>
        (options?.comparison && willBeAdded(node)) || willBeRemoved(node);

      const disabledEditIcon = isFunction(options?.disabledEditIcon)
        ? options?.disabledEditIcon(node)
        : options?.disabledEditIcon;

      const disabledDeleteIcon = isFunction(options?.disabledDeleteIcon)
        ? options?.disabledDeleteIcon(node)
        : options?.disabledDeleteIcon;

      const editTooltip = isFunction(options?.editTooltip)
        ? options?.editTooltip(node)
        : options?.editTooltip;

      const deleteTooltip = isFunction(options?.deleteTooltip)
        ? options?.deleteTooltip(node)
        : options?.deleteTooltip;

      return (
        <Actions
          actions={[
            {
              name: 'view',
              onClick: handleDetailIconClick,
              tooltip: 'View node',
              testid: `view-node-${node.hostname}`,
            },
            {
              disabled: disabledEditIcon,
              hidden: !options?.showEditorIcon,
              name: 'edit',
              onClick: handleEditorIconClick,
              tooltip: editTooltip || 'Edit node',
              testid: `edit-node-${node.hostname}`,
            },
            {
              disabled: disabledDeleteIcon,
              hidden: !options?.showDeleteIcon,
              name: 'delete',
              onClick: handleDeleteIconClick,
              tooltip: deleteTooltip || 'Delete node',
              testid: `delete-node-${node.hostname}`,
            },
            {
              hidden: !showUndoIcon(node),
              name: 'undo',
              onClick: handleUndoIconClick,
              tooltip: getUndoTooltipTitle(node),
            },
          ]}
          data={node}
        />
      );
    };

    return {
      cellStyle: { textAlign: 'right' },
      headerStyle: { textAlign: 'right' },
      hidden: !isShow,
      render,
      sorting: false,
      title: 'Actions',
    };
  };

  const getRowStyle = (node) => {
    if (options?.comparison && willBeRemoved(node)) {
      return {
        backgroundColor: 'rgba(255, 117, 159, 0.1)',
      };
    } else if (options?.comparison && willBeAdded(node)) {
      return {
        backgroundColor: 'rgba(114, 204, 255, 0.1)',
      };
    }
    return null;
  };

  return (
    <>
      <Table
        actions={[
          {
            hidden: !options?.showAddIcon,
            icon: 'add',
            isFreeAction: true,
            onClick: handleAddIconClick,
            tooltip: 'Add Node',
          },
          {
            hidden: !options?.showCreateIcon,
            icon: 'add',
            isFreeAction: true,
            onClick: handleCreateIconClick,
            tooltip: 'Create Node',
          },
          {
            hidden: !options?.showRefreshIcon,
            icon: 'refresh',
            isFreeAction: true,
            onClick: handleRefreshIconClick,
            tooltip: 'Refresh Nodes',
          },
        ]}
        columns={[
          { title: 'Name', field: 'hostname' },
          ...renderResourceColumns(),
          renderServiceColumn(),
          {
            title: 'State',
            field: 'state',
            render: (node) => <NodeStateChip node={node} />,
          },
          ...options?.customColumns,
          renderRowActions(),
        ]}
        data={data}
        onSelectionChange={onSelectionChange}
        options={{
          predicate: 'hostname',
          prompt: options?.prompt,
          rowStyle: (node) => getRowStyle(node),
          selection: options?.selection,
          selectedData: options?.selectedNodes,
          disabledData: options?.disabledNodes,
          showTitle: options?.showTitle,
        }}
        title={title}
      />

      <NodeCreateDialog
        isOpen={isCreateDialogOpen}
        onClose={() => setIsCreateDialogOpen(false)}
        onConfirm={onCreate}
      />
      <NodeDeleteDialog
        isOpen={isDeleteDialogOpen}
        node={activeNode}
        onClose={() => setIsDeleteDialogOpen(false)}
        onConfirm={onDelete}
      />
      <NodeDetailDialog
        isOpen={isDetailDialogOpen}
        node={activeNode}
        onClose={() => setIsDetailDialogOpen(false)}
      />
      <NodeEditorDialog
        isOpen={isEditorDialogOpen}
        node={activeNode}
        onClose={() => setIsEditorDialogOpen(false)}
        onConfirm={handleEditorDialogConfirm}
      />
    </>
  );
}

NodeTable.propTypes = {
  nodes: PropTypes.arrayOf(
    PropTypes.shape({
      hostname: PropTypes.string,
      state: PropTypes.string,
      resources: PropTypes.arrayOf(
        PropTypes.shape({
          name: PropTypes.string,
          unit: PropTypes.string,
          used: PropTypes.number,
          value: PropTypes.number,
        }),
      ),
      services: PropTypes.arrayOf(
        PropTypes.shape({
          name: PropTypes.string,
          clusterKeys: PropTypes.array,
        }),
      ),
    }),
  ),
  onCreate: PropTypes.func,
  onDelete: PropTypes.func,
  onUpdate: PropTypes.func,
  onSelectionChange: PropTypes.func,
  options: PropTypes.shape({
    comparison: PropTypes.bool,
    comparedNodes: PropTypes.array,
    customColumns: PropTypes.arrayOf(
      PropTypes.shape({
        customFilterAndSearch: PropTypes.func,
        field: PropTypes.string,
        render: PropTypes.func,
        title: PropTypes.string,
        type: PropTypes.string,
      }),
    ),
    disabledEditIcon: PropTypes.oneOfType([PropTypes.bool, PropTypes.func]),
    disabledDeleteIcon: PropTypes.oneOfType([PropTypes.bool, PropTypes.func]),
    disabledRemoveIcon: PropTypes.oneOfType([PropTypes.bool, PropTypes.func]),
    onAddIconClick: PropTypes.func,
    onCreateIconClick: PropTypes.func,
    onDeleteIconClick: PropTypes.func,
    onDetailIconClick: PropTypes.func,
    onEditorIconClick: PropTypes.func,
    onUndoIconClick: PropTypes.func,
    onRefreshIconClick: PropTypes.func,
    prompt: PropTypes.string,
    selection: PropTypes.bool,
    selectedNodes: PropTypes.array,
    disabledNodes: PropTypes.array,
    showAddIcon: PropTypes.bool,
    showCreateIcon: PropTypes.bool,
    showDeleteIcon: PropTypes.bool,
    showDetailIcon: PropTypes.bool,
    showEditorIcon: PropTypes.bool,
    showUndoIcon: PropTypes.bool,
    showRefreshIcon: PropTypes.bool,
    showRemoveIcon: PropTypes.bool,
    showTitle: PropTypes.bool,
    showServicesColumn: PropTypes.bool,
    editTooltip: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
    deleteTooltip: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
    removeTooltip: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
  }),
  title: PropTypes.string,
};

NodeTable.defaultProps = {
  nodes: [],
  onCreate: () => {},
  onDelete: () => {},
  onUpdate: () => {},
  onSelectionChange: () => {},
  options: defaultOptions,
  title: 'Nodes',
};

export default NodeTable;

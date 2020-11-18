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

import { useState, useRef } from 'react';
import PropTypes from 'prop-types';
import { find, isFunction, sortBy, unionBy } from 'lodash';
import moment from 'moment';
import NumberFormat from 'react-number-format';

import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';

import { Actions, MuiTable as Table } from 'components/common/Table';
import FileClassInfoDialog from './FileClassInfoDialog';
import FileDeleteDialog from './FileDeleteDialog';
import FileRemoveDialog from './FileRemoveDialog';
import FileDownload from './FileDownload';
import FileUpload from './FileUpload';

const defaultOptions = {
  comparison: false,
  comparedFiles: [],
  customColumns: [],
  disabledDeleteIcon: false,
  disabledRemoveIcon: false,
  onAddIconClick: null,
  onDeleteIconClick: null,
  onDownloadIconClick: null,
  onUndoIconClick: null,
  onUploadIconClick: null,
  onRemoveIconClick: null,
  selection: false,
  selectedFiles: [],
  showAddIcon: false,
  showDeleteIcon: true,
  showDownloadIcon: true,
  showUndoIcon: false,
  showUploadIcon: true,
  showRemoveIcon: false,
  showTitle: true,
  showUsedColumn: false,
};

function FileTable(props) {
  const {
    files,
    onDelete,
    onUpload,
    onRemove,
    onSelectionChange,
    title,
  } = props;
  const options = { ...defaultOptions, ...props.options };

  const fileUploadRef = useRef(null);
  const fileDownloadRef = useRef(null);

  const [activeFile, setActiveFile] = useState();
  const [isClassInfoDialogOpen, setIsClassInfoDialogOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);
  const [isRemoveDialogOpen, setIsRemoveDialogOpen] = useState(false);

  const data = options?.comparison
    ? sortBy(unionBy(options?.comparedFiles, files, 'name'), ['name'])
    : files;

  const willBeRemoved = (file) => !find(files, (f) => f.name === file.name);

  const willBeAdded = (file) =>
    !find(options?.comparedFiles, (f) => f.name === file.name);

  const handleAddIconClick = () => {
    if (isFunction(options?.onAddIconClick)) {
      options.onAddIconClick();
    }
  };

  const handleDeleteIconClick = (file) => {
    if (isFunction(options?.onDeleteIconClick)) {
      options.onDeleteIconClick(file);
    } else {
      setIsDeleteDialogOpen(true);
      setActiveFile(file);
    }
  };

  const handleDownloadIconClick = (file) => {
    if (isFunction(options?.onDownloadIconClick)) {
      options.onDownloadIconClick(file);
    } else {
      setActiveFile(file);
      setTimeout(() => fileDownloadRef.current.click(), 500);
    }
  };

  const handleUndoIconClick = (file) => {
    if (isFunction(options?.onUndoIconClick)) {
      options.onUndoIconClick(file);
    }
  };

  const handleRemoveIconClick = (file) => {
    if (isFunction(options?.onRemoveIconClick)) {
      options.onRemoveIconClick(file);
    } else {
      setIsRemoveDialogOpen(true);
      setActiveFile(file);
    }
  };

  const handleUploadIconClick = () => {
    if (isFunction(options?.onUploadIconClick)) {
      options.onUploadIconClick();
    } else {
      fileUploadRef.current.click();
    }
  };

  const handleViewIconClick = (file) => {
    setIsClassInfoDialogOpen(true);
    setActiveFile(file);
  };

  const handleDeleteDialogConfirm = (fileToDelete) => {
    onDelete(fileToDelete);
    setIsDeleteDialogOpen(false);
  };

  const handleRemoveDialogConfirm = (fileToRemove) => {
    onRemove(fileToRemove);
    setIsRemoveDialogOpen(false);
  };

  const renderRowActions = () => {
    const isShow =
      options?.showDeleteIcon ||
      options?.showDownloadIcon ||
      options?.showRemoveIcon;

    const render = (file) => {
      const getUndoTooltipTitle = (file) => {
        if (willBeAdded(file)) {
          return 'Undo add file';
        } else if (willBeRemoved(file)) {
          return 'Undo remove file';
        }
        return 'Undo';
      };

      const showUndoIcon = (file) =>
        (options?.comparison && willBeAdded(file)) || willBeRemoved(file);

      const showRemoveIcon = (file) =>
        options?.showRemoveIcon && !showUndoIcon(file);

      const disabledDeleteIcon = isFunction(options?.disabledDeleteIcon)
        ? options?.disabledDeleteIcon(file)
        : options?.disabledDeleteIcon;

      const disabledRemoveIcon = isFunction(options?.disabledRemoveIcon)
        ? options?.disabledRemoveIcon(file)
        : options?.disabledRemoveIcon;

      const deleteTooltip = isFunction(options?.deleteTooltip)
        ? options?.deleteTooltip(file)
        : options?.deleteTooltip;

      const removeTooltip = isFunction(options?.removeTooltip)
        ? options?.removeTooltip(file)
        : options?.removeTooltip;

      return (
        <Actions
          actions={[
            {
              name: 'view',
              onClick: handleViewIconClick,
              tooltip: 'View file',
            },
            {
              hidden: !options?.showDownloadIcon,
              name: 'download',
              onClick: handleDownloadIconClick,
              tooltip: 'Download file',
            },
            {
              disabled: disabledDeleteIcon,
              hidden: !options?.showDeleteIcon,
              name: 'delete',
              onClick: handleDeleteIconClick,
              tooltip: deleteTooltip || 'Delete file',
            },
            {
              disabled: disabledRemoveIcon,
              hidden: !showRemoveIcon(file),
              name: 'remove',
              onClick: handleRemoveIconClick,
              tooltip: removeTooltip || 'Remove file',
            },
            {
              hidden: !showUndoIcon(file),
              name: 'undo',
              onClick: handleUndoIconClick,
              tooltip: getUndoTooltipTitle(file),
            },
          ]}
          data={file}
        />
      );
    };

    return {
      title: 'Actions',
      cellStyle: { textAlign: 'right' },
      headerStyle: { textAlign: 'right' },
      hidden: !isShow,
      render,
      sorting: false,
    };
  };

  const getRowStyle = (file) => {
    if (options?.comparison && willBeRemoved(file)) {
      return {
        backgroundColor: 'rgba(255, 117, 159, 0.1)',
      };
    } else if (options?.comparison && willBeAdded(file)) {
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
            icon: 'add',
            tooltip: 'Add File',
            hidden: !options?.showAddIcon,
            isFreeAction: true,
            onClick: handleAddIconClick,
          },
          {
            icon: 'cloud_upload',
            tooltip: 'Upload File',
            hidden: !options?.showUploadIcon,
            isFreeAction: true,
            onClick: handleUploadIconClick,
          },
        ]}
        columns={[
          { title: 'Name', field: 'name' },
          {
            title: 'File size(KB)',
            type: 'numeric',
            field: 'size',
            render: (file) => (
              <NumberFormat
                displayType="text"
                thousandSeparator
                value={file.size}
              />
            ),
          },
          ...options?.customColumns,
          {
            title: 'Last modified',
            type: 'date',
            field: 'lastModified',
            render: (file) => {
              return (
                <Tooltip
                  title={moment(file?.lastModified).format(
                    'YYYY/MM/DD HH:mm:ss',
                  )}
                >
                  <Typography>
                    {moment(file?.lastModified).fromNow()}
                  </Typography>
                </Tooltip>
              );
            },
          },
          renderRowActions(),
        ]}
        data={data}
        onSelectionChange={onSelectionChange}
        options={{
          predicate: 'name',
          prompt: options?.prompt,
          rowStyle: (file) => getRowStyle(file),
          selection: options?.selection,
          selectedData: options?.selectedFiles,
          showTitle: options?.showTitle,
        }}
        title={title}
      />
      <FileClassInfoDialog
        file={activeFile}
        isOpen={isClassInfoDialogOpen}
        onClose={() => setIsClassInfoDialogOpen(false)}
      />
      <FileDeleteDialog
        file={activeFile}
        isOpen={isDeleteDialogOpen}
        onClose={() => setIsDeleteDialogOpen(false)}
        onConfirm={handleDeleteDialogConfirm}
      />
      <FileRemoveDialog
        file={activeFile}
        isOpen={isRemoveDialogOpen}
        onClose={() => setIsRemoveDialogOpen(false)}
        onConfirm={handleRemoveDialogConfirm}
      />
      <FileUpload onUpload={onUpload} ref={fileUploadRef} />
      <FileDownload file={activeFile} ref={fileDownloadRef} />
    </>
  );
}

FileTable.propTypes = {
  files: PropTypes.arrayOf(
    PropTypes.shape({
      lastModified: PropTypes.number,
      name: PropTypes.string,
      size: PropTypes.number,
    }),
  ),
  onDelete: PropTypes.func,
  onRemove: PropTypes.func,
  onSelectionChange: PropTypes.func,
  onUpload: PropTypes.func,
  options: PropTypes.shape({
    comparison: PropTypes.bool,
    comparedFiles: PropTypes.array,
    customColumns: PropTypes.arrayOf(
      PropTypes.shape({
        customFilterAndSearch: PropTypes.func,
        field: PropTypes.string,
        render: PropTypes.func,
        title: PropTypes.string,
        type: PropTypes.string,
      }),
    ),
    disabledDeleteIcon: PropTypes.oneOfType([PropTypes.bool, PropTypes.func]),
    disabledRemoveIcon: PropTypes.oneOfType([PropTypes.bool, PropTypes.func]),
    mode: PropTypes.string,
    onAddIconClick: PropTypes.func,
    onDeleteIconClick: PropTypes.func,
    onDownloadIconClick: PropTypes.func,
    onUndoIconClick: PropTypes.func,
    onUploadIconClick: PropTypes.func,
    onRemoveIconClick: PropTypes.func,
    prompt: PropTypes.string,
    selection: PropTypes.bool,
    selectedFiles: PropTypes.array,
    showAddIcon: PropTypes.bool,
    showDeleteIcon: PropTypes.bool,
    showDownloadIcon: PropTypes.bool,
    showUndoIcon: PropTypes.bool,
    showUploadIcon: PropTypes.bool,
    showRemoveIcon: PropTypes.bool,
    showTitle: PropTypes.bool,
    deleteTooltip: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
    removeTooltip: PropTypes.oneOfType([PropTypes.string, PropTypes.func]),
  }),
  title: PropTypes.string,
};

FileTable.defaultProps = {
  files: [],
  onDelete: () => {},
  onRemove: () => {},
  onSelectionChange: () => {},
  onUpload: () => {},
  options: defaultOptions,
  title: 'Files',
};

export default FileTable;

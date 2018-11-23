// Success messages
export const LOGIN_SUCCESS = 'You are now logged in!';
export const LOGOUT_SUCCESS = 'You are now logged out!';

export const SCHEMA_CREATION_SUCCESS = 'Schema successfully created!';
export const TOPIC_CREATION_SUCCESS = 'Topic successfully created!';
export const PIPELINE_CREATION_SUCCESS = 'New pipeline has been created!';
export const PIPELINE_DELETION_SUCCESS = 'Successfully deleted the pipeline:';
export const CONFIG_SAVE_SUCCESS = 'Configuration successfully saved!';
export const TEST_SUCCESS = 'Test has passed!';

// Error messages
export const EMPTY_COLUMN_NAME_ERROR = 'Column Name is a required field!';
export const EMPTY_SCHEMA_NAME_ERROR = 'Schema name is a required field!';
export const EMPTY_SCHEMAS_COLUMNS_ERROR =
  'Please supply at least a Schema column name!';
export const DUPLICATED_COLUMN_NAME_ERROR = 'Column Name cannot be repeated';
export const ONLY_NUMBER_ALLOW_ERROR =
  'partition or replication only accept numeric values';
export const TOPIC_ID_REQUIRED_ERROR =
  'You need to select a topi before creating a new pipeline!';
export const NO_TOPICS_FOUND_ERROR = `You don't have any topics!`;
export const INVALID_TOPIC_ID = `The selected topic doesn't exist!`;

// Pipelines
export const PIPELINE_DELETION_ERROR =
  'Oops, something went wrong, we cannot delete the selected pipeline:';
export const CANNOT_START_PIPELINE_ERROR =
  'Cannot complete your action, please check your connector settings';

export const NO_CONFIGURATION_FOUND_ERROR = `You don't any configuration set up yet, please create one before you can proceed`;

export const EMPTY_PIPELINE_TITLE_ERROR = 'Pipeline title cannot be empty!';
export const CANNOT_UPDATE_WHILE_RUNNING_ERROR = `You cannot update the pipeline while it's running`;

export const GENERIC_ERROR = 'Oops, something went wrong 😱 😱 😱';

// Warning
export const LEAVE_WITHOUT_SAVE =
  'You have unsaved changes or pending requests, are you sure you want to leave?';

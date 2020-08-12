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

const fs = require('fs');
const path = require('path');

module.exports = (app) => {
  // This route provides manager's version information for
  // our UI to consume. It reads a JSON file which is generated
  // by our Backend code
  app.get('/api/inspect/manager', (req, res) => {
    // For QA TEST
    // OHARA_MANAGER_RUNNING_MODE is set in ohara/bin/ohara-env.sh
    // NODE_ENV is set in npm scripts
    const isProduction =
      process.env.OHARA_MANAGER_NODE_ENV === 'production' ||
      process.env.NODE_ENV === 'production';

    try {
      const filePath = isProduction
        ? '/home/ohara/default/bin/ohara_version'
        : path.resolve(
            __dirname,
            '../../ohara-common/build/generated/ohara_version',
          );

      const file = fs.readFileSync(filePath);
      const versionInfo = JSON.parse(file);
      res.json(versionInfo);
    } catch (error) {
      res.json(400, {
        code: 'N/A',
        message: `Oops, something went wrong! We Couldn't obtain manager's version information`,
        stack: 'N/A',
      });
    }
  });

  return app;
};

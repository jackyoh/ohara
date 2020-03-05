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

package oharastream.ohara.kafka.connector.csv;

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.kafka.connector.TaskSetting;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import oharastream.ohara.kafka.connector.storage.FileType;
import org.junit.Assert;
import org.junit.Test;

public class TestCsvSourceTask extends OharaTest {

  @Test
  public void testFileQueue() {
    CsvSourceTask sourceTask = new MockCsvSourceTask();
    Map<String, String> map = new HashMap<String, String>();

    // The setting is fake
    map.put(INPUT_FOLDER_KEY, "/input");
    map.put(TASK_TOTAL_KEY, "1");
    map.put(TASK_HASH_KEY, "10");
    map.put(LIST_FILE_QUEUE_NUMBER_KEY, "3");

    sourceTask.run(TaskSetting.of(map));
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 0);

    sourceTask.pollRecords();
    // First poll the element, so queue size is 3 - 1 equals 2
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 2);

    sourceTask.pollRecords();
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 1);

    sourceTask.pollRecords();
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 0);

    sourceTask.pollRecords();
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 2);

    sourceTask.pollRecords();
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 1);

    sourceTask.pollRecords();
    Assert.assertEquals(sourceTask.getFileNameQueue().size(), 0);
  }
}

class MockCsvSourceTask extends CsvSourceTask {

  @Override
  public FileSystem fileSystem(TaskSetting config) {
    return new FileSystem() {
      @Override
      public boolean exists(String path) {
        return false;
      }

      @Override
      public Iterator<String> listFileNames(String dir) {
        return IntStream.range(1, 100)
            .boxed()
            .map(i -> "file" + i)
            .collect(Collectors.toList())
            .iterator();
      }

      @Override
      public FileType fileType(String path) {
        return FileType.FILE;
      }

      @Override
      public OutputStream create(String path) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public OutputStream append(String path) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public InputStream open(String path) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public void delete(String path) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public void delete(String path, boolean recursive) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public boolean moveFile(String sourcePath, String targetPath) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public void mkdirs(String dir) {
        throw new UnsupportedOperationException("Mock not support this function");
      }

      @Override
      public void close() {
        throw new UnsupportedOperationException("Mock not support this function");
      }
    };
  }
}

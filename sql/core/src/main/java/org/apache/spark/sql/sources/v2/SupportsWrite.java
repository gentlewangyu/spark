/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.sources.v2.writer.BatchWrite;
import org.apache.spark.sql.sources.v2.writer.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
<<<<<<< HEAD:sql/core/src/main/java/org/apache/spark/sql/sources/v2/DataSourceV2.java
 * The base interface for data source v2. Implementations must have a public, 0-arg constructor.
 *
 * Note that this is an empty interface. Data source implementations should mix-in at least one of
 * the plug-in interfaces like {@link ReadSupport} and {@link WriteSupport}. Otherwise it's just
 * a dummy data source which is un-readable/writable.
=======
 * A mix-in interface of {@link Table}, to indicate that it's writable. This adds
 * {@link #newWriteBuilder(CaseInsensitiveStringMap)} that is used to create a write
 * for batch or streaming.
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da:sql/core/src/main/java/org/apache/spark/sql/sources/v2/SupportsWrite.java
 */
public interface SupportsWrite extends Table {

  /**
   * Returns a {@link WriteBuilder} which can be used to create {@link BatchWrite}. Spark will call
   * this method to configure each data source write.
   */
  WriteBuilder newWriteBuilder(CaseInsensitiveStringMap options);
}

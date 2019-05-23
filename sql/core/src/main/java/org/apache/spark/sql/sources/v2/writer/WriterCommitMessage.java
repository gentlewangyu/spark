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

package org.apache.spark.sql.sources.v2.writer;

import java.io.Serializable;

<<<<<<< HEAD
import org.apache.spark.annotation.InterfaceStability;

/**
 * A commit message returned by {@link DataWriter#commit()} and will be sent back to the driver side
 * as the input parameter of {@link DataSourceWriter#commit(WriterCommitMessage[])}.
=======
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamingWrite;

/**
 * A commit message returned by {@link DataWriter#commit()} and will be sent back to the driver side
 * as the input parameter of {@link BatchWrite#commit(WriterCommitMessage[])} or
 * {@link StreamingWrite#commit(long, WriterCommitMessage[])}.
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
 *
 * This is an empty interface, data sources should define their own message class and use it in
 * their {@link DataWriter#commit()} and {@link DataSourceWriter#commit(WriterCommitMessage[])}
 * implementations.
 */
@Evolving
public interface WriterCommitMessage extends Serializable {}

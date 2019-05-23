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

package org.apache.spark.sql.streaming.sources

<<<<<<< HEAD
import java.util.Optional
=======
import java.util
import java.util.Collections

import scala.collection.JavaConverters._
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, Sink, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.sources.v2._
<<<<<<< HEAD
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader, Offset, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
=======
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.sources.v2.writer.{WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.{StreamingDataWriterFactory, StreamingWrite}
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

<<<<<<< HEAD
case class FakeReader() extends MicroBatchReader with ContinuousReader {
  def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {}
  def getStartOffset: Offset = RateStreamOffset(Map())
  def getEndOffset: Offset = RateStreamOffset(Map())
  def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  def commit(end: Offset): Unit = {}
  def readSchema(): StructType = StructType(Seq())
  def stop(): Unit = {}
  def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  def setStartOffset(start: Optional[Offset]): Unit = {}

  def planInputPartitions(): java.util.ArrayList[InputPartition[InternalRow]] = {
=======
class FakeDataStream extends MicroBatchStream with ContinuousStream {
  override def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
  override def initialOffset(): Offset = RateStreamOffset(Map())
  override def latestOffset(): Offset = RateStreamOffset(Map())
  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    throw new IllegalStateException("fake source - cannot actually read")
  }
}

<<<<<<< HEAD
trait FakeMicroBatchReadSupport extends MicroBatchReadSupport {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = {
    LastReadOptions.options = options
    FakeReader()
  }
}

trait FakeContinuousReadSupport extends ContinuousReadSupport {
  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    LastReadOptions.options = options
    FakeReader()
  }
}

trait FakeStreamWriteSupport extends StreamWriteSupport {
  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    LastWriteOptions.options = options
    throw new IllegalStateException("fake sink - cannot actually write")
=======
class FakeScanBuilder extends ScanBuilder with Scan {
  override def build(): Scan = this
  override def readSchema(): StructType = StructType(Seq())
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = new FakeDataStream
  override def toContinuousStream(checkpointLocation: String): ContinuousStream = new FakeDataStream
}

class FakeWriteBuilder extends WriteBuilder with StreamingWrite {
  override def buildForStreaming(): StreamingWrite = this
  override def createStreamingWriterFactory(): StreamingDataWriterFactory = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
}

trait FakeStreamingWriteTable extends Table with SupportsWrite {
  override def name(): String = "fake"
  override def schema(): StructType = StructType(Seq())
  override def capabilities(): util.Set[TableCapability] = {
    Set(STREAMING_WRITE).asJava
  }
  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    new FakeWriteBuilder
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
  }
}

class FakeReadMicroBatchOnly
    extends DataSourceRegister
<<<<<<< HEAD
    with FakeMicroBatchReadSupport
=======
    with TableProvider
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    with SessionConfigSupport {
  override def shortName(): String = "fake-read-microbatch-only"

  override def keyPrefix: String = shortName()
<<<<<<< HEAD
=======

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastReadOptions.options = options
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        Set(MICRO_BATCH_READ).asJava
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
}

class FakeReadContinuousOnly
    extends DataSourceRegister
<<<<<<< HEAD
    with FakeContinuousReadSupport
=======
    with TableProvider
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    with SessionConfigSupport {
  override def shortName(): String = "fake-read-continuous-only"

  override def keyPrefix: String = shortName()
<<<<<<< HEAD
}

class FakeReadBothModes extends DataSourceRegister
    with FakeMicroBatchReadSupport with FakeContinuousReadSupport {
=======

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastReadOptions.options = options
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        Set(CONTINUOUS_READ).asJava
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
}

class FakeReadBothModes extends DataSourceRegister with TableProvider {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
  override def shortName(): String = "fake-read-microbatch-continuous"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        Set(MICRO_BATCH_READ, CONTINUOUS_READ).asJava
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
}

class FakeReadNeitherMode extends DataSourceRegister with TableProvider {
  override def shortName(): String = "fake-read-neither-mode"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
      override def capabilities(): util.Set[TableCapability] = Collections.emptySet()
    }
  }
}

<<<<<<< HEAD
class FakeWrite
    extends DataSourceRegister
    with FakeStreamWriteSupport
=======
class FakeWriteOnly
    extends DataSourceRegister
    with TableProvider
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    with SessionConfigSupport {
  override def shortName(): String = "fake-write-microbatch-continuous"

  override def keyPrefix: String = shortName()
<<<<<<< HEAD
=======

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastWriteOptions.options = options
    new Table with FakeStreamingWriteTable {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
    }
  }
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
}

class FakeNoWrite extends DataSourceRegister with TableProvider {
  override def shortName(): String = "fake-write-neither-mode"
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
      override def capabilities(): util.Set[TableCapability] = Collections.emptySet()
    }
  }
}

case class FakeWriteV1FallbackException() extends Exception

class FakeSink extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {}
}

<<<<<<< HEAD
class FakeWriteV1Fallback extends DataSourceRegister
  with FakeStreamWriteSupport with StreamSinkProvider {
=======
class FakeWriteSupportProviderV1Fallback extends DataSourceRegister
  with TableProvider with StreamSinkProvider {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new FakeSink()
  }

  override def shortName(): String = "fake-write-v1-fallback"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with FakeStreamingWriteTable {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
    }
  }
}

object LastReadOptions {
  var options: CaseInsensitiveStringMap = _

  def clear(): Unit = {
    options = null
  }
}

<<<<<<< HEAD
object LastReadOptions {
  var options: DataSourceOptions = _

  def clear(): Unit = {
    options = null
  }
}

object LastWriteOptions {
  var options: DataSourceOptions = _
=======
object LastWriteOptions {
  var options: CaseInsensitiveStringMap = _
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

  def clear(): Unit = {
    options = null
  }
}

class StreamingDataSourceV2Suite extends StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set("spark.sql.streaming.checkpointLocation", fakeCheckpoint.getCanonicalPath)
  }

  override def afterEach(): Unit = {
    LastReadOptions.clear()
    LastWriteOptions.clear()
  }

  val readFormats = Seq(
    "fake-read-microbatch-only",
    "fake-read-continuous-only",
    "fake-read-microbatch-continuous",
    "fake-read-neither-mode")
  val writeFormats = Seq(
    "fake-write-microbatch-continuous",
    "fake-write-neither-mode")
  val triggers = Seq(
    Trigger.Once(),
    Trigger.ProcessingTime(1000),
    Trigger.Continuous(1000))

  private def testPositiveCase(readFormat: String, writeFormat: String, trigger: Trigger): Unit = {
    testPositiveCaseWithQuery(readFormat, writeFormat, trigger)(() => _)
  }

  private def testPositiveCaseWithQuery(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger)(check: StreamingQuery => Unit): Unit = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()
    check(query)
    query.stop()
  }

  private def testNegativeCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val ex = intercept[UnsupportedOperationException] {
      testPositiveCase(readFormat, writeFormat, trigger)
    }
    assert(ex.getMessage.contains(errorMsg))
  }

  private def testPostCreationNegativeCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()

    eventually(timeout(streamingTimeout)) {
      assert(query.exception.isDefined)
      assert(query.exception.get.cause != null)
      assert(query.exception.get.cause.getMessage.contains(errorMsg))
    }
  }

  test("disabled v2 write") {
    // Ensure the V2 path works normally and generates a V2 sink..
    testPositiveCaseWithQuery(
      "fake-read-microbatch-continuous", "fake-write-v1-fallback", Trigger.Once()) { v2Query =>
      assert(v2Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
<<<<<<< HEAD
        .isInstanceOf[FakeWriteV1Fallback])
=======
        .isInstanceOf[Table])
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    }

    // Ensure we create a V1 sink with the config. Note the config is a comma separated
    // list, including other fake entries.
    val fullSinkName = "org.apache.spark.sql.streaming.sources.FakeWriteV1Fallback"
    withSQLConf(SQLConf.DISABLED_V2_STREAMING_WRITERS.key -> s"a,b,c,test,$fullSinkName,d,e") {
      testPositiveCaseWithQuery(
        "fake-read-microbatch-continuous", "fake-write-v1-fallback", Trigger.Once()) { v1Query =>
        assert(v1Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
          .isInstanceOf[FakeSink])
      }
    }
  }

  Seq(
    Tuple2(classOf[FakeReadMicroBatchOnly], Trigger.Once()),
    Tuple2(classOf[FakeReadContinuousOnly], Trigger.Continuous(1000))
  ).foreach { case (source, trigger) =>
    test(s"SPARK-25460: session options are respected in structured streaming sources - $source") {
      // `keyPrefix` and `shortName` are the same in this test case
<<<<<<< HEAD
      val readSource = source.newInstance().shortName()
=======
      val readSource = source.getConstructor().newInstance().shortName()
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      val writeSource = "fake-write-microbatch-continuous"

      val readOptionName = "optionA"
      withSQLConf(s"spark.datasource.$readSource.$readOptionName" -> "true") {
        testPositiveCaseWithQuery(readSource, writeSource, trigger) { _ =>
          eventually(timeout(streamingTimeout)) {
            // Write options should not be set.
<<<<<<< HEAD
            assert(LastWriteOptions.options.getBoolean(readOptionName, false) == false)
            assert(LastReadOptions.options.getBoolean(readOptionName, false) == true)
=======
            assert(!LastWriteOptions.options.containsKey(readOptionName))
            assert(LastReadOptions.options.getBoolean(readOptionName, false))
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
          }
        }
      }

      val writeOptionName = "optionB"
      withSQLConf(s"spark.datasource.$writeSource.$writeOptionName" -> "true") {
        testPositiveCaseWithQuery(readSource, writeSource, trigger) { _ =>
          eventually(timeout(streamingTimeout)) {
            // Read options should not be set.
<<<<<<< HEAD
            assert(LastReadOptions.options.getBoolean(writeOptionName, false) == false)
            assert(LastWriteOptions.options.getBoolean(writeOptionName, false) == true)
=======
            assert(!LastReadOptions.options.containsKey(writeOptionName))
            assert(LastWriteOptions.options.getBoolean(writeOptionName, false))
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
          }
        }
      }
    }
  }

  // Get a list of (read, write, trigger) tuples for test cases.
  val cases = readFormats.flatMap { read =>
    writeFormats.flatMap { write =>
      triggers.map(t => (write, t))
    }.map {
      case (write, t) => (read, write, t)
    }
  }

  for ((read, write, trigger) <- cases) {
    testQuietly(s"stream with read format $read, write format $write, trigger $trigger") {
<<<<<<< HEAD
      val readSource = DataSource.lookupDataSource(read, spark.sqlContext.conf).newInstance()
      val writeSource = DataSource.lookupDataSource(write, spark.sqlContext.conf).newInstance()
      (readSource, writeSource, trigger) match {
        // Valid microbatch queries.
        case (_: MicroBatchReadSupport, _: StreamWriteSupport, t)
          if !t.isInstanceOf[ContinuousTrigger] =>
          testPositiveCase(read, write, trigger)

        // Valid continuous queries.
        case (_: ContinuousReadSupport, _: StreamWriteSupport, _: ContinuousTrigger) =>
          testPositiveCase(read, write, trigger)
=======
      val sourceTable = DataSource.lookupDataSource(read, spark.sqlContext.conf).getConstructor()
        .newInstance().asInstanceOf[TableProvider].getTable(CaseInsensitiveStringMap.empty())
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

      val sinkTable = DataSource.lookupDataSource(write, spark.sqlContext.conf).getConstructor()
        .newInstance().asInstanceOf[TableProvider].getTable(CaseInsensitiveStringMap.empty())

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      trigger match {
        // Invalid - can't read at all
<<<<<<< HEAD
        case (r, _, _)
            if !r.isInstanceOf[MicroBatchReadSupport]
              && !r.isInstanceOf[ContinuousReadSupport] =>
=======
        case _ if !sourceTable.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
          testNegativeCase(read, write, trigger,
            s"Data source $read does not support streamed reading")

        // Invalid - can't write
<<<<<<< HEAD
        case (_, w, _) if !w.isInstanceOf[StreamWriteSupport] =>
          testNegativeCase(read, write, trigger,
            s"Data source $write does not support streamed writing")

        // Invalid - trigger is continuous but reader is not
        case (r, _: StreamWriteSupport, _: ContinuousTrigger)
            if !r.isInstanceOf[ContinuousReadSupport] =>
          testNegativeCase(read, write, trigger,
            s"Data source $read does not support continuous processing")

        // Invalid - trigger is microbatch but reader is not
        case (r, _, t)
           if !r.isInstanceOf[MicroBatchReadSupport] && !t.isInstanceOf[ContinuousTrigger] =>
          testPostCreationNegativeCase(read, write, trigger,
            s"Data source $read does not support microbatch processing")
=======
        case _ if !sinkTable.supports(STREAMING_WRITE) =>
          testNegativeCase(read, write, trigger,
            s"Data source $write does not support streamed writing")

        case _: ContinuousTrigger =>
          if (sourceTable.supports(CONTINUOUS_READ)) {
            // Valid microbatch queries.
            testPositiveCase(read, write, trigger)
          } else {
            // Invalid - trigger is continuous but reader is not
            testNegativeCase(
              read, write, trigger, s"Data source $read does not support continuous processing")
          }

        case microBatchTrigger =>
          if (sourceTable.supports(MICRO_BATCH_READ)) {
            // Valid continuous queries.
            testPositiveCase(read, write, trigger)
          } else {
            // Invalid - trigger is microbatch but reader is not
            testPostCreationNegativeCase(read, write, trigger,
              s"Data source $read does not support microbatch processing")
          }
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      }
    }
  }
}

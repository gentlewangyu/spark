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

package org.apache.spark.sql.execution.streaming

<<<<<<< HEAD
import java.{util => ju}
import java.util.Optional
=======
import java.util
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
<<<<<<< HEAD
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal
=======
import scala.collection.mutable.ListBuffer
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset => OffsetV2}
import org.apache.spark.sql.streaming.OutputMode
=======
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousStream, MicroBatchStream, Offset => OffsetV2, SparkDataStream}
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object MemoryStream {
  protected val currentBlockId = new AtomicInteger(0)
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A : Encoder](implicit sqlContext: SQLContext): MemoryStream[A] =
    new MemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext)
}

/**
 * A base class for memory stream implementations. Supports adding data and resetting.
 */
abstract class MemoryStreamBase[A : Encoder](sqlContext: SQLContext) extends SparkDataStream {
  val encoder = encoderFor[A]
  protected val attributes = encoder.schema.toAttributes

  def toDS(): Dataset[A] = {
    Dataset[A](sqlContext.sparkSession, logicalPlan)
  }

  def toDF(): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, logicalPlan)
  }

  def addData(data: A*): OffsetV2 = {
    addData(data.toTraversable)
  }

<<<<<<< HEAD
  def readSchema(): StructType = encoder.schema
=======
  def addData(data: TraversableOnce[A]): OffsetV2

  def fullSchema(): StructType = encoder.schema
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

  protected val logicalPlan: LogicalPlan = {
    StreamingRelationV2(
      MemoryStreamTableProvider,
      "memory",
      new MemoryStreamTable(this),
      CaseInsensitiveStringMap.empty(),
      attributes,
      None)(sqlContext.sparkSession)
  }

  override def initialOffset(): OffsetV2 = {
    throw new IllegalStateException("should not be called.")
  }

  override def deserializeOffset(json: String): OffsetV2 = {
    throw new IllegalStateException("should not be called.")
  }

  override def commit(end: OffsetV2): Unit = {
    throw new IllegalStateException("should not be called.")
  }
}

// This class is used to indicate the memory stream data source. We don't actually use it, as
// memory stream is for test only and we never look it up by name.
object MemoryStreamTableProvider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new IllegalStateException("MemoryStreamTableProvider should not be used.")
  }
}

class MemoryStreamTable(val stream: MemoryStreamBase[_]) extends Table with SupportsRead {

  override def name(): String = "MemoryStreamDataSource"

  override def schema(): StructType = stream.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new MemoryStreamScanBuilder(stream)
  }
}

class MemoryStreamScanBuilder(stream: MemoryStreamBase[_]) extends ScanBuilder with Scan {

  override def build(): Scan = this

  override def description(): String = "MemoryStreamDataSource"

  override def readSchema(): StructType = stream.fullSchema()

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    stream.asInstanceOf[MicroBatchStream]
  }

  override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
    stream.asInstanceOf[ContinuousStream]
  }
}

/**
 * A [[Source]] that produces value stored in memory as they are added by the user.  This [[Source]]
 * is intended for use in unit tests as it can only replay data when the object is still
 * available.
 */
case class MemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
<<<<<<< HEAD
    extends MemoryStreamBase[A](sqlContext) with MicroBatchReader with Logging {
=======
    extends MemoryStreamBase[A](sqlContext) with MicroBatchStream with Logging {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

  protected val output = logicalPlan.output

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[Array[UnsafeRow]]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var startOffset = new LongOffset(-1)

  @GuardedBy("this")
  private var endOffset = new LongOffset(-1)

  /**
   * Last offset that was discarded, or -1 if no commits have occurred. Note that the value
   * -1 is used in calculations below and isn't just an arbitrary constant.
   */
  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  def addData(data: TraversableOnce[A]): Offset = {
    val objects = data.toSeq
    val rows = objects.iterator.map(d => encoder.toRow(d).copy().asInstanceOf[UnsafeRow]).toArray
    logDebug(s"Adding: $objects")
    this.synchronized {
      currentOffset = currentOffset + 1
      batches += rows
      currentOffset
    }
  }

  override def toString: String = {
    s"MemoryStream[${truncatedString(output, ",", SQLConf.get.maxToStringFields)}]"
  }

  override def setOffsetRange(start: Optional[OffsetV2], end: Optional[OffsetV2]): Unit = {
    synchronized {
      startOffset = start.orElse(LongOffset(-1)).asInstanceOf[LongOffset]
      endOffset = end.orElse(currentOffset).asInstanceOf[LongOffset]
    }
  }

  override def deserializeOffset(json: String): OffsetV2 = LongOffset(json.toLong)

  override def getStartOffset: OffsetV2 = synchronized {
    if (startOffset.offset == -1) null else startOffset
  }

<<<<<<< HEAD
  override def getEndOffset: OffsetV2 = synchronized {
    if (endOffset.offset == -1) null else endOffset
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
=======
  override def planInputPartitions(start: OffsetV2, end: OffsetV2): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[LongOffset]
    val endOffset = end.asInstanceOf[LongOffset]
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    synchronized {
      // Compute the internal batch numbers to fetch: [startOrdinal, endOrdinal)
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      // Internal buffer only holds the batches after lastCommittedOffset.
      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        batches.slice(sliceStart, sliceEnd)
      }

      logDebug(generateDebugString(newBlocks.flatten, startOrdinal, endOrdinal))

      newBlocks.map { block =>
        new MemoryStreamInputPartition(block): InputPartition[InternalRow]
      }.asJava
    }
  }

<<<<<<< HEAD
=======
  override def createReaderFactory(): PartitionReaderFactory = {
    MemoryStreamReaderFactory
  }

>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
  private def generateDebugString(
      rows: Seq[UnsafeRow],
      startOrdinal: Int,
      endOrdinal: Int): String = {
    val fromRow = encoder.resolveAndBind().fromRow _
    s"MemoryBatch [$startOrdinal, $endOrdinal]: " +
        s"${rows.map(row => fromRow(row)).mkString(", ")}"
  }

  override def commit(end: OffsetV2): Unit = synchronized {
    val newOffset = end.asInstanceOf[LongOffset]
    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop() {}

  def reset(): Unit = synchronized {
    batches.clear()
    startOffset = LongOffset(-1)
    endOffset = LongOffset(-1)
    currentOffset = new LongOffset(-1)
    lastOffsetCommitted = new LongOffset(-1)
  }
}


class MemoryStreamInputPartition(records: Array[UnsafeRow])
  extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new InputPartitionReader[InternalRow] {
      private var currentIndex = -1

      override def next(): Boolean = {
        // Return true as long as the new index is in the array.
        currentIndex += 1
        currentIndex < records.length
      }

      override def get(): UnsafeRow = records(currentIndex)

      override def close(): Unit = {}
    }
  }
}

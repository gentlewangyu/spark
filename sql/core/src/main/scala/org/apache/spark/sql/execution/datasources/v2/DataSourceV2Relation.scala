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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
<<<<<<< HEAD
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsReportStatistics}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType
=======
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{Statistics => V2Statistics, _}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

/**
 * A logical plan representing a data source v2 table.
 *
<<<<<<< HEAD
 * @param source An instance of a [[DataSourceV2]] implementation.
 * @param options The options for this scan. Used to create fresh [[DataSourceReader]].
 * @param userSpecifiedSchema The user-specified schema for this scan. Used to create fresh
 *                            [[DataSourceReader]].
 */
case class DataSourceV2Relation(
    source: DataSourceV2,
=======
 * @param table   The table that this relation represents.
 * @param options The options for this table operation. It's used to create fresh [[ScanBuilder]]
 *                and [[WriteBuilder]].
 */
case class DataSourceV2Relation(
    table: Table,
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    output: Seq[AttributeReference],
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  override def name: String = table.name()

  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

<<<<<<< HEAD
  def newReader(): DataSourceReader = source.createReader(options, userSpecifiedSchema)

  def newWriter(): DataSourceWriter = source.createWriter(options, schema)

  override def computeStats(): Statistics = newReader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.estimateStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
=======
  def newScanBuilder(): ScanBuilder = {
    table.asReadable.newScanBuilder(options)
  }

  override def computeStats(): Statistics = {
    val scan = newScanBuilder().build()
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }

  override def refresh(): Unit = table match {
    case table: FileTable => table.fileIndex.refresh()
    case _ => // Do nothing.
  }
}

/**
 * A specialization of [[DataSourceV2Relation]] with the streaming bit set to true.
 *
 * Note that, this plan has a mutable reader, so Spark won't apply operator push-down for this plan,
 * to avoid making the plan mutable. We should consolidate this plan and [[DataSourceV2Relation]]
 * after we figure out how to apply operator push-down for streaming data sources.
 */
case class StreamingDataSourceV2Relation(
<<<<<<< HEAD
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    reader: DataSourceReader)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {
=======
    output: Seq[Attribute],
    scan: Scan,
    stream: SparkDataStream,
    startOffset: Option[Offset] = None,
    endOffset: Option[Offset] = None)
  extends LeafNode with MultiInstanceRelation {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

  override def isStreaming: Boolean = true

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

<<<<<<< HEAD
  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.estimateStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
=======
  override def computeStats(): Statistics = scan match {
    case r: SupportsReportStatistics =>
      val statistics = r.estimateStatistics()
      DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
<<<<<<< HEAD
  private implicit class SourceHelpers(source: DataSourceV2) {
    def asReadSupport: ReadSupport = {
      source match {
        case support: ReadSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
      }
    }

    def asWriteSupport: WriteSupport = {
      source match {
        case support: WriteSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not writable: $name")
      }
    }

    def name: String = {
      source match {
        case registered: DataSourceRegister =>
          registered.shortName()
        case _ =>
          source.getClass.getSimpleName
      }
    }

    def createReader(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      val v2Options = new DataSourceOptions(options.asJava)
      userSpecifiedSchema match {
        case Some(s) =>
          asReadSupport.createReader(s, v2Options)
        case _ =>
          asReadSupport.createReader(v2Options)
      }
    }

    def createWriter(
        options: Map[String, String],
        schema: StructType): DataSourceWriter = {
      val v2Options = new DataSourceOptions(options.asJava)
      asWriteSupport.createWriter(UUID.randomUUID.toString, schema, SaveMode.Append, v2Options).get
    }
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      tableIdent: Option[TableIdentifier] = None,
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val reader = source.createReader(options, userSpecifiedSchema)
    val ident = tableIdent.orElse(tableFromOptions(options))
    DataSourceV2Relation(
      source, reader.readSchema().toAttributes, options, ident, userSpecifiedSchema)
  }

  private def tableFromOptions(options: Map[String, String]): Option[TableIdentifier] = {
    options
        .get(DataSourceOptions.TABLE_KEY)
        .map(TableIdentifier(_, options.get(DataSourceOptions.DATABASE_KEY)))
=======
  def create(table: Table, options: CaseInsensitiveStringMap): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(table, output, options)
  }

  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultRowCount: Option[BigInt],
      defaultSizeInBytes: Long): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }
    Statistics(
      sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
      rowCount = numRows)
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
  }
}

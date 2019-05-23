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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Repartition}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
<<<<<<< HEAD
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
=======
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

object DataSourceV2Strategy extends Strategy with PredicateHelper {

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  private def pushFilters(
<<<<<<< HEAD
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
=======
      scanBuilder: ScanBuilder,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    scanBuilder match {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      case r: SupportsPushDownFilters =>
        // A map from translated data source leaf node filters to original catalyst filter
        // expressions. For a `And`/`Or` predicate, it is possible that the predicate is partially
        // pushed down. This map can be used to construct a catalyst filter expression from the
        // input filter, or a superset(partial push down filter) of the input filter.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated =
            DataSourceStrategy.translateFilterWithMapping(filterExpr, Some(translatedFilterToExpr))
          if (translated.isEmpty) {
            untranslatableExprs += filterExpr
          } else {
            translatedFilters += translated.get
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilters.toArray).map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map { filter =>
          DataSourceStrategy.rebuildExpressionFromFilter(filter, translatedFilterToExpr)
        }
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return new output attributes after column pruning.
   */
  // TODO: nested column pruning.
  private def pruneColumns(
<<<<<<< HEAD
      reader: DataSourceReader,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): Seq[AttributeReference] = {
    reader match {
=======
      scanBuilder: ScanBuilder,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): (Scan, Seq[AttributeReference]) = {
    scanBuilder match {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)
<<<<<<< HEAD
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          r.readSchema().toAttributes.map {
=======
          val scan = r.build()
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          scan -> scan.readSchema().toAttributes.map {
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          relation.output
        }

<<<<<<< HEAD
      case _ => relation.output
=======
      case _ => scanBuilder.build() -> relation.output
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    }
  }

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
<<<<<<< HEAD
      val reader = relation.newReader()
      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFilters) = pushFilters(reader, filters)
      val output = pruneColumns(reader, relation, project ++ postScanFilters)
=======
      val scanBuilder = relation.newScanBuilder()

      val (withSubquery, withoutSubquery) = filters.partition(SubqueryExpression.hasSubquery)
      val normalizedFilters = DataSourceStrategy.normalizeFilters(
        withoutSubquery, relation.output)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) =
        pushFilters(scanBuilder, normalizedFilters)
      val postScanFilters = postScanFiltersWithoutSubquery ++ withSubquery
      val (scan, output) = pruneColumns(scanBuilder, relation, project ++ postScanFilters)
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

<<<<<<< HEAD
      val scan = DataSourceV2ScanExec(
        output, relation.source, relation.options, pushedFilters, reader)
=======
      val plan = BatchScanExec(output, scan)
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, plan)).getOrElse(plan)

      // always add the projection, which will produce unsafe rows required by some operators
      ProjectExec(project, withFilter) :: Nil

<<<<<<< HEAD
    case r: StreamingDataSourceV2Relation =>
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        DataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader)) :: Nil
=======
    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isDefined =>
      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        MicroBatchScanExec(
          r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)) :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isEmpty =>
      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        ContinuousScanExec(
          r.output, r.scan, continuousStream, r.startOffset.get)) :: Nil
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      CreateTableAsSelectExec(
        catalog, ident, parts, planLater(query), props, writeOptions, ifNotExists) :: Nil

    case AppendData(r: DataSourceV2Relation, query, _) =>
<<<<<<< HEAD
      WriteToDataSourceV2Exec(r.newWriter(), planLater(query)) :: Nil
=======
      AppendDataExec(r.table.asWritable, r.options, planLater(query)) :: Nil

    case OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).map {
        filter => DataSourceStrategy.translateFilter(deleteExpr).getOrElse(
          throw new AnalysisException(s"Cannot translate expression to source filter: $filter"))
      }.toArray

      OverwriteByExpressionExec(
        r.table.asWritable, filters, r.options, planLater(query)) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, _) =>
      OverwritePartitionsDynamicExec(r.table.asWritable, r.options, planLater(query)) :: Nil
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
<<<<<<< HEAD
      val isContinuous = child.collectFirst {
        case StreamingDataSourceV2Relation(_, _, _, r: ContinuousReader) => r
=======
      val isContinuous = child.find {
        case r: StreamingDataSourceV2Relation => r.stream.isInstanceOf[ContinuousStream]
        case _ => false
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case _ => Nil
  }
}

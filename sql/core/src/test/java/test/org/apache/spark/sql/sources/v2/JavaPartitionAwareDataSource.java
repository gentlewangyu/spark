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

package test.org.apache.spark.sql.sources.v2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalog.v2.expressions.Expressions;
import org.apache.spark.sql.catalog.v2.expressions.Transform;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
<<<<<<< HEAD
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
=======
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableProvider;
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.sources.v2.reader.partitioning.ClusteredDistribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Distribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;
<<<<<<< HEAD
import org.apache.spark.sql.types.StructType;

public class JavaPartitionAwareDataSource implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceReader, SupportsReportPartitioning {
    private final StructType schema = new StructType().add("a", "int").add("b", "int");

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      return java.util.Arrays.asList(
        new SpecificInputPartition(new int[]{1, 1, 3}, new int[]{4, 4, 6}),
        new SpecificInputPartition(new int[]{2, 4, 4}, new int[]{6, 2, 2}));
=======
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class JavaPartitionAwareDataSource implements TableProvider {

  class MyScanBuilder extends JavaSimpleScanBuilder implements SupportsReportPartitioning {

    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new SpecificInputPartition(new int[]{1, 1, 3}, new int[]{4, 4, 6});
      partitions[1] = new SpecificInputPartition(new int[]{2, 4, 4}, new int[]{6, 2, 2});
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new SpecificReaderFactory();
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
    }

    @Override
    public Partitioning outputPartitioning() {
      return new MyPartitioning();
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public Transform[] partitioning() {
        return new Transform[] { Expressions.identity("i") };
      }

      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder();
      }
    };
  }

  static class MyPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
      return 2;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
      if (distribution instanceof ClusteredDistribution) {
        String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
        return Arrays.asList(clusteredCols).contains("a");
      }

      return false;
    }
  }

  static class SpecificInputPartition implements InputPartition<InternalRow>,
    InputPartitionReader<InternalRow> {

    private int[] i;
    private int[] j;
    private int current = -1;

    SpecificInputPartition(int[] i, int[] j) {
      assert i.length == j.length;
      this.i = i;
      this.j = j;
    }

    @Override
    public boolean next() throws IOException {
      current += 1;
      return current < i.length;
    }

    @Override
    public InternalRow get() {
      return new GenericInternalRow(new Object[] {i[current], j[current]});
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
      return this;
    }
  }
<<<<<<< HEAD

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return new Reader();
  }
=======
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da
}

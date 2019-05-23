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

package org.apache.spark.sql.catalyst.csv

import org.apache.spark.SparkFunSuite

class CSVExprUtilsSuite extends SparkFunSuite {
  test("Can parse escaped characters") {
<<<<<<< HEAD:sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/csv/CSVUtilsSuite.scala
    assert(CSVUtils.toChar("""\t""") === '\t')
    assert(CSVUtils.toChar("""\r""") === '\r')
    assert(CSVUtils.toChar("""\b""") === '\b')
    assert(CSVUtils.toChar("""\f""") === '\f')
    assert(CSVUtils.toChar("""\"""") === '\"')
    assert(CSVUtils.toChar("""\'""") === '\'')
    assert(CSVUtils.toChar("""\u0000""") === '\u0000')
    assert(CSVUtils.toChar("""\\""") === '\\')
=======
    assert(CSVExprUtils.toChar("""\t""") === '\t')
    assert(CSVExprUtils.toChar("""\r""") === '\r')
    assert(CSVExprUtils.toChar("""\b""") === '\b')
    assert(CSVExprUtils.toChar("""\f""") === '\f')
    assert(CSVExprUtils.toChar("""\"""") === '\"')
    assert(CSVExprUtils.toChar("""\'""") === '\'')
    assert(CSVExprUtils.toChar("""\u0000""") === '\u0000')
    assert(CSVExprUtils.toChar("""\\""") === '\\')
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da:sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/csv/CSVExprUtilsSuite.scala
  }

  test("Does not accept delimiter larger than one character") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("ab")
    }
    assert(exception.getMessage.contains("cannot be more than one character"))
  }

  test("Throws exception for unsupported escaped characters") {
    val exception = intercept[IllegalArgumentException]{
      CSVExprUtils.toChar("""\1""")
    }
    assert(exception.getMessage.contains("Unsupported special character for delimiter"))
  }

  test("string with one backward slash is prohibited") {
    val exception = intercept[IllegalArgumentException]{
<<<<<<< HEAD:sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/csv/CSVUtilsSuite.scala
      CSVUtils.toChar("""\""")
=======
      CSVExprUtils.toChar("""\""")
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da:sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/csv/CSVExprUtilsSuite.scala
    }
    assert(exception.getMessage.contains("Single backslash is prohibited"))
  }

  test("output proper error message for empty string") {
    val exception = intercept[IllegalArgumentException]{
<<<<<<< HEAD:sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/csv/CSVUtilsSuite.scala
      CSVUtils.toChar("")
=======
      CSVExprUtils.toChar("")
>>>>>>> 5fae8f7b1d26fca3cbf663e46ca0da6d76c690da:sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/csv/CSVExprUtilsSuite.scala
    }
    assert(exception.getMessage.contains("Delimiter cannot be empty string"))
  }
}

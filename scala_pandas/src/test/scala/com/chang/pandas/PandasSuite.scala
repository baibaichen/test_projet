package com.chang.pandas


import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

/**
  * Created by ChangDev on 4/11/2017.
  */
class PandasSuite extends QueryTest with SharedSQLContext{

  test("dfZipWithIndex") {
    val zipped = Pandas.dfZipWithIndex(testData)
    zipped.collect().foreach { x =>
      assert(x.getAs[Long](0) + 1 == x(1))
    }
  }
}

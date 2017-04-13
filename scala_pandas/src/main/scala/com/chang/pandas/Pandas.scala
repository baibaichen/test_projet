package com.chang.pandas

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}


object Pandas {

  def dfZipWithIndex(df: DataFrame, colName: String = "row_num") : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(r => Row.fromSeq(Seq(r._2) ++ r._1.toSeq) ),
      StructType(Array(StructField(colName, LongType, false)) ++ df.schema.fields )
    )
  }

  def concat(left: DataFrame, right: DataFrame): DataFrame = {

    null
  }
}

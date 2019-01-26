package com.zhaochen.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.api.java._

class DepartmentDao(sqlc: SQLContext) {
  def sumBudget(): Long =
    sqlc.sql("select sum(budget) from department").rdd
    .map(_.getLong(0)).first()

  def numberOfEmployee(): RDD[(Int, Long)] =
    sqlc.sql("select department,count(*) from department group by department").rdd
    .map(row => (row.getInt(0), row.getLong(1)))
  }



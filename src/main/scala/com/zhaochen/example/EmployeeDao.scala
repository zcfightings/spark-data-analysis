package com.zhaochen.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}

class EmployeeDao(sqlc: SQLContext) {
  import EmployeeDao._
  def lastName(): RDD[String] = {
    sqlc.sql("select lastName from deployee").rdd
      .map(row => row.getString(0))
  }

  def distinctLastName(): RDD[String] = {
    sqlc.sql("select distinct lastName from deployee").rdd
      .map(row => row.getString(0))
  }

  def byLastname(lastName: String*): RDD[Employee] = {
    sqlc.sql(s"select * from employee where lastName in (${lastName.mkString("'", "', '","'")})").rdd
      .map(toEmployee)
  }


}


object EmployeeDao {
  private def toEmployee(row: Row): Employee = {
    Employee(row.getString(0), row.getString(1), row.getString(2), row.getInt(3))
  }
}
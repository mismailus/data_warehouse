package com.softech.blog.datamart.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._



//Use Spark To Generate A DataFrame Representing 100 Years

class DateDimension (startDate: String = "1970-01-01",
                     dataFormatMask: String = "yyyy-MM-dddd",
                     numYears: Int = 100,
                     spark: SparkSession = SparkSession.builder().getOrCreate()) extends Serializable {

//declare variables
  private val dtFormat = DateTimeFormatter.ISO_LOCAL_DATE
  // convert the stat data into the local date.
  private val startTime : LocalDate = LocalDate.parse(startDate,DateTimeFormatter.ISO_LOCAL_DATE)
  private val endTime = startTime.plusYears(numYears)
  private val startTimeEpoch = startTime.toEpochDay


  def createDataFrame(): DataFrame= {
    val numOfDaysToGenerate = DAYS.between(startTime,endTime)

    val dateDim = spark.range(10)
      .withColumnRenamed("id", "date_seq")
      .selectExpr("*", s"CASE date_seq WHEN 0 THEN $startTimeEpoch ELSE $startTimeEpoch + (date_seq *86400) END as unix_time")
      .selectExpr("*", "to_date(from_unixtime(unix_time)) as date_value")
      .withColumn("date_key", from_unixtime(col("unix_time"), "yyyyMMdd").cast("Int"))
      .withColumn("year_key", year(from_unixtime(col("unix_time"))))
      .withColumn("year_key", year(from_unixtime(col("unix_time"))))
      .withColumn("holidays", lit(false))
      .withColumn("year", year(from_unixtime(col("unix_time"))))
      .withColumn("quarter_of_year", quarter(from_unixtime(col("unix_time"))))
      .withColumn("month_of_year", month(from_unixtime(col("unix_time"))))
      .withColumn("day_number_of_week", from_unixtime(col("unix_time"), "u").cast("Int"))
      .selectExpr("*", """CASE WHEN day_number_of_week > 5 THEN true ELSE false END as weekend""")
      .withColumn("day_of_week_short", from_unixtime(col("unix_time"), "EEE"))
      .withColumn("day_of_week_long", from_unixtime(col("unix_time"), "EEEEEEEEE"))
      .withColumn("month_short", from_unixtime(col("unix_time"), "MMM"))
      .withColumn("month_long", from_unixtime(col("unix_time"), "MMMMMMMM"))
      .withColumn("week_key",expr("date_format(date_value, 'YYYYww')"))
      .selectExpr("*", """CASE WHEN month_of_year < 10
        THEN cast(concat(year,'0', month_of_year) as Int)
        ELSE cast(concat(year, month_of_year) as Int)
        END as month_key""")
      .selectExpr("*", s"cast(concat(year, quarter_of_year) as Int) as quarter_key")
      .selectExpr("*", s"concat('Q', quarter_of_year) as quarter_short")

    dateDim

  }

  /*def main(args: Array[String]): Unit= {

    val dateDimeDefault = new DateDimension().createDataFrame()
    dateDimeDefault.groupBy("date_value").agg(count("date_value"))
      .where("count(date_value) > 1")

    dateDimeDefault.foreach(r =>println(r))
  }*/

}

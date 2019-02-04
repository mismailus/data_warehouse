name := "data_warehourse"

version := "0.1"

scalaVersion := "2.11.8"

organization := "com.softech.blog"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.7" % "test")



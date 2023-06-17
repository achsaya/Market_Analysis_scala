package com.market.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark
object Marketing_Analysis {

     @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

     def main(args: Array[String]): Unit = {
       val spark = SparkSession.builder()
         .master("local[3]")
         .appName("Marketing Analysis Demo")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
         .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
         .config("spark.sql.shuffle.paritions", 3)
         .config("stopGracefullyOnShutdown", true)
         .getOrCreate()


       val marketdf = spark.read.option("header", true)
         .format("csv")
         .option("path", "input")
         .option("maxFilesPerTrigger", 1)
         .load()

       val df2 = marketdf.select(split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(0).as("age"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(1).as("job"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(2).as("marital"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(3).as("education"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(4).as("default"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(5).as("balance"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(6).as("housing"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(7).as("loan"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(8).as("contact"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(9).as("day"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(10).as("month"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(11).as("duration"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(12).as("campaign"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(13).as("pdays"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(14).as("previous"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(15).as("poutcome"),
         split(col("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\""), ";").getItem(16).as("y"))

         .drop("age;\"job\";\"marital\";\"education\";\"default\";\"balance\";\"housing\";\"loan\";\"contact\";\"day\";\"month\";\"duration\";\"campaign\";\"pdays\";\"previous\";\"poutcome\";\"y\"")

       df2.printSchema()

       df2.show(false)

       df2.withColumn("age", col("age").cast("int"))
       df2.withColumn("balance", col("balance").cast("int"))
       df2.withColumn("day", col("day").cast("int"))
       df2.withColumn("duration", col("duration").cast("int"))
       df2.withColumn("campaign", col("campaign").cast("int"))
       df2.withColumn("pdays", col("pdays").cast("int"))
       df2.withColumn("previous", col("previous").cast("int"))

       df2.printSchema()
       df2.createTempView("bank")

       val success = spark.sql("select (subscribed/total)*100 as marketing_success_rate from (select count(*) as subscribed from bank where y = '\"yes\"') , (select count (*) as total from bank)").show()

       val failure = spark.sql("select (not_subscribed/total)*100 as marketing_failure_rate from (select count(*) as not_subscribed from bank where y = '\"no\"') , (select count (*) as total from bank)").show()

       df2.select(max("age")).show()
       df2.select(min("age")).show()
       df2.select(avg("age")).show()

       df2.select(avg("balance")).show()

       val median = spark.sql("select percentile_approx(balance, 0.5) from bank").show()

       val age = spark.sql("select age, count(*) as number from bank where y='\"yes\"' group by age order by number desc").show()
       val marital = spark.sql("select marital, count(*) as number from bank where y='\"yes\"' group by marital order by number desc").show()

       val age_marital = spark.sql("select age,marital, count(*) as number from bank where y='\"yes\"' group by age,marital order by number desc").show()


       val agedf = spark.udf.register("agedf", (age: Int) => {
         if (age < 20)
           "Teen"
         else if (age > 20 && age <= 32)
           "Young"
         else if (age > 33 && age <= 55)
           "Middle Aged"
         else
           "Old"
       })
       val newage = df2.withColumn("age", agedf(df2("age")))
       newage.createTempView("newbank")

       val age_target_yes = spark.sql("select age, count(*) as number from newbank where y='\"yes\"' group by age order by number desc ").show()
       val age_target_no = spark.sql("select age, count(*) as number from newbank where y='\"no\"' group by age order by number desc ").show()


       /*val market1 = marketrdd.map(x => x.split(";"))
      val bankf = market1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }*/
       val dataFrame = newage.withColumn("incremental_id", monotonically_increasing_id())
       val visual = dataFrame.select(col("incremental_id"), col("age"), col("y"))
       visual.show()

       visual.write
         .format("org.apache.spark.sql.cassandra")
         .option("keyspace", "visual")
         .option("table", "age")
         .mode("append")
         .save()


     }
   }


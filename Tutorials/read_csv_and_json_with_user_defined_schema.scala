package com.spark.app
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ArrayType


object readcsvandjson {
 def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkByExamplescom")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val csvschema = StructType(Array(
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("Subject", StringType, true),
        StructField("Other", StringType, false),
        StructField("Active", BooleanType, true)))
    
    val text_data = spark.sqlContext.read.format("csv")
    .schema(csvschema)
    .option("header", true)
    .load("C://YouTube//Python//Sample_Input.csv")
    
    println("sample data in input file")
    println("Number of rows in input file = ",text_data.count())
    text_data.take(3).foreach(println)
    println("****************************************************\n")
    text_data.printSchema()
    
    val json_schema = StructType(Array(
        StructField("Name",ArrayType(StructType(
            Array(StructField("FName",StringType, true),
                  StructField("MName",StringType, true),
                  StructField("LName",StringType, true))))),
                  StructField("Age",IntegerType, true)))
                  
     val json_data = spark.sqlContext.read.format("json")
    .schema(json_schema)
    .option("header", true)
    .load("C://YouTube//spark//csv with schema//Input_Data.json")
    
    json_data.take(1).foreach(println)
    json_data.printSchema()
    
  }
}
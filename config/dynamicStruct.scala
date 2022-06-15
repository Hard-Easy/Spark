package ScalaDemo.com.scala
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.sql.{SparkSession, Row}


object dynamicStruct {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkByExamplescom")
    .getOrCreate()
    
  val url = ClassLoader.getSystemResource("schema.json")
  println(url)
  val schemaSource = Source.fromFile(url.getFile).getLines.mkString
  val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    
  val structureData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )
  val df3 = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData),schemaFromJson)
  df3.printSchema()

  }
}
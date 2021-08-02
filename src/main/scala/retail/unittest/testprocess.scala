package retail.unittest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import retail.layers._

object testprocess 
{
  val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder()
       .config("hive.metastore.uris","thrift://localhost:9083")
      .appName("Retail-coreengine")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
     
      //curationprocess.curateprocess(spark)
    
  }
}
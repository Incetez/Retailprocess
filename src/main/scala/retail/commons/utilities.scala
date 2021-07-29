package retail.commons
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.driver.runretail.log


object utilities 
{
   def readfileandwriteintostaging(spark:SparkSession,filename:String,tablename:String)=
  {
    log.info("reading data from the file:" + filename)
    println("load data into hive data: " + tablename)
    val df = spark.read.format("csv")
    .option("header",true)
    .option("quote","\"")
    .option("inferSchema",true)
    .load(filename)
    df.show()
    
    val df1 = df.withColumn("load_dt", current_date())
    
    //write data into hive table
    df1.write.mode("overwrite").saveAsTable(tablename)
    log.info("written data into hive table:" + tablename)     
    
  }
}
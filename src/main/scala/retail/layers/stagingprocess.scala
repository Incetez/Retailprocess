package retail.layers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import retail.driver.runretail.logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.regexp_replace

object stagingprocess 
{
  val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
  def stageprocess(spark:SparkSession)=
  {
    
    //create database
      spark.sql("create database if not exists retail_stg")
      logger.warn("======staging process started at " + format.format(Calendar.getInstance().getTime()))
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Customers.csv","retail_stg.tblcustomer_stg")
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Product_Categories.csv","retail_stg.tblproductcategory_stg")
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Product_Subcategories.csv","retail_stg.tblproductsubcategory_stg")
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Sales_*.csv","retail_stg.tblsales_stg")
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Territories.csv","retail_stg.tblterritory_stg")
      readfileandwriteintostaging(spark,"hdfs://ip-172-31-30-223.ec2.internal:8020/tmp/retaildata/Retail_Products.csv","retail_stg.tblproduct_stg")
      logger.warn("======staging process completed at " + format.format(Calendar.getInstance().getTime()))
    
  }
   def readfileandwriteintostaging(spark:SparkSession,filename:String,tablename:String)=
  {
    logger.warn("reading data from the file:" + filename)
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
    logger.warn("written data into hive table:" + tablename)     
    
  }
}
package retail.driver
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import retail.commons.utilities._

object runretail {
  
  val log = Logger.getLogger(this.getClass)
  def main(args:Array[String])=
  {
    try
    {
      val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
      log.info("======process started at " + format.format(Calendar.getInstance().getTime()))
      
      val spark = SparkSession.builder()
       .config("hive.metastore.uris","thrift://localhost:9083")
      .appName("Retail-coreengine")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
      
      spark.sparkContext.setLogLevel("ERROR")
      
      //===================staging load==========================
      
      //create database
      spark.sql("create database if not exists retail_stg")
      log.info("======staging process started at " + format.format(Calendar.getInstance().getTime()))
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Customers.csv","retail_stg.tblcustomer_stg")
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Product_Categories.csv","retail_stg.tblproductcategory_stg")
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Product_Subcategories.csv","retail_stg.tblproductsubcategory_stg")
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Sales_*.csv","retail_stg.tblsales_stg")
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Territories.csv","retail_stg.tblterritory_stg")
      readfileandwriteintostaging(spark,"file:/apps/data/RetailData/Retail_Products.csv","retail_stg.tblproduct_stg")
      log.info("======staging process completed at " + format.format(Calendar.getInstance().getTime()))
    
      //==================curation process======================
      
      
      
      //==================consumer/publish load ===================
    
    }
     catch 
        {
          // Catch block contain cases. 
          case ex: Exception => 
          {
              println("Error Occured:" + ex.getMessage)
              log.info("Error Occured:" + ex.getMessage)
          }
        }
  }
  
 
  
}
package retail.driver
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator
import retail.layers._
import java.util.Properties
import java.io.FileInputStream

object runretail {
  
  val logger = Logger.getLogger(this.getClass.getName)
  
 
  
  def main(args:Array[String])=
  {
    try
    {
      PropertyConfigurator.configure("log4j.properties")
      val confdata = new FileInputStream("app.properties")
      val prop = new Properties()
      prop.load(confdata)
      val format = new SimpleDateFormat("yyyy-MM-dd h:m:s")
      val starttime = Calendar.getInstance().getTimeInMillis()
      logger.warn("======process started at " + format.format(starttime))
      
      val spark = SparkSession.builder()
      //.config("hive.metastore.uris","thrift://localhost:9083")
      .appName("Retail-coreengine")
      .config("spark.sql.debug.maxToStringFields", 1000)
      
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()
      
      spark.sparkContext.setLogLevel("WARN")
      
      //===================staging load==========================
      stagingprocess.stageprocess(spark,prop)
      
    
      //==================curation process======================
      curationprocess.curateprocess(spark,prop)
      
      //==================aggregation load =====================
      aggregateprocess.aggrprocess(spark,prop)
      val endtime = Calendar.getInstance().getTimeInMillis()
      
      val processtime = (endtime - starttime) / (100 * 60) 
      logger.warn("Time taken to complete the process: " + processtime)
      
    
    }
     catch 
        {
          // Catch block contain cases. 
          case ex: Exception => 
          {
              println("Error Occured:" + ex.getMessage)
              logger.warn("Error Occured:" + ex.getMessage)
          }
        }
  }
  
 
  
}
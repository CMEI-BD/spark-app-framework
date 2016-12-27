package demos

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by canhuamei on 11/29/16.
  */
object StreamingKafkaDemo extends App{

  val sparkSession=SparkSession.builder().appName("MLETS").master("local[2]").getOrCreate()

  val LOG4J_PATH = "conf" + File.separator + "log4j.properties"

  val currentPath: String = System.getProperty("user.dir")

  var confPath: String = currentPath + File.separator + LOG4J_PATH


  PropertyConfigurator.configure(confPath)

  val streamReader=sparkSession.readStream

  val ds0=streamReader.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe","test").load()

  import sparkSession.implicits._


  val ds1=ds0.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").as[(String,String)]

  val ds2=ds1.select("value").flatMap( row => row.get(0).toString.split("\\s+"))

  val dsn=ds2.writeStream
      .format("console")
      .start()


  sparkSession.streams.awaitAnyTermination()



}

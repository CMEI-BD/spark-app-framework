package demos

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * Created by canhuamei on 11/29/16.
  */
object SqlDemo extends App{
  val sparkSession=SparkSession.builder().appName("MLETS").master("local[2]").getOrCreate()

  val LOG4J_PATH = "conf" + File.separator + "log4j.properties"

  val currentPath: String = System.getProperty("user.dir")

  var confPath: String = currentPath + File.separator + LOG4J_PATH

  val list=List(1,2,3,4,5)

  import sparkSession.implicits._

  sparkSession.sparkContext.setCheckpointDir("/Users/canhuamei/workspace/MY_GIT/spark-app-framework/src/test/cp2");

  val ds1=sparkSession.sqlContext.createDataset(list).filter(x => x==2)

  val rdd2=sparkSession.sparkContext.parallelize(list)

  println("rdd2:"+rdd2.toDebugString)
  //rdd2.persist()



  //  rdd2.collect()
  rdd2.persist()

  rdd2.checkpoint()

  rdd2.foreach(k=>print(k))


  val rdd3=rdd2.filter(x=> x==2)

  println("rdd3:"+rdd3.toDebugString)

  println("rdd3:"+rdd3.count())


  Thread.sleep(100000)

}

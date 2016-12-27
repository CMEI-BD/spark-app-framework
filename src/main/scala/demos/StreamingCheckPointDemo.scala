package demos


import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time, State,StateSpec}
import org.apache.spark.util.LongAccumulator

/**
  * Created by canhuamei on 12/26/16.
  */
object StreamingCheckPointDemo {

    def main(args: Array[String]): Unit = {
        val host = "localhost"
        val port: Int = 9999
        val checkPointDir = "/Users/canhuamei/workspace/MY_GIT/spark-app-framework/target/cp"
        val output = "target/output/res1"

        val ssc = StreamingContext.getOrCreate(checkPointDir, () => createContext(host, port, checkPointDir))
        ssc.start()
        ssc.awaitTermination()
    }



    def createContext(host:String,port:Int,checkPointDir:String) : StreamingContext = {
        println("Creating new context")

        val sparkSession = SparkSession.builder().appName("MLETS")
          .config("spark.streaming.receiver.writeAheadLog.enable", true)
          .master("local[2]").getOrCreate()


        //val wordBlack=sparkSession.sparkContext.broadcast(wordBlackList)

        //val longDropWordsCounter=sparkSession.sparkContext.longAccumulator("dropWordCounter")
        val batchDuration = Seconds(30)

        val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)

        ssc.checkpoint(checkPointDir)

        //transformations
        val lines = ssc.socketTextStream(host, port)
        val words = lines.flatMap(_.split(" "))
        val wordsMap = words.map((_, 1))
        val wordCounts = wordsMap.reduceByKey(_ + _)
        wordCounts.foreachRDD {
            (rdd: RDD[(String, Int)], time: Time) =>
                val wordBlack = WordBlacklist.getInstance(rdd.sparkContext)
                val longDropWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
                val counts = rdd.filter { case (word, count) =>
                    //println(wordBlack.value)
                    if (wordBlack.value.contains(word)) {
                        longDropWordsCounter.add(count)
                        false
                    } else {
                        true
                    }
                }.collect().mkString("[", ",", "]")
                println(counts + " time:" + time)
                println("dropWordsCounter:" + longDropWordsCounter.value)
        }

        //stateful usage
        val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
            val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
            val output = (word, sum)
            state.update(sum)
            output
        }

        val updateFuc=(newValues: Seq[Int], runningCount:Option[Int]) => {
            val newCount = newValues.sum+runningCount.getOrElse(0)
            Some(newCount)
        }

        //UpdateStateByKey 与 mapWithState 功能类似。并且依赖 checkpoint 功能的设置。
        val wordCounts3 = wordsMap.updateStateByKey[Int](updateFuc)
        wordCounts3.foreachRDD{rdd:RDD[(String,Int)] => rdd.foreach(println(_))}

        val wordCounts2 = wordsMap.mapWithState(StateSpec.function(mappingFunc))
        wordCounts2.foreachRDD{rdd:RDD[(String,Int)] => rdd.foreach(print(_))}

        ssc
    }


}

object WordBlacklist{
    //Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
    // They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.
    // Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.
    @volatile private var instance: Broadcast[Seq[String]] = null

    def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
        if (instance == null ) {
            synchronized {
                if (instance == null) {
                    val wordBlacklist = Seq("a","b","c")
                    instance = sc.broadcast(wordBlacklist)
                }
            }
        }
        instance
    }
}

object DroppedWordsCounter{
    @volatile  private var instance: LongAccumulator =null

    def getInstance(sc:SparkContext): LongAccumulator = {
        if (instance == null) {
            synchronized{
                if (instance == null){
                    instance = sc.longAccumulator("dropWordsCounter")
                }
            }
        }
        instance
    }
}

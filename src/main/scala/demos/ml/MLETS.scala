package demos.ml

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession


/**
  * Created by canhuamei on 11/29/16.
  */

object MLETS extends App{

  val sparkSession=SparkSession.builder().appName("MLETS").master("local[2]")getOrCreate()

  val sc=sparkSession.sparkContext

  val rawData="/Users/canhuamei/workspace/OPEN_GIT/spark-latest/data/mllib/sample_libsvm_data.txt"

  val parsedData=MLUtils.loadLibSVMFile(sc,rawData)

  val splits=parsedData.randomSplit(Array(0.7,0.3))

  val (trainingData,testData)=(splits(0),splits(1))

  val numClasses=2
  val categoricalFeaturesInfo = Map [Int,Int]()
  val impurity="gini"
  val maxDepth=5
  val maxBins=32

  val model=DecisionTree.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,impurity, maxDepth, maxBins)

  val labelAndPreds=testData.map{ point =>
    val prediction=model.predict(point.features)
    (point.label,prediction)
  }

  val testErr=labelAndPreds.filter( r => r._1 != r._2).count().toDouble / testData.count()

  println("testErr="+testErr)

  println (model.toDebugString)

  model.save(sc,"/Users/canhuamei/workspace/OPEN_GIT/spark-latest/data/mllib/target/model_dt")

  Thread.sleep(100000)

}
package entry;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;


/**
 * Created by cmei on 2016/6/27.
 * The factory provide simple way to construct all singleton based on sparkContext on the JVM.
 */
public class SparkContextFactory {


    public static final Boolean ENABLE_SPARK_STREAMING=false;

    public static final Long STREAMING_BATCH_INTERVAL=2L;

    private static SparkContext sparkContext;

    private static SQLContext sqlContext;

    private static StreamingContext streamingContext;

    private static SparkSession sparkSession;


    /*******************
     * The following functions only be called in driver program's start or stop
     ***************/
    public static void initializeContextFactory(SparkConf sparkConf) {
        sparkSession=buildSparkSession(sparkConf);
        if(sparkSession==null) {
            sparkContext = buildSparkContext(sparkConf);
            //sparkContext.addJar("E:\\MSTR_GIT\\Lightning\\Lightning-master\\lightning-lib\\target\\lib\\butterfly-1.0.jar");
            sqlContext = buildSQLContext(sparkContext);
        }else{
            sparkContext=sparkSession.sparkContext();
            sqlContext=sparkSession.sqlContext();
        }
        if (ENABLE_SPARK_STREAMING) {
            streamingContext = buildStreamingContext(sparkContext);
        }
    }

    public static void clearContextFactory() {
        if(sparkSession!=null){
            sparkSession.stop();
        }else if (sparkContext != null) {
            sparkContext.stop();
        }
        if (streamingContext != null) {
            streamingContext.stop(false, true);
        }
    }

    private static SparkContext buildSparkContext(SparkConf sparkConf) {
        return SparkContext.getOrCreate(sparkConf);
    }

    private static SQLContext buildSQLContext(SparkContext sparkContext) {
        return SQLContext.getOrCreate(sparkContext);
    }

    private static StreamingContext buildStreamingContext(SparkContext sparkContext) {
        //There are a lot of constructor to build streamingContext, if we need to use it in future, we need to enhance this
        // eg: provide checkpoint setting, here just provide the simplest way.
        return new StreamingContext(sparkContext, Seconds.apply(STREAMING_BATCH_INTERVAL));
    }


    /********************
     * The Getters to provide singleton instances
     *****************************************/
    public static SparkContext getSparkContext() {
        return sparkContext;
    }

    public static JavaSparkContext getJavaSparkContext() {
        return new JavaSparkContext(sparkContext);
    }

    public static SQLContext getSqlContext() {
        return sqlContext;
    }

    /**
     * @since spark2.0
     */
    private static SparkSession buildSparkSession(SparkConf sparkConf){
        return SparkSession.builder().config(sparkConf).getOrCreate();
    }

}

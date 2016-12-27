package entry;

import org.apache.spark.SparkConf;

/**
 * Created by cmei on 2016/6/27.
 */
public class SparkDriver {

    private static final SparkConfLoader sparkConfLoader=new SparkConfLoader();

    private static SparkConf currentSparkConf;

    private static SparkDriver sparkDriver=new SparkDriver();

    private SparkDriver(){}

    public static SparkDriver getInstance(){
        return sparkDriver;
    }
    public void start(){
        currentSparkConf=sparkConfLoader.load();
        SparkContextFactory.initializeContextFactory(currentSparkConf);
    }

    public void stop(){
        SparkContextFactory.clearContextFactory();
    }
}

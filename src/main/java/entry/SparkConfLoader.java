package entry;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by cmei on 2016/6/27.
 * The order of loading spark conf:
 * 0: loading provided by spark lib by default
 * 1.global SPARK_HOME from installed spark if has.--------------outside of Lightning
 * 2.Lightning default spark.conf (spark.conf.template)----------in classpath /conf
 * 3.User defined spark.conf for Lightning-----------------------outside of classpath
 */
public final class SparkConfLoader {

    private static final Logger logger= LoggerFactory.getLogger(SparkConfLoader.class);

    private static final String PATH_DEFAULT= "conf" + File.separator + "lightning-spark-default.conf";

    private static final String PATH_USERDEFINED="conf"+File.separator+"spark.conf";

    public SparkConf load(){
        //0:load default from spark lib jars.
        SparkConf sparkConf=new SparkConf();

        //1.load from global SPARK_HOME------can be nonexistent
        loadFromSparkHome(sparkConf);

        //2.load from lightning default---------should be exist,otherwise, check build jar.
        loadFromLightningDefault(sparkConf);

        //3.load from user-defined spark.conf
        loadFromUserDefined(sparkConf);

        uploadJars(sparkConf);

        logger.info("spark conf loaded:"+sparkConf.toDebugString());
        //System.out.println(sparkConf.toDebugString());
        return sparkConf;
    }

    /**
     * only reload from user-defined spark.conf now. TODO
     * @return
     */
    public  SparkConf reload(SparkConf sparkConf){

        loadFromUserDefined(sparkConf);

        return sparkConf;
    }

    private  void loadFromSparkHome(SparkConf sparkConf){
        try{
            String sparkHome=System.getenv("SPARK_HOME");
            if(sparkHome==null) {
                logger.debug("NO SPARK_HOME in environment!");
            }else {
                if(!(new File(sparkHome)).exists()){
                    logger.warn("SPARK_HOME="+sparkHome+" doesn't exist!");
                }
            }
            if (sparkHome != null) {
                loadFromFile(sparkHome, sparkConf);
            }
        }catch(IOException e){
            throw new DriverStartingException("Error in loading spark.conf from SPARK_HOME");
        }
    }

    private void loadFromLightningDefault(SparkConf sparkConf){
        try{
            //String jarPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
            //String confPath=jarPath+File.separator+PATH_DEFAULT;
            InputStream confStream=this.getClass().getClassLoader().getResourceAsStream(PATH_DEFAULT);
            String confPath=null;
//            if(url==null){
//                String currentPath=System.getProperty("user.dir");
//                confPath=currentPath+File.separator+PATH_USERDEFINED;
//            }else{
//                confPath=url.getPath();
//            }
//            logger.info("confPath1:"+confPath);

       //     if(!(new File(confPath)).exists()){
                //
                // throw new DriverStartingException("Lightning default spark.conf is missing in !"+confPath);

      //      }
            loadFromInputStream(confStream,sparkConf);
             logger.info("lightning-spark-default.conf is loaded!");
       // }catch(URISyntaxException e1){
        //   logger.error(e1.getMessage(),e1);
        //   throw new DriverStartingException("Error in loading default spark.conf!");
        }catch(IOException e2){
            logger.warn("Not find Lightning-spark-default.conf by classloader!");
            //throw new DriverStartingException("Error in loading default spark.conf!");
        }
    }

    private void loadFromUserDefined(SparkConf sparkConf){
        try{
            String currentPath=System.getProperty("user.dir");
            //temp for debug TODO new File(currentPath).getParent()
            String confPath=currentPath+File.separator+PATH_USERDEFINED;
            if(!(new File(confPath)).exists()){
                logger.info("Check user's defined spark.conf under "+confPath+" no such file, use default!");
              return;
            }
            loadFromFile(confPath,sparkConf);
            logger.info("user defined spark.conf is loaded!");
        }catch(IOException e){
            //e.printStackTrace();
            logger.error(e.getMessage(),e);
            throw new DriverStartingException("Error in loading user's spark.conf!");
        }
    }

    /**
     *
     * @param absolutePath
     * @param sparkConf
     * @throws IOException
     * @Since 1.6
     */
    private void loadFromFile(String absolutePath,SparkConf sparkConf) throws IOException{
        InputStream confStream=new FileInputStream(new File(absolutePath));
        loadFromInputStream(confStream,sparkConf);
    }

    private void loadFromInputStream(InputStream confStream,SparkConf sparkConf) throws IOException{
        Properties props = new Properties();
        props.load(confStream);
        for(String key:props.stringPropertyNames()) {
            sparkConf.set(key, props.getProperty(key));
        }
    }

    private void  uploadJars(SparkConf sparkConf){
        //temporarily solution for debug
        String currentPath=System.getProperty("user.dir");
        //for debug
       // String jarlib=(currentPath)+File.separator+"lightning-lib"+File.separator+"target"+File.separator+"lib";
        //for production
        String jarlib=currentPath+File.separator+"lib"+File.separator+"upload-libs";

        File f=new File(jarlib);
        if(f.exists()&&f.isDirectory()){
           File[] jars=f.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".jar");
                }
            });
           int jarNumber=jars.length;

           String[] jarPath=new String[jarNumber+1];
           for(int i=0;i<jars.length;i++){
               jarPath[i]=jars[i].getAbsolutePath();
           }
            //temp solution TODO
           jarPath[jarNumber]=currentPath+File.separator+"lib"+File.separator+"lightning-lib-1.0-SNAPSHOT.jar";

           sparkConf.setJars(jarPath);
        }else{
            logger.warn("Can't find the upload-libs");
        }

    }



}

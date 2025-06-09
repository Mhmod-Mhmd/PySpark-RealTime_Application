from pyspark.sql import SparkSession
import logging.config


logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")
loggers = logging.getLogger("create_spark")


def get_spark_object(env, appname):

    loggers.info("get_spark_object function started ")
    try:
        if env =="DEV":
            master = "local"
        else:
            master = "Yarn"
        
        loggers.info(f"master is: {master}")
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appname) \
            .enableHiveSupport() \
            .config('spark.driver.extraClassPath', 'mysql-connector-java-8.0.29.jar') \
            .getOrCreate()
    
    except Exception as e:
         loggers.error("an error occured in get_spark_object method: %s ", str(e))

         raise

    else:
        loggers.info("Spark object created...")
    
    return spark
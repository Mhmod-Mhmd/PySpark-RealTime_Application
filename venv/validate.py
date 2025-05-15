import os
from datetime import datetime
import logging.config

logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")
loggers = logging.getLogger("validate")

def get_current_date(spark):

    try:
        loggers.warning("Start get_current_date method... ")
        currentDate = spark.sql(""" select current_date()""")
        loggers.warning("Vaildating Spark object with current DateTime: %s" + str(currentDate.collect()))
    
    except Exception as e:
        loggers.error("an error occured in get_current_date method %s", str(e))

        raise
    else:
        loggers.info("validation done... ")

def df_count(df, df_name):
    try:
        loggers.warning(f"Validate DataFrame {df_name}")
        df_c = df.count()

    except Exception as e:
        logging.warning(f"an error ocurred while validating dataframe : {e}")
        raise
    return df_c
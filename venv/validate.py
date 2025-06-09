import os
from datetime import datetime
import logging.config
from pyspark.sql.functions import count, isnan, col, when

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


def print_schema(df, dfName):
    try:
        loggers.warning('print schema method executing....{} '.format(dfName))

        sch = df.schema.fields

        for i in sch:
            loggers.info(f"\t{i}")

    except Exception as e:
        loggers.error("An error ocuured at print_schema ::: ", str(e))

        raise
    else:
        loggers.info("print_schema done, go frwd....")



def check_nulls(df , df_name:str):
    try:
         # dataframe to count every null in each columns and print it in log file

        df_presc_null_counts = df.select([ count(
            when( (isnan(col(c)) | col(c).isNull()) , True)).alias(c)  
            for c in df.columns ]).collect()[0].asDict()
        
        loggers.warning(F" Null counts in every column in Dataframe {df_name}")
        for col_name, null_count in df_presc_null_counts.items():
            loggers.warning(f"{col_name} has null values = {null_count}")


    except Exception as e:
        loggers.warning(f"An error occured while checking for null : {e}")
        raise e
    else:
        loggers.warning("Null checking finshed sucessfully")

import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date, df_count
from ingest import load_data, load_input_data, display_df
from data_processing import data_cleaning
from datetime import datetime
import logging.config
import logging
import sys
import os
import pyspark.sql.functions as fn

logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")


def main():

    try:
        logging.info("I am in main ....")

        logging.info("Calling Saprk object")
        spark = get_spark_object(gev.envn, gev.appName)

        logging.info("Validating Spark object")
        # get_current_date(spark)
        
        if gev.RELOAD_DATA == "True":
            logging.info("Reloading input data as per configuration.")
            df_city, df_fact = load_input_data(spark)
        else:
            logging.info("RELOAD_DATA is False — skipping data load.")

        df_city_slct, df_presc_slct = data_cleaning(df_city, df_fact) 
        # display_df(df_city_slct)
        # display_df(df_presc_slct)

        for col_name in df_presc_slct.columns:
            null_count = df_presc_slct.filter(fn.col(col_name).isNull()).count()
            print(f"Column: {col_name} — Null values: {null_count}")


    except Exception as e:
        logging.error("An error occured when calling main(), please check the trace.: %s " , str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

from time import perf_counter
import sys
import os

import get_env_variables as gev
from create_spark import get_spark_object
from validate import *
from ingest import *
from data_processing import data_cleaning
from data_transformation import data_report1, data_report2
from extraction_file import extract_files
from persist_data import *
from datetime import datetime
import logging.config
import logging

import pyspark.sql.functions as fn
from pyspark.sql.functions import desc


logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")


def main():

    try:
        start_time = perf_counter()
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

       
        # validate::
        df_count(df_fact, 'df_fact')

        logging.info("implementing data_processing methods...")

        df_city_sel, df_presc_sel = data_cleaning(df_city, df_fact)

        display_df(df_city_sel, 'df_city')

        display_df(df_presc_sel, 'df_fact')

        logging.info("validating schema for the dataframes....")

        print_schema(df_city_sel, 'df_city_sel')

        print_schema(df_presc_sel, 'df_presc_Sel')

        display_df(df_presc_sel, 'df_fact')

        logging.info('checking for null values in dataframes...after processing ')

        # check_df = check_for_nulls(df_presc_sel, 'df_fact')

        # display_df(check_df, 'df_fact')

        logging.info('data_transformation executing....')

        df_report_1 = data_report1(df_city_sel, df_presc_sel)

        logging.info('displaying the df_report_1')
        # display_df(df_report_1, 'data_report')

        logging.info('displaying data_report2 method....')

        df_report_2 = data_report2(df_presc_sel)

        display_df(df_report_2, 'data_report2')

        logging.info("extracting files to Output...")
        city_path = gev.city_path

        extract_files(df_report_1, 'orc', city_path, 1, False, 'snappy')

        presc_path = gev.presc_path

        extract_files(df_report_2, 'parquet', presc_path, 2, False, 'snappy')

        logging.info("extracting files to output completed.....")

        logging.info('writing into hive table')

        persist_data_to_hive_cities(spark=spark,
                                     df=df_report_1, 
                                     dfname='df_city', 
                                     partitionBy='state_name', 
                                     mode='append')

        persist_data_to_hive_presc(spark=spark, 
                                   df=df_report_2, 
                                   dfname='df_presc', 
                                   partitionBy='presc_state', 
                                   mode='append')

        logging.info("successfully written into Hive")

        logging.info("Now write {} into MYSQL".format(df_report_1))

        persist_data_mysql(spark=spark,
                            df=df_report_1,
                            dfname='df_city',
                            url="jdbc:mysql://localhost:3306/datapipeline", 
                            dbtable='city_df',
                            mode='append',
                            user= gev.user, 
                            password= gev.password)

        logging.info("successfully data inserted into table...")

        end_time = perf_counter()
        print(f"the process time {end_time - start_time: 0.2f} seconds()")


    except Exception as e:
        logging.error("An error occured when calling main(), please check the trace.: %s " , str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
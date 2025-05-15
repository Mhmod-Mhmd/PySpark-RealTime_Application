import logging.config
from pyspark.sql.functions import *
import pyspark.sql.functions as fn

logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")
loggers = logging.getLogger("data_processing")


def data_cleaning(df1, df2):

    try:
        loggers.warning("Starting data_cleaning method ")

        loggers.warning('Select columns from city DataFrame')
        df_city_slct = df1.select(
            upper(col("city")).alias("city"),
            col("state_id"),
            upper(col('state_name')).alias('state_name'),
            col("county_name").alias('province_name'),
            col("population"),
            col('zips')
        ) 

        loggers.warning("Select columns from presc DataFrame")

        df_presc_slct = df2.select(
            col('npi').alias('presc_id'),
            col('nppes_provider_last_org_name').alias('presc_lastName'),
            col('nppes_provider_first_name').alias('presc_firstName'),
            col('nppes_provider_city').alias('presc_city'),
            col('nppes_provider_state').alias('presc_state'),
            col('specialty_description').alias('presc_spcialty'),
            col('drug_name'),
            col('total_claim_count').alias('claim_count'),
            col('total_day_supply'),
            col('total_drug_cost'),
            col('years_of_exp')
        )

        loggers.warning('Adding a new column to df_presc_slct')
        df_presc_slct = df_presc_slct.withColumn("country_name", lit("USA"))

        loggers.warning('Cleaning column years_of_exp and convert string type to int type')
        df_presc_slct = df_presc_slct.withColumn("years_of_exp", regexp_replace(col('years_of_exp'), '=', '').cast('Int'))

        loggers.warning("Concating presc_lastName,presc_firstName ")
        df_presc_slct = df_presc_slct.withColumn("presc_fullName", concat_ws(' ', 'presc_lastName', 'presc_firstName'))
        df_presc_slct = df_presc_slct.drop('presc_lastName', 'presc_firstName')

    except Exception as e:
        loggers.error('An error occured in data_cleaning() method %s',str(e))
        raise

    else:
        loggers.info(' data_cleaning method completed sucessfuly')
    
    return df_city_slct, df_presc_slct
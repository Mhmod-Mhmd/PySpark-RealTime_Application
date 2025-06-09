import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


logging.config.fileConfig('/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf')

loggers = logging.getLogger('data_transformation')


def data_report1(df_city, df_presc):
    try:
        loggers.warning("processing the data_report1 method..")

        loggers.warning(f"calculating total zip counts in {df_city}")

        df_city_split= df_city.withColumn("zipcounts", size(split("zips", " "))).orderBy(col("zipcounts").desc())

        loggers.warning("calculating distinct prescribers and total tx_count")

        df_presc_grp = df_presc.groupBy(col("p_state"), col("p_city")).agg(countDistinct("presc_id").alias('presc_counts'), sum("claim_count").alias('tx_counts'))

        loggers.warning("Don't report a city if no prescriber is assigned to it.....lets join df_city_split and"
                        "df_presc_grp")

        df_city_join = df_city_split.join(df_presc_grp, (df_city_split.state_id == df_presc_grp.p_state) & (df_city_split.city == df_presc_grp.p_city), 'inner')
        df_final = df_city_join.select("city", "state_name", "county_name", "population", "zipcounts", "presc_counts")

    except Exception as e:
        loggers.error("An error occured while dealing daat_report1....", str(e))
        raise

    else:
        loggers.warning("Data_report1 succesfully executed..., go frwd")

    return df_final


def data_report2(df_presc_sel):
	
    spec = window.partitionBy("state").orderBy("claim_count". desc())
	
    df_presc_final = df_presc_sel.select("presc_id", "p_fullName", "p_state", "claim_count", "year_of_exp", "total_day_supply", "country_name") \
                    .filter((df_presc_sel.year_of_exp >=20) & (df_presc_sel.year_of_exp <=50)) \
					.withColumn("dense_rank", dense_rank().over(spec)).filter(col("dense_rank") <=5)
    
    return df_presc_final
    
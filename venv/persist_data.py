import logging.config

logging.config.fileConfig('/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf')

loggers = logging.getLogger('persist_data')


def persist_data_to_hive_cities(spark, df, dfName, partitionBy, mode ):
    try:
        loggers.warning('persisting the data into Hive Table for {}'.format(dfname))

        loggers.warning('lets create a database....')

        spark.sql(""" create database if not exists cities """)

        spark.sql("""use cities """)

        loggers.warning("No writing {} into hive_table by {} ".format(df, partitionBy))

        df.write.mode(mode).partitionBy(partitionBy).saveAsTable(dfName)

    except Exception as exp:
        loggers.error("An error occurred while processing data_hive_persist::::", str(exp))

        raise
    else:
        loggers.warning('Data successfully persisted into hive tables...')


def persist_data_to_hive_presc(spark, df, dfName, partitionBy, mode ):
    try:
        loggers.warning('persisting the data into Hive Table for {}'.format(dfName))

        loggers.warning('lets create a database....')

        spark.sql("create database if not exists prescribers")
        spark.sql("use prescribers")

        df.write.mode(mode).partitionBy(partitionBy).saveAsTable(dfName)

    except Exception as exp:
        loggers.error("An error occurred while processing data_hive_persist::::", str(exp))

        raise
    else:
        loggers.warning('Data successfully persisted into hive tables...')

        
def persist_data_mysql(spark, df, dfname, url,  dbtable, mode, user, password):

    try:
        loggers.warning('executing the data_persist_mysql method...{}'.format(dfname))

        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .mode(mode) \
            .save()

    except Exception as e:
        loggers.error("An error occured @ persist_data_mysql method::::", str(e))

        raise

    else:
        loggers.warning('Persist_data_mysql method executed succesfully...into {}'.format(dbtable))

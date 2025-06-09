import logging.config
import get_env_variables as gev
import os

logging.config.fileConfig("/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf")
loggers = logging.getLogger("Ingest")


def load_data(spark,file_dir, file_format, file_header, file_inferschema, json_multiline=False):

    try:

        loggers.warning("Starting load_data() method")

        if file_format == "parquet":
            df = spark.read.format(file_format)\
                .load(file_dir)
        elif file_format == "csv":
            df = spark.read.format(file_format)\
                .option("header", file_header)\
                .option("inferschema", file_inferschema)\
                .load(file_dir)
        elif file_format == "json":
            df = spark.read.format(file_format)\
                .option("inferschema",file_inferschema)\
                .option("multiline",json_multiline)\
                .load(file_dir)

    except Exception as e:
        loggers.error("an error occured in load_data method :" , str(e))
        raise
    
    else:
        logging.info("Data Loaded sucessfully")

    return df



def load_input_data(spark):
    
    source_list = [gev.src_olap, gev.src_oltp]
    for source in source_list:
        for file in os.listdir(source):
            print("file is : " , file)

            file_name = os.path.splitext(file)[0]
            file_dir = os.path.join(source , file)

                # Check if file path exists
            if not os.path.exists(file_dir):
                logging.warning(f"File not found, skipping: {file_dir}")
                continue

            if file.endswith(".parquet"):
                format = "parquet"
                header = "False"
                inferschema = "False"
                multiline = False
                
            elif file.endswith(".csv"):
                format = "csv"
                header = gev.header
                inferschema = gev.inferSchema
                multiline = False
            elif file.endswith(".json"):
                format = "json"
                header = "False"
                inferschema = "False"
                multiline = True
            else:
                print(f"Skipping unsupported file: {file}")
                continue 

        logging.info(f"Reading file: {file_dir} with format '{format}' ")
        if source == gev.src_olap:
            logging.info(f"Processing file: {file_name} from folder: {source}")
            df_city = load_data(spark=spark, 
                                file_dir=file_dir, 
                                file_format=format, 
                                file_header=header, 
                                file_inferschema=inferschema, 
                                json_multiline=multiline)
            
            
        else:
            logging.info(f"Processing file: {file_name} from folder: {source}")
            df_fact = load_data(spark=spark, 
                                file_dir=file_dir, 
                                file_format=format, 
                                file_header=header, 
                                file_inferschema=inferschema, 
                                json_multiline=multiline)
    return df_city, df_fact


def display_df(df):

    return df.show()
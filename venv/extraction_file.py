import logging.config

logging.config.fileConfig('/home/mahmoud/pyspark-real-time-Application/properties/configuration/logging.conf')

loggers = logging.getLogger('extract_files')


def extract_files(df, format, filepath, num_of_part, headerReq=True, compressionType=None):

    try:
        logging.INFO("Starting Extracting Reports")

        writer = df.coalesce(num_of_part).write.mode('overwrite').format(format)

        # Add header only if CSV or similar format
        if headerReq and format in ["csv"]:
            logging.INFO(f"Saving report to a format: {format}")
            writer = writer.option("header", "true")

        # Add compression if specified
        if compressionType:
            logging.INFO(f"Saving report to a format: {format}")
            writer = writer.option("compression", compressionType)

        # Save the file
        writer.save(filepath)

    except Exception as e:
        logging.WARNING(" An Error occured while extracting report")
        raise e

    else:
        logging.INFO(" Report saved successfully")

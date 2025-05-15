import os

# sitting up environment variables 
os.environ["envn"] = "DEV"
os.environ["header"] = "True"
os.environ["inferSchema"] = "True"
os.environ["RELOAD_DATA"] = "True"


envn = os.environ["envn"]
header = os.environ["header"]
inferSchema = os.environ["inferSchema"]
RELOAD_DATA = os.environ["RELOAD_DATA"]

appName = "Spark_Application"

current = os.getcwd()

src_olap = "/home/mahmoud/pyspark-real-time-Application/source/olap"
src_oltp = "/home/mahmoud/pyspark-real-time-Application/source/oltp"


import os

# sitting up environment variables 
os.environ["envn"] = "DEV"
os.environ["header"] = "True"
os.environ["inferSchema"] = "True"
os.environ["RELOAD_DATA"] = "True"

os.environ['user'] = 'root'
os.environ['password'] = 'urdbpassword'
os.environ["url"] = "jdbc:mysql://localhost:3306/datapipeline2"

user = os.environ['user']
password = os.environ['password']
url = os.environ['url']

envn = os.environ["envn"]
header = os.environ["header"]
inferSchema = os.environ["inferSchema"]
RELOAD_DATA = os.environ["RELOAD_DATA"]

appName = "Spark_Application"

current = os.getcwd()

src_olap = "/home/mahmoud/pyspark-real-time-Application/source/olap"
src_oltp = "/home/mahmoud/pyspark-real-time-Application/source/oltp"

city_path = '/home/mahmoud/pyspark-real-time-Application/Output/cities'
presc_path = '/home/mahmoud/pyspark-real-time-Application/Output/cities'


import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("DailyIngestion").getOrCreate()

data_path = "datasets/"

def extract_date(file_name):
    match = re.search(r"(\d{8})", file_name)
    if match:
        date_str = match.group(1)
        return date_str[:4] + "-" + date_str[4:6] + "-" + date_str[6:], date_str
    return None, None

for file_name in os.listdir(data_path):
    file_path = os.path.join(data_path, file_name)

    if file_name.startswith("CUST_MSTR"):
        date_str, _ = extract_date(file_name)
        df = spark.read.option("header", True).csv(file_path)
        df = df.withColumn("Date", lit(date_str))
        df.show()

    elif file_name.startswith("master_child_export"):
        date_str, date_key = extract_date(file_name)
        df = spark.read.option("header", True).csv(file_path)
        df = df.withColumn("Date", lit(date_str)).withColumn("DateKey", lit(date_key))
        df.show()

    elif file_name.startswith("H_ECOM_ORDER"):
        df = spark.read.option("header", True).csv(file_path)
        df.show()

spark.stop()

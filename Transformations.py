# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date


# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
# Stop the Spark session when
# Read CSV file into table
df = spark.read.option("header",True) \
          .csv("D:/End_to_End/Files/Adidas_US.csv")
#df.printSchema()
df.show(10)


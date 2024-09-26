# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from  pyspark.sql.functions   import  col,to_date,regexp_replace
from pyspark.sql.types import DoubleType,IntegerType
import  pandas  as  pd


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
#Finding    Duplicaticates
print(df.count())
#9648

df=df.drop_duplicates()
print(df.count())
print(df.columns)
print(df.dtypes)
df.show(5)
columns_out=df.columns
#Finding   Null Values   into  Data  set


for  colo  in   columns_out:
      df_null = df.filter(df[colo].isNull()).count()
      print(f"Column: {colo}, Null count: {df_null}")
#  Datatypes   of    Columns
data_types=df.dtypes
print(data_types)


#Changing   data   types
df = df.withColumn('Invoice Date', to_date(F.col('Invoice Date'), 'yyyy-MM-dd')) \
       .withColumn('Units Sold', regexp_replace(F.col('Units Sold'), ',', '')) \
       .withColumn('Total Sales', regexp_replace(F.col('Total Sales'), '\\$', '')) \
       .withColumn('Operating Profit', regexp_replace(F.col('Operating Profit'), '\\$', '')) \
       .withColumn('Total Sales', regexp_replace(F.col('Total Sales'), ',', '')) \
       .withColumn('Operating Profit', regexp_replace(F.col('Operating Profit'), ',', '')) \
       .withColumn('Total Sales', F.col('Total Sales').cast(DoubleType())) \
       .withColumn('Operating Profit' ,F.col('Operating Profit').cast(DoubleType())) \
       .withColumn('Operating Margin',regexp_replace(F.col('Operating Margin'),'%','')) \
       .withColumn('Operating Margin',F.col('Operating Margin').cast(IntegerType())) \
       .withColumn('Price per Unit',regexp_replace(F.col('Price per Unit'),'\\$','')) \
       .withColumn('Price per Unit',F.col('Price per Unit').cast(DoubleType()))     
print(df.select('Price per Unit','Total Sales' ,'Price per Unit','Units Sold','Operating Profit','Operating Margin').dtypes)
print(df.dtypes)
   



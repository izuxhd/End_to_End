# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date
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
df=df.withColumn('Invoice Date',to_date(df['Invoice Date'],'yyyy-mm-dd'))

print(type(col))
df=df.withColumn('Price per Unit',col('Price per Unit').cast('float'))\
.withColumn('Total Sales',col('Total Sales').cast('float')).withColumn('Price per Unit',col('Price per Unit').cast('float'))\
      .withColumn('Units Sold',col('Units Sold').cast('float'))\
      .withColumn('Operating Profit',col('Operating Profit').cast('float'))\
      .withColumn('Operating Margin' ,col('Operating Margin').cast('float'))            
print(df.select('Price per Unit','Total Sales' ,'Price per Unit','Units Sold','Operating Profit','Operating Margin').dtypes)
print(df.dtypes)
      

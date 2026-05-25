# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`2010_summary`;

# COMMAND ----------

spark
/Workspace/Users/yalamelivishal555@gmail.com/New Pipeline 2026-01-23 16:35







# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

flight_df = spark.table("workspace.default.2010_summary")
flight_df.show(5)
  

# COMMAND ----------

flight_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/myfiles/2011-summary.csv")

flight_df.show(5)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .load("/Volumes/workspace/default/myfiles/2011-summary.csv")

df.show(5)

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------



# COMMAND ----------

flight_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType


# COMMAND ----------

my_Schema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", IntegerType(), True)
])


# COMMAND ----------


flight_df_schema = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .schema(my_Schema)\
    .option("mode","PERMISSIVE")\
    .load("/Volumes/workspace/default/myfiles/2010-summary.csv")

flight_df_schema.show(5)

# COMMAND ----------

#  Handling Corrupted Record  Handling Corrupted Record 

# Read the csv file with schema
# Read the csv file with the schema defined above 
# PERMISSIVE:  Sets corrupt fields to null, places raw record in _corrupt_record column.

emp_def =spark.read.format("csv")\
  .option("header","true")\
  .option("inferschema","true")\
  .option("mode","PERMISSIVE")\
  .load("/Volumes/workspace/default/myfiles/2010-summary.csv")
emp_def.show()

# COMMAND ----------

#  Handling Corrupted Record  Handling Corrupted Record 

'''
# In PySpark, FAILFAST is a data parsing mode (.option("mode", "FAILFAST")) used when reading structured files like CSV, JSON, or Parquet.
 
This mode instructs the parser to immediately throw an exception and stop processing if it encounters any malformed records

When to Use It
FAILFAST is ideal for high-stakes data pipelines where data integrity is paramount, such as: 
Financial Records: Where skipping one record means inaccurate totals.
Critical Data Ingestion: Where you need to know immediately if the source data format has changed. 


FAILFAST: Throws an exception and stops the application. 
'''

emp_def =spark.read.format("csv")\
  .option("header","true")\
  .option("inferschema","true")\
  .option("mode","FAILFAST")\
  .load("/Volumes/workspace/default/myfiles/2010-summary.csv")
emp_def.show()

# COMMAND ----------

#  Handling Corrupted Record  Handling Corrupted Record 

emp_Schema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("--Corrupt_record", StringType(), True)
])



# COMMAND ----------

#  Handling Corrupted Record  Handling Corrupted Record 
#  How can  we  print bad records  ?
# in corrupted column are shows .... data then use  emp_def.show(truncate=False)

emp_def =spark.read.format("csv")\
  .option("header","true")\
  .option("inferschema","true")\
  .option("mode","PERMISSIVE")\
  .schema(emp_Schema)\
  .load("/Volumes/workspace/default/myfiles/2010-summary.csv")
emp_def.show()

# COMMAND ----------

"""
Use Case: It is best used when you only care about clean data and are willing to discard any records that do not adhere to the specified schema. 

In data processing (specifically within Apache Spark/PySpark), DROPMALFORMED is a mode option used during data ingestion (reading CSV or JSON files) to handle corrupt or incorrectly formatted records. 
When DROPMALFORMED is enabled, any records that do not conform to the specified schema are dropped from the resulting DataFrame.
"""

emp_def =spark.read.format("csv")\
  .option("header","true")\
  .option("inferschema","true")\
  .option("mode","dropMalformed")\
  .load("/Volumes/workspace/default/myfiles/2010-summary.csv")
emp_def.show()

# COMMAND ----------

# Transformation and Action



# COMMAND ----------


######### DAG  creation and Lazy  evaluation  ##########

from pyspark.sql import functions as F

# 1. Create Sample Data (Triggering no job yet)
data = [("Alice", "Sales", 3400), ("Bob", "Sales", 4000), 
        ("Charlie", "IT", 2800), ("David", "IT", 4500), 
        ("Eve", "HR", 3000)]
df = spark.createDataFrame(data, ["Name", "Dept", "Salary"])

# 2. Transformations (This builds the Logical DAG)
# Narrow transformation (filter) and Wide transformation (groupBy/shuffle)
processed_df = df.filter(df.Salary > 3000) \
                 .groupBy("Dept") \
                 .agg(F.avg("Salary").alias("Avg_Salary"))

# 3. Trigger the JOB (The Action)
# This will appear in the Spark UI as a Job with multiple Stages
processed_df.show()

# 4. View the Plan in code (Optional but helpful)
processed_df.explain(True)


# COMMAND ----------

# Read JSON Data using pyspark
# Line-Delimited JSON

'''
In a true Line-delimited JSON file, there should be no commas at the end of the lines. Spark sees that comma and gets confused.
'''

spark.read.format("json")\
  .option("inferSchema","true")\
  .option("mode","permissive")\
  .load("/Volumes/workspace/default/myfiles/spark.json").show()

# COMMAND ----------

#line_delimited_extrafield.json
# Read JSON Data using pyspark
# Extra field in JSON file
spark.read.format("json")\
  .option("inferSchema","true")\
  .option("mode","permissive")\
  .load("/Volumes/workspace/default/myfiles/line_delimited_extrafield.json").show()



# COMMAND ----------

# what is Apache Parquet file

df = spark.read.parquet("/Volumes/workspace/default/myfiles/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
df.show()


# COMMAND ----------

# Dataframes to disk 
df.write.format("csv")\
  .option("header","true")\
  .option("mode","overwrite")\
  .option("path","/fileStore/tables/csv_write")\
  .save()

# COMMAND ----------



df =  spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
   .load("/Volumes/workspace/default/myfiles/employee_write.csv")
  # .load("dbfs:/FileStore/workspace/default/employee_write.csv")
    
df.show()
# Partitioning and  Bucketing
#

# COMMAND ----------

df.write.format("csv")\
  .option("header","true")\
  .mode("overwrite")\
  .save("/Volumes/workspace/default/myfiles/employee1_write.csv")


# COMMAND ----------

# Partitioning and  Bucketing



# COMMAND ----------

# Create Dataframe 

dataFrame =[(1,1),(2,1),(3,1),(4,2),(5,1),(6,2),(7,2),(8,1),(9,1),(10,1)]

# COMMAND ----------

Myschema = ['id','num']

# COMMAND ----------

spark.createDataFrame(data= dataFrame, schema=Myschema).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

# 1) Define a schema explicitly (industry best practice to avoid type ambiguity)
schema = T.StructType([
    T.StructField("id",      T.IntegerType(),  nullable=False),
    T.StructField("name",    T.StringType(),   nullable=False),
    T.StructField("age",     T.IntegerType(),  nullable=True),
    T.StructField("salary",  T.DoubleType(),   nullable=True),
    T.StructField("country", T.StringType(),   nullable=True),
    T.StructField("dept",    T.StringType(),   nullable=True),
])

# 2) Sample data rows
data = [
    (1, "Asha",   26,  55000.0, "IN", "Engineering"),
    (2, "Rohit",  29,  72000.0, "IN", "Data"),
    (3, "Meera",  24,  48000.0, "US", "Support"),
    (4, "Karan",  31,  88000.0, "UK", "Data"),
    (5, "Vijay",  None, 60000.0, "IN", "HR"),
    (6, "Vikas",  45, 45600.0, "IN", "CJ"),
    (7, "Yashu",  35,  90000.0, "IN", "Data"),
    (8, "Harshal",  45, 908000.0, "China", "Data"),
    (9, "Rajesh",  35,  90000.0, "IN", "Data"),
    (10, "Rakesh",  78,  90000.0, "IN", "Data"),
    (11, "Vijay",  None, 60000.0, "IN", "HR"),
    (12, "Vikas",  45, 45600.0, "IN", "CJ"), 
    (13, "Yashu",  35,  90000.0, "IN", "Data"), 
    (14, "Harshal",  45, 908000.0, "China", "Data"), 
    (15, "Rajesh",  35,  90000.0, "IN", "Data"),      # null age example
]

# 3) Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# 4) Inspect
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data.withColumn("adult",when(col("age")<18,"No")
                  .when(col("age")>18,"Yes")
                  .otherwise("Novalue")).show()

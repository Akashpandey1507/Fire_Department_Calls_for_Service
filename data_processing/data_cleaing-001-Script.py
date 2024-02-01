# Databricks notebook source
display(dbutils.fs.ls("dbfs:/databricks-datasets/timeseries/Fires/"))

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv",
               inferSchema=True,
               header=True
               )
df.display()

# Loading the datasets

# COMMAND ----------

df.count()

# Craete the count of dataframe

# COMMAND ----------

from pyspark.sql.functions import * 

# import the libraries

# COMMAND ----------

null_val = df.select([sum(col(i).isNull().cast('int')).alias(i) for i in df.columns])
display(null_val)

# Check the null values

# COMMAND ----------

column = ["On Scene DtTm", "Transport DtTm", "Hospital DtTm", "Call Type Group"]

for i in column:
    print(i)

# the columns where the high null values

# COMMAND ----------

for i in column:
    df = df.drop(i)

# delete the high null values columns


# COMMAND ----------

null_val = df.select([sum(col(i).isNull().cast('int')).alias(i) for i in df.columns])
display(null_val)

# again check null values

# COMMAND ----------

df = df.dropna()
# Drop the all null values

# COMMAND ----------

null_val = df.select([sum(col(i).isNull().cast('int')).alias(i) for i in df.columns])
display(null_val)
# again check the null values in the final.

# COMMAND ----------

df.columns

# columns

# COMMAND ----------

df.count()

# check dataframe size afetr the cleaned null values

# COMMAND ----------

column_datatypes = dict(df.dtypes)
column_datatypes

# COMMAND ----------

# Select columns with datatype 'int'
int_columns = [col for col, dtype in column_datatypes.items() if dtype == 'int']
int_columns

# COMMAND ----------

df[int_columns].display()

# COMMAND ----------

# Select columns with datatype 'bool'
bool_columns = [col for col, dtype in column_datatypes.items() if dtype == 'boolean']
bool_columns

# COMMAND ----------

df[bool_columns].display()

# COMMAND ----------

# Select columns with datatype 'date'
date_columns = [col for col, dtype in column_datatypes.items() if dtype == 'date']
date_columns

# COMMAND ----------

df[date_columns].display()

# COMMAND ----------

# Select columns with datatype 'string'
string_columns = [col for col, dtype in column_datatypes.items() if dtype == 'string']
string_columns

# COMMAND ----------

df[string_columns].display()

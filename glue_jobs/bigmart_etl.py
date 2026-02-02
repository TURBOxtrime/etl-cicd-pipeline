import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, mean
from datetime import datetime


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ---------- Extract ----------
input_path = f"s3://<source bucket name>/BigMart_Sales.csv"
df = spark.read.option("header", True).csv(input_path)

# ---------- Transform ----------

# 1. Handle missing Item_Weight per Item_Type
avg_weight = (
    df.groupBy("Item_Type")
    .agg(mean(col("Item_Weight")).alias("avg_weight"))
)
df = df.join(avg_weight, "Item_Type", "left")
df = df.withColumn(
    "Item_Weight",
    when(col("Item_Weight").isNull(), col("avg_weight")).otherwise(col("Item_Weight"))
).drop("avg_weight")

# 2. Fill missing Outlet_Size with mode (assuming "Medium")
df = df.fillna({"Outlet_Size": "Medium"})

# 3. Add Outlet_Age
current_year = datetime.now().year
df = df.withColumn("Outlet_Age", lit(current_year) - col("Outlet_Establishment_Year"))

# 4. Add Average_Outlet_Sales
avg_sales = (
    df.groupBy("Outlet_Identifier")
    .agg(mean(col("Item_Outlet_Sales")).alias("Avg_Sales"))
)
df = df.join(avg_sales, "Outlet_Identifier", "left")

# ---------- Load ----------
output_path = f"s3://<target bucket name>/glue_etl.parquet"
df.write.mode("overwrite").parquet(output_path)


job.commit()
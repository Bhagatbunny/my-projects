import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit

## @params: [JOB_NAME, INPUT_S3_PATH, OUTPUT_S3_PATH]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_S3_PATH', 'OUTPUT_S3_PATH'])

# Initialize Spark & Glue context
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# Read CSV from S3
df = spark.read.option("header", "true").csv(args['INPUT_S3_PATH'])

df = df.toDF(*[c.lower() for c in df.columns])

df = df.withColumnRenamed("1d_returns", "one_day_returns")
df = df.withColumnRenamed("change_%", "change_percent")

# Add new column with load timestamp
df = df.withColumn("load_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

df.write.mode("overwrite").option("header", "true").csv(args['OUTPUT_S3_PATH'])

job.commit()
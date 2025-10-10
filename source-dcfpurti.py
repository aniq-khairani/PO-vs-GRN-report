import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, to_timestamp
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import boto3

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

s3_client = boto3.client('s3')
bucket = "kkmart-da-k3"
source_prefix = "temp/DCFPURTI/"

# Step 1: Filter untagged files (not yet ingested)
def get_untagged_files(bucket, prefix):
    untagged_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):  # Only process CSV files
                tags = s3_client.get_object_tagging(Bucket=bucket, Key=key).get('TagSet', [])
                # Check if 'ingested' tag is absent or not 'true'
                ingested_tag = next((tag for tag in tags if tag['Key'] == 'ingested'), None)
                if not ingested_tag or ingested_tag['Value'] != 'true':
                    untagged_files.append(f"s3://{bucket}/{key}")
    return untagged_files
    
# Get list of untagged files
file_list = get_untagged_files(bucket, source_prefix)
if not file_list:
    print("No new files to process. Exiting.")
    job.commit()
    sys.exit(0)

print(f"Processing files: {file_list}")

def convert_columns(df, type_mapping):
    for column, dtype in type_mapping.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df

def convert_date_columns(df, date_columns, format="yyyy-MM-dd HH:mm:ss"):
    for column in date_columns:
        df = df.withColumn(column, to_date(col(column), format))
    return df

# Read only untagged CSV files
data_df = spark.read.csv(
    file_list,  # List of S3 paths instead of wildcard
    header=True,
    sep="|",
    inferSchema=False,
    mode="DROPMALFORMED"
)
data_df = data_df.toDF(*[c.lower() for c in data_df.columns])

type_mapping = {
"str_no": "varchar(8)",
 "seq_no": "decimal(8,0)",
 "item_no": "decimal(5,0)",
"goo_no": "varchar(13)",
"plu_no": "varchar(16)",
 "iprice": "decimal(12,4)",
 "qty": "decimal(8,2)",
 "tqty": "decimal(8,2)",
 "pn1": "decimal(8,2)",
 "pn2": "decimal(8,2)",
"pgoo": "varchar(13)",
"gift_yn": "varchar(1)",
 "amt": "decimal(14,3)",
 "tax": "decimal(12,3)",
 "ht_price": "decimal(12,4)",
 "ht_amt": "decimal(12,3)",
"proc_code": "varchar(1)",
"upd_no": "varchar(10)",
"crt_no": "varchar(10)",
"sale_yn": "varchar(1)",
 "payment_dis": "decimal(12,3)",
"memo": "varchar(80)",
"s_goo_no": "varchar(13)",
 "ori_qty": "decimal(9,2)",
"ori_plu_no": "varchar(18)",
 "qty1": "decimal(8,2)",
 "qty2": "decimal(8,2)",
"batch_no": "varchar(20)",
"rg_ind": "varchar(5)",
"par_goo": "varchar(13)",
 "par_no": "decimal(5,0)",
"spck_qty": "varchar(20)",
"pty_no": "varchar(8)"
}
date_cols = [
    "kdate", 
    "expire_date"
    ]

result = convert_columns(data_df, type_mapping)
result = convert_date_columns(result, date_cols)

result = result.withColumn("rg_date", to_timestamp(col("rg_date"), "yyyy-MM-dd HH:mm:ss"))
result = result.withColumn("proc_date", to_timestamp(col("proc_date"), "yyyy-MM-dd HH:mm:ss"))
result = result.withColumn("upd_date", to_timestamp(col("upd_date"), "yyyy-MM-dd HH:mm:ss"))
result = result.withColumn("crt_date", to_timestamp(col("crt_date"), "yyyy-MM-dd HH:mm:ss"))
result = result.withColumn("exp_date", to_timestamp(col("exp_date"), "yyyy-MM-dd HH:mm:ss"))

result = result.withColumn("extracted_date", to_timestamp(col("extracted_date"), "dd/MM/yyyy HH:mm:ss"))

dynamic_frame = DynamicFrame.fromDF(result, glueContext, "dynamic_frame")

redshift_options = {
    "url": "jdbc:redshift://kkmart-redshift.cyk7xpzn3jbq.ap-southeast-1.redshift.amazonaws.com:5439/kkmart",
    "dbtable": "source.dcfpurti",
    "user": "admin",
    "password": "BZGUUzwush472)*",
    "redshiftTmpDir": f"s3://kkmart-da-poc-luqman-analysis/poc-4-moving-average/dcfpurti-temp/{datetime.now().strftime('%Y%m%d')}/"
}

try:
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="redshift",
        connection_options=redshift_options
    )
    print("Data written to Redshift")
    
    # Step 2: Tag ingested files
    for file_path in file_list:
        key = file_path.replace(f"s3://{bucket}/", "")
        s3_client.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={
                'TagSet': [
                    {'Key': 'ingested', 'Value': 'true'},
                    {'Key': 'ingestion_date', 'Value': datetime.now().strftime('%Y-%m-%d')}
                ]
            }
        )
    print("Files tagged as ingested")
except Exception as e:
    print(f"Error during ingestion or tagging: {str(e)}")
    raise

job.commit()
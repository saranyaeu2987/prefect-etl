from itertools import chain
from datetime import timedelta

import prefect
from HEBDataUtil.aws.S3.S3 import S3Connection
from HEBDataUtil.spark.spark import get_spark_session
from prefect import task
from pyspark.sql.functions import col
from pyspark.sql.functions import map_concat, create_map, lit, map_keys
import time
import boto3


@task(max_retries=2, retry_delay=timedelta(seconds=5))
def write_delta_lookup_table(s3_source_bucket: str, s3_source_bucket_raw_prefix:str, dest_delta_prefix:str, table_name:str, date_hour_path:str, pk:str):
    try:
        s3_connection = S3Connection(default_bucket=None)
        spark = get_spark_session()
        spark.conf.set("spark.sql.mapKeyDedupPolicy", 'LAST_WIN')
        spark_context = spark.sparkContext
        sql_context = spark.builder.getOrCreate()
        logger = prefect.context.get("logger")

        s3_location = f"s3a://{s3_source_bucket}/{s3_source_bucket_raw_prefix}"
        hour_data_path = f"{s3_location}/{date_hour_path}"
        file_list = s3_connection.read_s3_folder(s3_folder_path=f"{s3_source_bucket_raw_prefix}/{date_hour_path}",s3_bucket=s3_source_bucket)
        logger.info(f"file_list --> {file_list}")
        if len(file_list)<=0:
            logger.warn(f"No transaction data in path - {hour_data_path}")
        else:
            """
            lookup_table_df = spark_context.textFile(hour_data_path)
            lookup_json_df = sql_context.read.json(lookup_table_df)
            lookup_snapshot_df = lookup_json_df.select("op_type", "op_ts","after.*")
            logger.info(f"Delta_raw path - {s3_location}/{dest_delta_prefix}")
            lookup_snapshot_df.write.format("delta").mode("append").save(f"{s3_location}/{dest_delta_prefix}")
            sql_context.sql("CREATE DATABASE IF NOT EXISTS test;")
            sql_context.sql(f"CREATE TABLE {table_name}_raw USING DELTA LOCATION '{s3_location}/{dest_delta_prefix}';")
            logger.info(f"Delta write complete in path - {s3_location}/{dest_delta_prefix} . Table name - {table_name}")

            filter = sql_context.sql(f"select row_number() OVER (partition by LIN_OF_BUS_ID order by LIN_OF_BUS_ID, op_ts desc) as row_number, * from {table_name}_raw order by LIN_OF_BUS_ID, current_ts desc ")

            filter.where("row_number =1 and op_type <> 'D'").select("*").drop("op_ts","op_type").write.format("delta").mode("overwrite").save(
                f"{s3_location}/delta_snapshot/")
            """



            lookup_table_df = spark_context.textFile(hour_data_path)
            cleansed_df =  lookup_table_df.map(lambda s: s.replace("{\"table","[{\"table",1)).map(lambda s: s.replace("}{\"table","},{\"table")).map(lambda s: s.replace("}}","}}]")).map(lambda s: s.replace("}}],","}},"))
            lookup_json_df = sql_context.read.json(cleansed_df)

            update_df = lookup_json_df.where("op_type='U'").select("*")
            insert_df = lookup_json_df.where("op_type='I'").select("*")
            delete_df = lookup_json_df.where("op_type='D'").select("*")

            #Handling update transactions
            if (update_df.count() != 0):
                lookup_after = update_df.select("after.*")
                lookup_before = update_df.select("before.*")
                logger.info(f'op_ts - {list((lit("op_ts"),"op_ts"))}')
                op_ts = list((lit("op_ts"),"op_ts"))
                op_type = list((lit("op_type"),"op_type"))
                op_ts.extend(op_type)

                a = list(chain(*((lit(name), ("after."+name)) for name in lookup_after.columns)))
                a.extend(op_ts)
                b = list(chain(*((lit(name), ("before."+name)) for name in lookup_before.columns)))
                b.extend(op_ts)

                after = create_map(a).alias("after")
                before = create_map(b).alias("before")
                final = update_df.select(map_concat(before,after).alias("final"))

                keys = final.select(map_keys("final").alias("keys")).first()
                exprs = [col("final").getItem(k).alias(k) for k in keys['keys']]
                raw = final.select(*exprs)
                time.sleep(2)
                raw.write.format("delta").mode("append").save(f"{s3_location}/{dest_delta_prefix}")

            #Handling insert transactions
            if (insert_df.count() != 0):
                i_df = insert_df.select("after.*","op_type","op_ts")
                i_df = i_df.select(*(col(c).cast("String").alias(c) for c in i_df.columns))
                time.sleep(2)
                i_df.write.format("delta").mode("append").save(f"{s3_location}/{dest_delta_prefix}")

            #Handling delete transactions
            if (delete_df.count() != 0):
                d_df = delete_df.select("before.*","op_type","op_ts")
                d_df = d_df.select(*(col(c).cast("String").alias(c) for c in d_df.columns))
                time.sleep(2)
                d_df.write.format("delta").mode("append").save(f"{s3_location}/{dest_delta_prefix}")

            #table creation to run window functions
            sql_context.sql("CREATE DATABASE IF NOT EXISTS test;")
            sql_context.sql(f"drop table if exists {table_name}_raw")
            sql_context.sql(f"CREATE TABLE {table_name}_raw USING DELTA LOCATION '{s3_location}/{dest_delta_prefix}';")
            logger.info(f"Delta write complete in path - {s3_location}/{dest_delta_prefix} . Table name - {table_name}")
            # To support S3 lag (https://issues.apache.org/jira/browse/SPARK-18512)
            time.sleep(5)
            #sort created table rows and filter delete transactions and old transactions
            ordered_df = sql_context.sql(f"select row_number() OVER (partition by {pk} order by {pk}, op_ts desc) as row_number, * from {table_name}_raw order by {pk}, op_ts desc ")
            filter = ordered_df.where("row_number =1 and op_type <> 'D'").select("*").drop("op_ts","op_type")

            #delete snapshot folder since mode(overwrite) has lag
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(s3_source_bucket)
            for obj in bucket.objects.filter(Prefix=f"{s3_source_bucket_raw_prefix}/delta_snapshot"):
                s3.Object(bucket.name, obj.key).delete()
            filter.write.format("delta").mode("overwrite").save(f"{s3_location}/delta_snapshot/")

            #Optimize Delta table and vacuum files
            # sql_context.sql(f"CREATE TABLE {table_name}_snapshot USING DELTA LOCATION '{s3_location}/delta_snapshot/';")
            # sql_context.sql(f"OPTIMIZE {table_name}_snapshot")
            # sql_context.sql(f"VACUUM {table_name}_snapshot RETAIN 0 HOURS")
    except Exception as e:
        raise Exception('Exception caught - '+ str(e))

@task
def final_task(date_hour_path:str):
    logger = prefect.context.get("logger")
    logger.info(f"Task successfully completed for all test tables for datetime - {date_hour_path}")

@task
def get_S3_connection():
    return S3Connection(default_bucket=None)

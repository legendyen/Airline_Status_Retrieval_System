from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GCS Files Processing") \
    .getOrCreate()

df = spark.read.csv('gs://test_5day_3hour_weather_forecast/weather_forecasts/5_days_3_hour_forecast_2024*.csv', header=True, inferSchema=True)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define the schema of your DataFrame columns for reference
numeric_columns = ['main_temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'sea_level', 'grnd_level', 'humidity', 'temp_kf', 'clouds', 'wind_speed', 'wind_deg', 'wind_gust', 'visibility', 'pop', 'rain_3h']
categorical_columns = ['weather_main', 'weather_description', 'weather_icon', 'sys_pod']

# Group by 'dt_txt' and calculate averages for numeric columns, rounding to two decimal places
average_df = df.groupBy('dt_txt').agg(*(F.round(F.avg(c), 2).alias(c) for c in numeric_columns))

# For each categorical column, find the mode (most frequent value)
mode_dfs = []
for column in categorical_columns:
    mode_df = df.groupBy('dt_txt', column).count().withColumnRenamed('count', f'{column}_count')
    # Window function to determine the row with the highest count per group
    window = Window.partitionBy('dt_txt').orderBy(F.desc(f'{column}_count'))
    mode_df = mode_df.withColumn('rn', F.row_number().over(window)).filter(F.col('rn') == 1).drop('rn', f'{column}_count')
    mode_dfs.append(mode_df)

# Assuming all mode DataFrames have the same 'dt_txt' ordering, join them back to the average DataFrame
# You may need to adjust this if 'dt_txt' ordering isn't consistent
for mode_df in mode_dfs:
    average_df = average_df.join(mode_df.drop('count'), ['dt_txt'], 'inner')

# average_df.orderBy('dt_txt').show(average_df.count())

# Configure the destination BigQuery table and temporary GCS bucket
bq_table = "data-engineer-project-411122.clean_5day_3hour_weather_forecast.ml"
temp_gcs_bucket = "gs://dataproc-temp-us-central1-795990509558-u4gpyog1"

# # Change PySpark DataFrame back to Spark DataFrame
# spark_df = ps_df.to_spark()

# Write the DataFrame to BigQuery
average_df.write \
    .format("bigquery") \
    .option("table", bq_table) \
    .option("temporaryGcsBucket", temp_gcs_bucket) \
    .mode("append").save()

from google.cloud import storage

def move_files_and_maintain_folder(source_bucket_name, source_folder, destination_bucket_name, destination_folder):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blobs = source_bucket.list_blobs(prefix=source_folder)
    files_moved = False
    for blob in blobs:
        source_blob = source_bucket.blob(blob.name)
        destination_blob_name = blob.name.replace(source_folder, destination_folder, 1)
        destination_blob = destination_bucket.blob(destination_blob_name)

        # Copy the blob to the new destination
        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
        
        # Delete the original blob
        source_blob.delete()
        files_moved = True

    if files_moved:
        # Create a placeholder file in the original folder
        placeholder_blob = source_bucket.blob(source_folder + "placeholder.txt")
        placeholder_blob.upload_from_string("This is a placeholder to maintain the folder structure.")

# Example usage:
move_files_and_maintain_folder('test_5day_3hour_weather_forecast', 'weather_forecasts/', 'test_5day_3hour_weather_forecast', 'weather_forecasts_processed/')
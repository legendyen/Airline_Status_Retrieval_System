from pyspark.sql import SparkSession

# Set up a Spark session and read it into dataframe
spark = SparkSession.builder.appName('GCS Files Processing').getOrCreate()

# Read the folder of unprocessed dataset
path = 'gs://test_3hours_flight_data/flight_data/real_time_flights_2024*.csv'
df = spark.read.csv(path, header = True, inferSchema = True)

# Drop redundant columns
columns_to_drop = ['flight_number', 'flight_date', 'departure_estimated', 'departure_estimated_runway', 'departure_actual_runway', 'arrival_estimated', 'arrival_estimated_runway',
    'arrival_actual_runway','live_update_time', 'live_latitude', 'live_longitude', 'live_altitude', 'live_speed_horizontal', 'live_speed_vertical', 'live_is_ground']
df = df.drop(*columns_to_drop)

# Rearrange columns
order = [
    'flight_iata',
    'departure_scheduled',
    'departure_actual',
    'departure_delay',
    'arrival_scheduled',
    'arrival_actual',
    'arrival_delay',
    'flight_status',
    'departure_iata',
    'departure_airport',
    'departure_timezone',
    'arrival_iata',
    'arrival_airport',
    'arrival_timezone',
    'airline_iata',
    'airline_name'
]

df = df.select(order)

# Filter out rows where both 'departure_actual' and 'arrival_actual' are NULL
filtered_df = df.filter((df.departure_actual.isNotNull()) & (df.arrival_actual.isNotNull()) & (df.flight_status!='active'))

# Order by 'flight_iata' and 'departure_scheduled'
sorted_df = filtered_df.orderBy(df.flight_iata, df.departure_scheduled)

# Drop duplicate flights infromation
df_new = sorted_df.dropDuplicates(['flight_iata', 'departure_scheduled', 'departure_actual', 'arrival_scheduled', 'arrival_actual']).orderBy('flight_iata')

from pyspark.sql.functions import col, expr, hour, from_unixtime, unix_timestamp

# Add a new column 'UTC_timezone' by converting 'departure_scheduled' from EDT to UTC
df_new = df_new.withColumn("UTC_timezone", expr("from_utc_timestamp(departure_scheduled, 'UTC-4')"))

# Modify the 'UTC_timezone' column to round down to the nearest 3-hour interval
df_new = df_new.withColumn(
    "UTC_time_interval",
    from_unixtime(
        (unix_timestamp("UTC_timezone") / (3 * 3600)).cast("int") * (3 * 3600)
    ).cast("timestamp")
)

# Configure the destination Bigquery table
bq_table_path = 'data-engineer-project-411122.clean_flight_data.ml'
temp_gcs_bucket = "gs://dataproc-temp-us-central1-795990509558-u4gpyog1"

# # Change PySpark DataFrame back to Spark DataFrame
# spark_df = pandas_df.to_spark()

# Write the DataFrame to BigQuery
df_new.write \
    .format("bigquery") \
    .option("table", bq_table_path) \
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
move_files_and_maintain_folder('test_3hours_flight_data', 'flight_data/', 'test_3hours_flight_data', 'flight_data_processed/')
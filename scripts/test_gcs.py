from pyspark.sql import SparkSession
import os

def test_gcs_connection():
    # Initialize Spark Session
    # We include the GCS connector package which is required to read gs:// paths
    spark = SparkSession.builder \
        .appName("GCSTestConnection") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

    print("==================================================")
    print("Spark Session created successfully.")
    
    # The GCS connector will automatically pick up the credentials from the 
    # GOOGLE_APPLICATION_CREDENTIALS environment variable that we set in docker-compose.
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"Using Google Credentials from: {cred_path}")
    
    # Replace this with your actual bucket name
    bucket_name = "logistics-data-platform" # Ensure this matches your actual bucket
    test_path = f"gs://{bucket_name}/raw/"
    
    try:
        print(f"Attempting to list or access: {test_path}")
        # A simple operation to trigger GCS access
        df = spark.read.format("csv").load(test_path)
        print("SUCCESS! Successfully connected to Google Cloud Storage and accessed the raw folder.")
    except Exception as e:
        print("FAILED to connect or read from GCS. See error below:")
        print(str(e))
    finally:
        print("==================================================")
        spark.stop()

if __name__ == "__main__":
    test_gcs_connection()

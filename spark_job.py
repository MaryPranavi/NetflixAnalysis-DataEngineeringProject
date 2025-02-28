from pyspark.sql import SparkSession

# Initialize Spark session with S3 configuration
spark = SparkSession.builder \
    .appName("Netflix Titles Analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()

# Configure S3A filesystem to use credentials file
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
               "com.amazonaws.auth.profile.ProfileCredentialsProvider")

# Use s3a:// protocol
s3_path = "s3a://de-netflix-proj/netflix_titles.csv"

# Read the CSV file
netflix_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(s3_path)

# Display the first few rows
#netflix_df.show(5)

# Get basic information about the DataFrame
#print("DataFrame Schema:")
#netflix_df.printSchema()

print("Row Count:", netflix_df.count())
print("\nDATA CLEANING AND TRANSFORMATION")

# ----- DATA CLEANING AND TRANSFORMATION -----

# 1. Handle Missing Data
df_cleaned = netflix_df.dropna(subset=["title", "release_year", "rating"])
print("\nData after handling missing values:")
df_cleaned.show(5)
print(f"Cleaned Row Count: {df_cleaned.count()}")

   # Fill missing values
df_filled = df_cleaned.fillna({"director": "Unknown", "cast": "Unknown"})

# 2. Filter Data (e.g., Movies Released After 2015)
df_filtered = df_filled.filter(df_cleaned["release_year"] > 2015)
print("\nData filtered for movies after 2015:")
df_filtered.show(5)
print(f"Filtered Row Count: {df_filtered.count()}")

   # Filter TV Shows
df_tv_shows = df_filtered.filter(df_filtered["type"] == "TV Show")
print("\nData showing those are TV shows:")
df_tv_shows.show(5)
print(f"TV Shows Row Count: {df_tv_shows.count()}")

# 3. Select Specific Columns
df_selected = df_tv_shows.select("title", "release_year", "rating")
print("\nSelected columns only:")
df_selected.show(5)

# --------- INSIGHTS ---------------
#Count of movies vs TV shows
netflix_df.groupBy("type").count().show()

#number of titles per year
netflix_df.groupBy("release_year").count().orderBy("release_year", ascending=False).show(10)


# -----  SAVE PROCESSED DATA BACK TO S3 -----

# Save the processed data as Parquet format back to S3
output_path = "s3a://de-netflix-proj/processed/netflix_titles_cleaned.parquet"
df_filled.write.parquet(output_path, mode="overwrite")
print(f"\nProcessed data saved to: {output_path}")

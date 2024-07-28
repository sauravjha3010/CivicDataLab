from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, upper

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Load the data into a DataFrame
data_path = "/content/Assignment Task _ Dataset - Sheet1.csv"
df = spark.read.option("header", "true").csv(data_path)

# 1. Data Cleaning

# Handle missing values by replacing empty strings with None
df = df.replace('', None)

# 2. Standardize Gender Column
df = df.withColumn("Gender", upper(col("Gender")))

# Map various gender values to 'M' and 'F'
df = df.withColumn("Gender", when(col("Gender") == "MALE", "M")
                              .when(col("Gender") == "FEMALE", "F")
                              .when(col("Gender") == "F", "F")
                              .when(col("Gender") == "M", "M")
                              .otherwise(None))

# 3. Standardize Date Columns
df = df.withColumn("Join_Date", to_date(col("Join_Date"), "yyyy-MM-dd"))
df = df.withColumn("Last_Login", to_date(col("Last_Login"), "yyyy-MM-dd"))

# 4. Remove duplicates based on ID or Name and Email
df = df.dropDuplicates(["ID"])
df = df.dropDuplicates(["Name", "Email"])

# 5. Validate and clean email addresses
df = df.withColumn("Email", when(df.Email.contains('@'), df.Email).otherwise(None))

# 6. Remove rows with null values in essential columns (ID, Name, Email, Gender)
df = df.dropna(subset=["ID","Name","Age","Gender","Email","Join_Date","Last_Login"])

# Show the cleaned DataFrame

# Show the cleaned DataFrame
df.write.mode("overwrite").parquet("/content/data")

# Additional analysis or transformations can be added here
spark.stop()

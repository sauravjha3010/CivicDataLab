from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, when, upper

class DataCleaningPipeline:
    def __init__(self, spark: SparkSession, data_path: str):
        self.spark = spark
        self.df = self.spark.read.option("header", "true").csv(data_path)

    def handle_missing_values(self, column: str):
        """Replace empty strings with None in the specified column."""
        self.df = self.df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
        return self

    def standardize_gender(self):
        """Standardize the gender column to 'M' and 'F'."""
        self.df = self.df.withColumn("Gender", upper(col("Gender")))
        self.df = self.df.withColumn("Gender", when(col("Gender").isin("MALE", "M"), "M")
                                     .when(col("Gender").isin("FEMALE", "F"), "F")
                                     .otherwise(None))
        return self

    def standardize_dates(self, columns: list):
        """Convert specified columns to date type."""
        for column in columns:
            self.df = self.df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))
        return self

    def remove_duplicates(self, columns: list):
        """Remove duplicates based on the specified columns."""
        self.df = self.df.dropDuplicates(columns)
        return self

    def validate_email(self):
        """Validate email addresses and replace invalid emails with None."""
        self.df = self.df.withColumn("Email", when(self.df.Email.contains('@'), self.df.Email).otherwise(None))
        return self

    def drop_nulls(self, columns: list = None):
        """Drop rows with null values in the specified columns. If no columns are specified, drop rows with nulls in any column."""
        if columns:
            self.df = self.df.dropna(subset=columns)
        else:
            self.df = self.df.dropna()
        return self

    def show_data(self, num_rows: int = 20):
        """Display the cleaned data."""
        self.df.show(num_rows)

    def save_data(self, output_path: str):
        """Save the cleaned data to a specified path."""
        self.df.write.mode("overwrite").parquet(output_path)

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Initialize the pipeline with the data path
pipeline = DataCleaningPipeline(spark, "/content/Assignment Task _ Dataset - Sheet1.csv")

# Run the data cleaning steps
pipeline.handle_missing_values("Name")\
        .handle_missing_values("Email")\
        .standardize_gender()\
        .standardize_dates(["Join_Date", "Last_Login"])\
        .remove_duplicates(["ID"])\
        .validate_email()\
        .drop_nulls()  # Show cleaned data

# Save the cleaned data if needed
pipeline.save_data("/content/data")

# tasks/identify_missing_values.py

from pyspark.sql import DataFrame

class IdentifyMissingValuesTask:
    def __init__(self, columns, threshold):
        self.columns = columns
        self.threshold = threshold
        self.results = {}

    def execute(self, df: DataFrame) -> DataFrame:
        for column in self.columns:
            missing_count = df.filter(df[column].isNull()).count()
            total_count = df.count()
            missing_percentage = (missing_count / total_count) * 100
            self.results[column] = missing_percentage
        return df

    def log_results(self):
        print(f"Missing Values Results: {self.results}")

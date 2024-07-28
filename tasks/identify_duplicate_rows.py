# tasks/identify_duplicate_rows.py

from pyspark.sql import DataFrame

class IdentifyDuplicateRowsTask:
    def __init__(self, columns):
        self.columns = columns
        self.duplicates_count = 0

    def execute(self, df: DataFrame) -> DataFrame:
        self.duplicates_count = df.count() - df.dropDuplicates(self.columns).count()
        return df

    def log_results(self):
        print(f"Duplicate Rows Count: {self.duplicates_count}")

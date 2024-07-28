# app.py

from flask import Flask, request, jsonify
from pipeline_manager import PipelineManager
from tasks import IdentifyMissingValuesTask, IdentifyDuplicateRowsTask

app = Flask(__name__)
pipeline_manager = PipelineManager()

@app.route('/create_pipeline', methods=['POST'])
def create_pipeline():
    tasks_config = request.json.get('tasks', [])
    for task_config in tasks_config:
        task_type = task_config.get('type')
        if task_type == 'missing_values':
            task = IdentifyMissingValuesTask(
                columns=task_config.get('columns', []),
                threshold=task_config.get('threshold', 0)
            )
        elif task_type == 'duplicate_rows':
            task = IdentifyDuplicateRowsTask(
                columns=task_config.get('columns', [])
            )
        else:
            continue  # Invalid task type
        pipeline_manager.add_task(task)
    return jsonify({"message": "Pipeline created successfully"}), 201

@app.route('/run_pipeline', methods=['POST'])
def run_pipeline():
    df = load_data()
    pipeline_manager.run(df)
    return jsonify({"message": "Pipeline executed successfully"}), 200

def load_data():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("DataCleaning").getOrCreate()
    data_path = "/path/to/your/file.csv"  # Replace with the actual path to your data
    return spark.read.option("header", "true").csv(data_path)

if __name__ == '__main__':
    app.run(debug=True)

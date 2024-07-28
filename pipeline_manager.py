# pipeline_manager.py

class PipelineManager:
    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def run(self, df):
        for task in self.tasks:
            df = task.execute(df)
            task.log_results()
        return df

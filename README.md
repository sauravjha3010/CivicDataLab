# Data Cleaning Framework

## Overview

This project is a configurable data cleaning pipeline framework that allows users to process datasets and ensure data quality. The framework includes a library of atomic data cleaning tasks and supports API-based configuration and execution.

## Also check etl_script.py for given dataset in assingment used the object oriented and/or functional programming paradigm. 

## Features

- **Identify Missing Values**: Checks for missing values in specified columns with configurable thresholds.
- **Identify Duplicate Rows**: Checks for duplicate rows in the dataset based on specified columns.

## Installation

1. Clone the repository:
git clone  https://github.com/sauravjha3010/CivicDataLab-data-cleaning-framework.git

2. Install the dependencies:
pip install -r requirements.txt

## Usage

### API Endpoints

- **Create Pipeline**: `/create_pipeline` (POST)
- **Request Body**:
 ```json
 {
   "tasks": [
     {
       "type": "missing_values",
       "columns": ["col1", "col2"],
       "threshold": 0
     },
     {
       "type": "duplicate_rows",
       "columns": ["id"]
     }
   ]
 }
 ```
- **Response**: 201 Created

- **Run Pipeline**: `/run_pipeline` (POST)
- **Response**: 200 OK

## Contributing

To add or update tasks, follow the guidelines provided in `CONTRIBUTING.md`.

Additional Notes:
Data Loading: The load_data function in app.py should be customized to load your specific dataset.
Error Handling: This basic implementation does not include robust error handling or input validation, which should be added for production use.
Task Expansion: Additional tasks can be added to the tasks/ directory and integrated similarly to the existing tasks.
This setup provides a foundational framework for building a configurable data cleaning pipeline, allowing for easy extension and customization based on specific data quality requirements.

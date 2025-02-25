lmao doc la cho
day la commit tu terminal

# Temperature_ELT_Pipeline-Spark_Airflow

## Project Overview
This project automates the extraction, loading, and visualization of temperature data using Apache Airflow, MySQL, and PySpark. It monitors a directory as data resource, then ingests CSV files to the pipeline, loads them into a MySQL database, transforms and generates visual insights of average temperatures across regions and months.

## Architecture
1. Data Ingestion: Monitor a directory and watch for the presence of new CSV files using Airflow PythonSensor.

2. MySQL Loading: Load new CSV files into MySQL database one by one using Airflow's MySqlHook.

3. Data Visualization: Use PySpark to fetch data from MySQL table and generate visualization of temperature data.

4. Orchestration: Use Airflow DAG to manages and orchestrates end-to-end workflow.

## Technologies
- Apache Airflow
- MySQL
- PySpark, Pandas
- Matplotlib, Seaborn

## Project Structure
Temperature_ELT_Pipeline-Spark_Airflow  
├── airflow/  
│   └── dags/  
│       └── temperature_pipeline.py   # Airflow DAG  
├── data/  
│   ├── raw_source.txt                # Kaggle URL for downloading  
│   └── simulated_raw_source/         # Data after simulation for pipeline event checking  
├── modules/  
│   ├── analysis_for_visualize/  
│   │   └── spark_analysis.ipynb      # Pre-analysis for visualizing  
│   ├── fetch_and_visualize/  
│   │   ├── pyspark_load_visualize.py # Running modules  
│   │   └── pyspark_modules.py        # Modules for fetching and visualizing  
│   ├── monitor_and_loadMySQL/  
│   │   └── python_modules.py         # Monitor directory and load to MySQL  
│   └── simulate_data_source/  
│       └── analysis_simulate.ipynb   # Handle raw file in Kaggle to simulate event checking  
└── README.md  


## Visualization Output
* Yearly Average Temperature by Region (Bar Chart)
* Monthly Average Temperature by Region (Heatmap)

import os
from airflow.providers.mysql.hooks.mysql import MySqlHook

def check_new_csv(watch_dir, processed_files, **kwargs):
    # Get list of csv files in watching directory
    files = [f for f in os.listdir(watch_dir) if f.endswith(".csv")]

    # Get new files
    new_files = [f for f in files if f not in processed_files]

    # Update processed files
    processed_files.extend(new_files)

    # Push new files to XCom for downstream tasks
    ti = kwargs['ti']
    ti.xcom_push(key="new_files", value=new_files)

    return len(new_files) > 0

def load_csv_to_mysql(new_files, **kwargs):
    # Connect to MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_local')

    # Load new files to MySQL
    for file in new_files:
        try:
            sql = f"""
            LOAD DATA INFILE '{file}'
            INTO TABLE temperature_data
            FIELDS TERMINATED BY ','
            IGNORE 1 ROWS;
            """
            mysql_hook.run(sql)
            print(f"Loaded file: {file}")
        except Exception as e:
            print(f"Failed to load {file}: {e}")
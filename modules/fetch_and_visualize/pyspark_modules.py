# Import necessary libraries
import os
import pandas as pd # Use for data manipulation and small dataset
from matplotlib import pyplot as plt
import seaborn as sns

# Function for loading data from mysql
def mysql_ingestor(spark):
    jdbc_url = "jdbc:mysql://localhost:3306/Temperature"
    table_name = "temperature_data"
    db_properties = {
        "user": "root",
        "password": "Bavinh2704@",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Load the table into a Spark DataFrame
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
    return df

# Function to visualize the yearly average temperature of each region in the dataset
def visualize_yearly_average_region_temp(df):
    region_avg_temp = df.groupBy("Region").avg("AvgTemperature")
    region_avg_temp = region_avg_temp.toPandas()
    # Visualize
    plt.figure(figsize=(10, 5))
    bars = plt.bar(region_avg_temp["Region"], region_avg_temp["avg(AvgTemperature)"])
    for bar in bars:
        plt.text(
            bar.get_x() + bar.get_width() / 2, # x-coordinate
            bar.get_height(), # y-coordinate
            round(bar.get_height(), 2), # value text (rounded to 2 decimal places)
            ha="center", # horizontal alignment
            va="bottom" # vertical alignment
        )
    plt.xlabel("Region")
    plt.ylabel("Average Temperature")
    plt.title("Average Temperature of Each Region")
    plt.xticks(rotation=45)
    plt.show()

    # Show the plot
    plt.show()

# Function to visualize the monthly average temperature of each region in the dataset
def visualize_monthly_average_region_temp(df):
    region_month_avg_temp = df.groupBy("Region", "Month").avg("AvgTemperature")
    region_month_avg_temp = region_month_avg_temp.toPandas()
    heatmap_data = region_month_avg_temp.pivot(index="Region", columns="Month", values="avg(AvgTemperature)")
    # Visualize
    plt.figure(figsize=(10, 5))
    sns.heatmap(heatmap_data, annot=True, cmap="coolwarm", fmt=".1f", linewidths=0.5)
    # Add labels and title
    plt.title("Average Monthly Temperature by Region")
    plt.xlabel("Month")
    plt.ylabel("Region")
    plt.xticks(rotation=0)
    plt.tight_layout()

    # Show the plot
    plt.show()
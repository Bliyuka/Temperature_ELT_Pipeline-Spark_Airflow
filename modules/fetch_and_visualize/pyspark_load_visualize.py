from pyspark.sql import SparkSession
from pyspark_modules import mysql_ingestor, visualize_yearly_average_region_temp, visualize_monthly_average_region_temp

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Temperature_Pipeline").getOrCreate()

    # Load data from MySQL
    df = mysql_ingestor(spark)
    print(f"Data loaded from MySQL: {df.count()} rows")

    # Visualize the yearly average temperature of each region
    visualize_yearly_average_region_temp(df)

    # Visualize the monthly average temperature of each region
    visualize_monthly_average_region_temp(df)

    print(f"Data visualization completed")


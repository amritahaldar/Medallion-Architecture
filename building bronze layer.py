# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists amazon_mskl

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists amrita_haldar_databricks_npmentorskool_onmicrosoft_com.bronze_ecom

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data should be available in ADLS & Container should be mounted in Databricks
# MAGIC

# COMMAND ----------



# COMMAND ----------

# creating a list of all the tables (csv files) in the mounted bucket
# Ensure to replace the mount path with your own mount path

csv_files = dbutils.fs.ls(f"dbfs:/mnt/globalmart_data/")
Tables = [file.name[:-4] for file in csv_files if file.name.endswith('.csv')]
print(Tables)

# COMMAND ----------

import re

def clean_and_save_csv(mount_name, table_name):
    # Step 1: Read the CSV file from the mounted path
    df = spark.read.option("header", "true").csv(f"dbfs:/mnt/{mount_name}/{table_name}.csv")

    # Step 2: Display original columns
    print("Original Columns:", df.columns)

    # Step 3: Clean the column names by replacing special characters with underscores
    cleaned_columns = []
    for col in df.columns:
        new_col = col.replace(" ", "_").replace(",", "_").replace(";", "_").replace("{", "_") \
                     .replace("}", "_").replace("(", "_").replace(")", "_").replace("\n", "_") \
                     .replace("\t", "_").replace("=", "_").rstrip('_')
        df = df.withColumnRenamed(col, new_col)

    # Step 4: Display cleaned columns
    print("Cleaned Columns:", df.columns)
    
    # Step 5: Define a temporary path to save the cleaned data
    temp_path = f"dbfs:/mnt/{mount_name}/temp_cleaned_{table_name}"

    # Step 6: Write the cleaned data to a temporary path in CSV format
    df.write.option("header", "true").csv(temp_path, mode="overwrite")

    print(f"Cleaned data written to {temp_path}")

for table in Tables:
    clean_and_save_csv('globalmart_data', table)

# COMMAND ----------


# creating empty tables in the bronze layer for each csv file
for table in Tables:
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS amrita_haldar_databricks_npmentorskool_onmicrosoft_com.bronze_ecom.{table} 
    """
    spark.sql(create_table_query)
    
    # Check if the file exists before copying data
    file_path = f"dbfs:/mnt/globalmart_data/temp_cleaned_{table}/"
    if dbutils.fs.ls(file_path):
        # copying data from csv files to the bronze tables    
        copy_into_query = f"""
        COPY INTO amrita_haldar_databricks_npmentorskool_onmicrosoft_com.bronze_ecom.{table}
        FROM '{file_path}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS(
            "header" = "true", 
            "inferSchema" = "true", 
            "mergeSchema" = "true", 
            "timestampFormat" = "dd-MM-yyyy HH.mm", 
            "quote" = '"'
        )
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
        spark.sql(copy_into_query)
    else:
        print(f"File {file_path} does not exist.")

# COMMAND ----------

# List all tables in the 'bronze' database and store the result in a DataFrame
tables_df = spark.sql("SHOW TABLES IN amrita_haldar_databricks_npmentorskool_onmicrosoft_com.bronze_ecom")
display(tables_df)  

# Loop through each row in the tables DataFrame to get table names
for row in tables_df.collect():
    table_name = row['tableName'] 
    df_name = f"{table_name}_df"
    globals()[df_name] = spark.table(f"bronze.{table_name}")

# COMMAND ----------

file_path = "dbfs:/mnt/globalmart_data/addresses.csv"

df_spark = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True,
    quote='"',   
    escape='"',
    multiLine=True,
    mode="DROPMALFORMED"
)

# COMMAND ----------

display(df_spark)

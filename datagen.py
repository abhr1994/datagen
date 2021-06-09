# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Initialization of Widgets

# COMMAND ----------

targets = ["CSV","JSON","Parquet","Delta","BigQuery","SQL Server"]
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("Target_Type","CSV", targets,"Target_Type")
dbutils.widgets.text("DBFS_Path", "dbfs://", "Target Path for Files")

def validate_required_argument_and_return_value(name):
  value = getArgument(name)
  if len(value) < 1:
    dbutils.notebook.exit("'{}' argument value is required.".format(name))
  return value

# COMMAND ----------

target_type = validate_required_argument_and_return_value("Target_Type")
dbfs_path = validate_required_argument_and_return_value("DBFS_Path")

# COMMAND ----------

if target_type in ["BigQuery","SQL Server"]:
  print("Please enter your {} credentials".format(target_type))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Fill these configurations needed as required

# COMMAND ----------

#Fill this if your target is Big Query

bq_project = "gcp-cs-shared-resources"
bq_temporaryGcsBucket = "cs-shared-resources-bucket"
bq_table = "gcp-cs-shared-resources.kafka_demo.flight_locations"
'''
Add these 2 values in your cluster environmental variables (Example Values Shown)
GOOGLE_CLOUD_PROJECT=<project_id>
GOOGLE_APPLICATION_CREDENTIALS=<path_to_json_credentials_file>
'''

#Fill this if your target is SQL Server

sql_server_name = "jdbc:sqlserver://{SERVER_ADDR}"
sql_database_name = "database_name"
sql_url = server_name + ";" + "databaseName=" + database_name + ";"
sql_table_name = "table_name"
sql_username = "username"
sql_password = "password123!#" # Please specify password here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Below code connects to snowflake and lists all the TPCDS sample tables available

# COMMAND ----------

user = dbutils.secrets.get("datagen-secret", "snowflake_user")
password = dbutils.secrets.get("datagen-secret", "snowflake_pass")
snowflake_url = dbutils.secrets.get("datagen-secret", "snowflake_url")
options = {
  "sfUrl": snowflake_url,
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema": "TPCH_SF001",
  "sfWarehouse": "COMPUTE_WH"
}
df_tables = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query",  "select t.table_catalog,t.table_schema,t.table_name,t.row_count,abs(t.bytes/(1024*1024*1024)) as size_in_GB from information_schema.tables t where t.table_catalog='SNOWFLAKE_SAMPLE_DATA' and t.table_type = 'BASE TABLE' order by size_in_GB desc") \
  .load()
display(df_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Choose the table you want to read from snowflake and write to the target

# COMMAND ----------

user = dbutils.secrets.get("datagen-secret", "snowflake_user")
password = dbutils.secrets.get("datagen-secret", "snowflake_pass")
snowflake_url = dbutils.secrets.get("datagen-secret", "snowflake_url")
# snowflake connection options
options = {
  "sfUrl": snowflake_url,
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema": "TPCH_SF001",
  "dbtable": "CUSTOMER",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .load()

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Below section writes the dataset to the requested target

# COMMAND ----------

def write_to_target(dataframe,path,file_format):
  path = path+"/csv"
  print("Writing to ",path)
  if file_format == "csv":
    dataframe.repartition(12).write.format(file_format).mode("overwrite").option("header", "true").option("delimiter", ",").save(path)
  else:
    dataframe.repartition(12).write.format(file_format).mode("overwrite").save(path)

# COMMAND ----------

def write_to_rdbms(df,format_name,url,table,username,password):
  try:
    df.repartition(12).write \
      .format(format_name) \
      .mode("overwrite") \
      .option("url", url) \
      .option("dbtable", table) \
      .option("user", username) \
      .option("password", password) \
      .save()
  except ValueError as error :
      print("Connector write failed", error)

# COMMAND ----------

if target_type == "BigQuery":
  df.repartition(12).write.format("bigquery").mode("overwrite").option("project", bq_project).option("temporaryGcsBucket", bq_temporaryGcsBucket).option("table", bq_table).save()
  print(df.count())
elif target_type in ["CSV","JSON","Parquet","Delta"]:
  write_to_target(df,dbfs_path,target_type.lower())
  print(df.count())
elif target_type == "SQL Server":
  write_to_rdbms(df,"com.microsoft.sqlserver.jdbc.spark",sql_url,sql_table_name,sql_username,sql_password)
else:
  pass

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

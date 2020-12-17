# Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

with open("/dbfs/databricks-datasets/README.md") as f:
    x = ''.join(f.readlines())

print(x)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/samples/lending_club/parquet

# COMMAND ----------

with open("/dbfs/databricks-datasets/samples/lending_club/readme.md", "r") as f:
  for line in f:
    print(line)

# COMMAND ----------

# MAGIC %md # Load Data

# COMMAND ----------

lspq_path = "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
data = (spark
        .read
        .parquet(lspq_path)
       )

# COMMAND ----------

# Select only the columns needed
loan_stats = data.select(
  "addr_state",
  "loan_status"
)


# COMMAND ----------


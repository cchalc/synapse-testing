# Databricks notebook source
# MAGIC %scala
# MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC 
# MAGIC //*******************************************
# MAGIC // GET USERNAME AND USERHOME
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get the user's name
# MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC 
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC // Set the user's name and home directory
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
# MAGIC 
# MAGIC // Set the user's notebook name and path
# MAGIC val notebook_path = dbutils.notebook.getContext.notebookPath.mkString("")
# MAGIC val notebook_name = dbutils.notebook.getContext.notebookPath.mkString("").drop(44)
# MAGIC 
# MAGIC displayHTML(s"""
# MAGIC Initialized user variables...
# MAGIC   <br>
# MAGIC   You may continue working through the <b>$notebook_name</b> notebook!
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC username = spark.conf.get("com.databricks.training.username", "unknown-username")
# MAGIC userhome = spark.conf.get("com.databricks.training.userhome", "unknown-userhome")

# COMMAND ----------


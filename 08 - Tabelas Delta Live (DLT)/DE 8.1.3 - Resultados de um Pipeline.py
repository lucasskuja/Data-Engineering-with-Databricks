# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explorando os Resultados de um Pipeline DLT
# MAGIC
# MAGIC Este notebook explora os resultados de execução de um pipeline Delta Live Tables (DLT).
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O diretório **`system`** captura eventos associados ao pipeline.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Esses logs de eventos são armazenados como uma tabela Delta. Vamos consultar essa tabela.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos visualizar o conteúdo do diretório *tables*.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos consultar a tabela gold.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
# MAGIC

# COMMAND ----------

DA.cleanup()

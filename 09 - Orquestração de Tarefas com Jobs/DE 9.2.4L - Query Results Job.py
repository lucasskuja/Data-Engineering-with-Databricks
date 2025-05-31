# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-9.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Explorando os Resultados de um Pipeline DLT
# MAGIC
# MAGIC Execute a célula a seguir para listar os arquivos no seu diretório de armazenamento:
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O diretório **system** captura eventos associados ao pipeline.
# MAGIC
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Esses logs de eventos são armazenados como uma tabela Delta.
# MAGIC
# MAGIC Vamos consultar essa tabela.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.working_dir}/storage/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos visualizar o conteúdo do diretório *tables*.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos consultar a tabela gold.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

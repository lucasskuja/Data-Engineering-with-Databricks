# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Usando Auto Loader e Structured Streaming com Spark SQL
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você será capaz de:
# MAGIC * Ingerir dados usando o Auto Loader
# MAGIC * Agregar dados de streaming
# MAGIC * Enviar dados de streaming para uma tabela Delta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuração
# MAGIC Execute o seguinte script para configurar as variáveis necessárias e limpar execuções anteriores deste notebook. Reexecutar esta célula permitirá que você reinicie o laboratório do zero.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.3L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configurar Leitura de Streaming
# MAGIC
# MAGIC Este laboratório usa um conjunto de dados CSV relacionados a clientes, localizado no DBFS em */databricks-datasets/retail-org/customers/*.
# MAGIC
# MAGIC Leia esses dados usando o <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> com inferência de esquema (utilize **`customers_checkpoint_path`** para armazenar as informações de esquema). Crie uma visualização temporária de streaming chamada **`customers_raw_temp`**.
# MAGIC

# COMMAND ----------

from pyspark.sql import Row

customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", customers_checkpoint_path)
    .load("/databricks-datasets/retail-org/customers/")
    .createOrReplaceTempView("customers_raw_temp")
)

# COMMAND ----------


assert (
    Row(tableName="customers_raw_temp", isTemporary=True)
    in spark.sql("show tables").select("tableName", "isTemporary").collect()
), "Tabela não encontrada ou não é temporária"
assert spark.table("customers_raw_temp").dtypes == [
    ("customer_id", "string"),
    ("tax_id", "string"),
    ("tax_code", "string"),
    ("customer_name", "string"),
    ("state", "string"),
    ("city", "string"),
    ("postcode", "string"),
    ("street", "string"),
    ("number", "string"),
    ("unit", "string"),
    ("region", "string"),
    ("district", "string"),
    ("lon", "string"),
    ("lat", "string"),
    ("ship_to_address", "string"),
    ("valid_from", "string"),
    ("valid_to", "string"),
    ("units_purchased", "string"),
    ("loyalty_segment", "string"),
    ("_rescued_data", "string"),
], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definir uma agregação de streaming
# MAGIC
# MAGIC Usando a sintaxe CTAS, defina uma nova visualização de streaming chamada **`customer_count_by_state_temp`** que conta o número de clientes por **`state`**, em um campo chamado **`customer_count`**.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC --SELECT
# MAGIC --  <FILL-IN>

# COMMAND ----------

assert (
    Row(tableName="customer_count_by_state_temp", isTemporary=True)
    in spark.sql("show tables").select("tableName", "isTemporary").collect()
), "Table not present or not temporary"
assert spark.table("customer_count_by_state_temp").dtypes == [
    ("state", "string"),
    ("customer_count", "bigint"),
], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definir uma agregação de streaming
# MAGIC
# MAGIC Usando a sintaxe CTAS, defina uma nova visualização de streaming chamada **`customer_count_by_state_temp`** que conta o número de clientes por **`state`**, em um campo chamado **`customer_count`**.
# MAGIC

# COMMAND ----------

# TODO
# customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"
#
# query = (spark
#   <FILL-IN>

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

assert (
    Row(tableName="customer_count_by_state", isTemporary=False)
    in spark.sql("show tables").select("tableName", "isTemporary").collect()
), "Table not present or not temporary"
assert spark.table("customer_count_by_state").dtypes == [
    ("state", "string"),
    ("customer_count", "bigint"),
], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Consultar os resultados
# MAGIC
# MAGIC Consulte a tabela **`customer_count_by_state`** (esta não será uma consulta de streaming). Plote os resultados como um gráfico de barras e também utilizando o gráfico de mapa.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Finalizando
# MAGIC
# MAGIC Execute a célula a seguir para remover o banco de dados e todos os dados associados a este laboratório.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ao concluir este laboratório, você deve estar confortável com:
# MAGIC * Usar PySpark para configurar o Auto Loader para ingestão incremental de dados
# MAGIC * Usar Spark SQL para agregar dados de streaming
# MAGIC * Transmitir dados para uma tabela Delta
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

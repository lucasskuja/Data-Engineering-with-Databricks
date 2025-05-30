# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Propagando Atualizações Incrementais com Structured Streaming e Delta Lake
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC * Aplicar seu conhecimento de Structured Streaming e Auto Loader para implementar uma arquitetura multi-hop simples
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuração
# MAGIC Execute o seguinte script para configurar as variáveis necessárias e limpar execuções anteriores deste notebook. Observe que reexecutar esta célula permitirá que você reinicie o laboratório do zero.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingestão de Dados
# MAGIC
# MAGIC Este laboratório utiliza um conjunto de dados de clientes no formato CSV disponíveis no DBFS, localizados em */databricks-datasets/retail-org/customers/*.
# MAGIC
# MAGIC Leia esses dados utilizando o Auto Loader com inferência de esquema (use **`customers_checkpoint_path`** para armazenar as informações de esquema). Transmita os dados brutos para uma tabela Delta chamada **`bronze`**.
# MAGIC

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

#query = (spark
#  .readStream
#  <FILL-IN>
#  .load("/databricks-datasets/retail-org/customers/")
#  .writeStream
#  <FILL-IN>
#  .table("bronze")
#)

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para verificar seu trabalho.
# MAGIC

# COMMAND ----------

assert spark.table("bronze"), "Table named `bronze` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("bronze").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos criar uma visualização temporária de streaming na tabela bronze para podermos realizar transformações usando SQL.
# MAGIC

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpeza e Enriquecimento dos Dados
# MAGIC
# MAGIC Utilizando a sintaxe CTAS (Create Table As Select), defina uma nova visualização de streaming chamada **`bronze_enhanced_temp`** que execute as seguintes ações:
# MAGIC * Ignora registros com valor nulo em **`postcode`** (substitui por zero)
# MAGIC * Insere uma coluna chamada **`receipt_time`** contendo a timestamp atual
# MAGIC * Insere uma coluna chamada **`source_file`** contendo o nome do arquivo de entrada
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC --SELECT
# MAGIC --  <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para verificar seu trabalho.
# MAGIC

# COMMAND ----------

assert spark.table("bronze_enhanced_temp"), "Table named `bronze_enhanced_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze_enhanced_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("bronze_enhanced_temp").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("bronze_enhanced_temp").isStreaming, "Not a streaming table"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tabela Silver
# MAGIC
# MAGIC Transmita os dados da visualização **`bronze_enhanced_temp`** para uma tabela chamada **`silver`**.
# MAGIC

# COMMAND ----------

# TODO
#silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"
#
#query = (spark.table("bronze_enhanced_temp")
#  <FILL-IN>
#  .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para verificar seu trabalho.
# MAGIC

# COMMAND ----------

assert spark.table("silver"), "Table named `silver` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'silver'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("silver").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("silver").filter("postcode <= 0").count() == 0, "Null postcodes present"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos criar uma visualização temporária de streaming na tabela silver para que possamos realizar análises de negócio usando SQL.
# MAGIC

# COMMAND ----------

#(spark
#  .readStream
#  .table("silver")
#  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tabelas Gold
# MAGIC
# MAGIC Utilizando a sintaxe CTAS, defina uma nova visualização de streaming chamada **`customer_count_temp`** que conte os clientes por estado.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC --CREATE OR REPLACE TEMPORARY VIEW customer_count_temp AS
# MAGIC --SELECT 
# MAGIC --<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para verificar seu trabalho.
# MAGIC

# COMMAND ----------

assert spark.table("customer_count_temp"), "Table named `customer_count_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'customer_count_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("customer_count_temp").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Por fim, transmita os dados da visualização **`customer_count_temp`** para uma tabela Delta chamada **`gold_customer_count_by_state`**.
# MAGIC

# COMMAND ----------

# TODO
#customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"
#
#query = (spark
#  .table("customer_count_temp")
#  .writeStream
#  <FILL-IN>
#  .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para verificar seu trabalho.
# MAGIC

# COMMAND ----------

assert spark.table("gold_customer_count_by_state"), "Table named `gold_customer_count_by_state` does not exist"
assert spark.sql(f"show tables").filter(f"tableName == 'gold_customer_count_by_state'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("gold_customer_count_by_state").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"
assert spark.table("gold_customer_count_by_state").count() == 51, "Incorrect number of rows" 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Consultando os Resultados
# MAGIC
# MAGIC Consulte a tabela **`gold_customer_count_by_state`** (esta não será uma consulta de streaming). Plote os resultados como um gráfico de barras e também utilizando o gráfico de mapa.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Encerramento
# MAGIC
# MAGIC Execute a célula abaixo para remover o banco de dados e todos os dados associados a este laboratório.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ao concluir este laboratório, você agora deve se sentir confortável em:
# MAGIC * Usar PySpark para configurar o Auto Loader para ingestão incremental de dados
# MAGIC * Usar Spark SQL para agregar dados em streaming
# MAGIC * Transmitir dados para uma tabela Delta
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

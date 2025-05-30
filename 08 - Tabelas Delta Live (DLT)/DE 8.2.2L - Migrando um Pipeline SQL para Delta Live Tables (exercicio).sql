-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Laboratório: Migrando um Pipeline SQL para Delta Live Tables
-- MAGIC
-- MAGIC Este notebook será completado por você para implementar um pipeline DLT usando SQL.
-- MAGIC
-- MAGIC **Não é** destinado a ser executado interativamente, mas sim para ser implantado como pipeline após você concluir suas alterações.
-- MAGIC
-- MAGIC Para ajudar na conclusão deste notebook, consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">documentação da sintaxe DLT</a>.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Tabela Bronze
-- MAGIC
-- MAGIC Declare uma tabela bronze que ingere dados JSON incrementalmente (usando Auto Loader) da fonte simulada na nuvem. O local da fonte já está fornecido como argumento; usar esse valor é ilustrado na célula abaixo.
-- MAGIC
-- MAGIC Como fizemos anteriormente, inclua duas colunas adicionais:
-- MAGIC * **`receipt_time`** que registra um timestamp conforme retornado por **`current_timestamp()`**
-- MAGIC * **`source_file`** que é obtido por **`input_file_name()`**
-- MAGIC

-- COMMAND ----------

-- TODO
--CREATE <FILL-IN>
--AS SELECT <FILL-IN>
--  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Arquivo PII
-- MAGIC
-- MAGIC Usando uma sintaxe CTAS similar, crie uma tabela **live** para os dados CSV encontrados em */mnt/training/healthcare/patient*.
-- MAGIC
-- MAGIC Para configurar corretamente o Auto Loader para essa fonte, você precisará especificar os seguintes parâmetros adicionais:
-- MAGIC
-- MAGIC | opção | valor |
-- MAGIC | --- | --- |
-- MAGIC | **`header`** | **`true`** |
-- MAGIC | **`cloudFiles.inferColumnTypes`** | **`true`** |
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Configurações do Auto Loader para CSV podem ser encontradas <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">aqui</a>.
-- MAGIC

-- COMMAND ----------

-- TODO
--CREATE <FILL-IN> pii
--AS SELECT *
--  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map(<FILL-IN>))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Tabelas Silver
-- MAGIC
-- MAGIC Nossa tabela silver, **`recordings_parsed`**, conterá os seguintes campos:
-- MAGIC
-- MAGIC | Campo | Tipo |
-- MAGIC | --- | --- |
-- MAGIC | **`device_id`** | **`INTEGER`** |
-- MAGIC | **`mrn`** | **`LONG`** |
-- MAGIC | **`heartrate`** | **`DOUBLE`** |
-- MAGIC | **`time`** | **`TIMESTAMP`** (exemplo fornecido abaixo) |
-- MAGIC | **`name`** | **`STRING`** |
-- MAGIC
-- MAGIC Esta consulta também deve enriquecer os dados através de um join interno com a tabela **`pii`** na coluna comum **`mrn`** para obter o nome.
-- MAGIC
-- MAGIC Implemente controle de qualidade aplicando uma restrição que descarte registros com **`heartrate`** inválido (ou seja, menor ou igual a zero).
-- MAGIC

-- COMMAND ----------

-- TODO
--CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
--  (<FILL-IN add a constraint to drop records when heartrate ! > 0>)
--AS SELECT 
--  CAST(<FILL-IN>) device_id, 
--  <FILL-IN mrn>, 
--  <FILL-IN heartrate>, 
--  CAST(FROM_UNIXTIME(DOUBLE(time), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time 
--  FROM STREAM(live.recordings_bronze)
--  <FILL-IN specify source and perform inner join with pii on mrn>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Tabela Gold
-- MAGIC
-- MAGIC Crie uma tabela gold, **`daily_patient_avg`**, que agrega **`recordings_enriched`** por **`mrn`**, **`name`** e **`date`** e entregue as seguintes colunas:
-- MAGIC
-- MAGIC | Nome da coluna | Valor |
-- MAGIC | --- | --- |
-- MAGIC | **`mrn`** | **`mrn`** da fonte |
-- MAGIC | **`name`** | **`name`** da fonte |
-- MAGIC | **`avg_heartrate`** | Média do **`heartrate`** do grupo |
-- MAGIC | **`date`** | Data extraída de **`time`** |
-- MAGIC

-- COMMAND ----------

-- TODO
--CREATE <FILL-IN> daily_patient_avg
--  COMMENT <FILL-IN insert comment here>
--AS SELECT <FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
-- MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
-- MAGIC

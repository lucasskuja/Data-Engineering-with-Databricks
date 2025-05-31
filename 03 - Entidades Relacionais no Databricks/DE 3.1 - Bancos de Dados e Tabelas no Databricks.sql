-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Bancos de Dados e Tabelas no Databricks
-- MAGIC
-- MAGIC Nesta demonstração, você criará e explorará bancos de dados e tabelas.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Usar o DDL do Spark SQL para definir bancos de dados e tabelas
-- MAGIC * Descrever como a palavra-chave **`LOCATION`** impacta o diretório padrão de armazenamento
-- MAGIC
-- MAGIC ## Recursos
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Bancos de Dados e Tabelas - Documentação Databricks</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Tabelas Gerenciadas e Não Gerenciadas</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Criando uma Tabela pela Interface</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Criar uma Tabela Local</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Salvando em Tabelas Persistentes</a>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Configuração da Lição  
-- MAGIC O script a seguir limpa execuções anteriores desta demonstração e configura algumas variáveis Hive que serão usadas em nossas consultas SQL.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Uso de Variáveis Hive  
-- MAGIC Embora não seja um padrão geralmente recomendado no Spark SQL, este notebook usará algumas variáveis Hive para substituir valores de string derivados do email da conta do usuário atual.
-- MAGIC
-- MAGIC A célula seguinte demonstra este padrão.

-- COMMAND ----------

SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como você pode estar trabalhando em um workspace compartilhado, este curso usa variáveis derivadas do seu nome de usuário para que os bancos de dados não entrem em conflito com outros usuários. Novamente, considere este uso de variáveis Hive como um truque para nosso ambiente de lição, e não como uma boa prática para desenvolvimento.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Bancos de Dados  
-- MAGIC Vamos começar criando dois bancos de dados:  
-- MAGIC - Um sem especificar **`LOCATION`**  
-- MAGIC - Um com **`LOCATION`** especificado  
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que a localização do primeiro banco de dados está no local padrão em **`dbfs:/user/hive/warehouse/`** e que o diretório do banco de dados é o nome do banco com a extensão **`.db`**
-- MAGIC

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que a localização do segundo banco de dados está no diretório especificado após a palavra-chave **`LOCATION`**.
-- MAGIC

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Vamos criar uma tabela no banco de dados com localização padrão e inserir dados.
-- MAGIC
-- MAGIC Note que o esquema deve ser fornecido porque não há dados de onde inferir o esquema.
-- MAGIC

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Podemos olhar a descrição estendida da tabela para encontrar a localização (você precisará rolar para baixo nos resultados).
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Por padrão, tabelas gerenciadas em um banco de dados sem localização especificada serão criadas no diretório **`dbfs:/user/hive/warehouse/<nome_do_banco>.db/`**.
-- MAGIC
-- MAGIC Podemos ver que, como esperado, os dados e metadados para nossa tabela Delta são armazenados nesse local.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hive_root =  f"dbfs:/user/hive/warehouse"
-- MAGIC db_name =    f"{DA.db_name}_default_location.db"
-- MAGIC table_name = f"managed_table_in_db_with_default_location"
-- MAGIC
-- MAGIC tbl_location = f"{hive_root}/{db_name}/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Apague a tabela.
-- MAGIC

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que o diretório da tabela e seus arquivos de log e dados são deletados. Apenas o diretório do banco de dados permanece.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC db_location = f"{hive_root}/{db_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Agora vamos criar uma tabela no banco de dados com localização personalizada e inserir dados.
-- MAGIC
-- MAGIC Note que o esquema deve ser fornecido porque não há dados dos quais se possa inferir o esquema.
-- MAGIC

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Novamente, vamos olhar a descrição para encontrar a localização da tabela.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como esperado, essa tabela gerenciada é criada no caminho especificado com a palavra-chave **`LOCATION`** durante a criação do banco de dados. Portanto, os dados e metadados da tabela são armazenados nesse diretório.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC table_name = f"managed_table_in_db_with_custom_location"
-- MAGIC tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Vamos apagar a tabela.
-- MAGIC

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que a pasta da tabela, o arquivo de log e o arquivo de dados são deletados.
-- MAGIC
-- MAGIC Apenas a localização do banco de dados permanece.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Tabelas  
-- MAGIC Vamos criar uma tabela externa (não gerenciada) a partir de dados de exemplo.
-- MAGIC
-- MAGIC Os dados que vamos usar estão no formato CSV. Queremos criar uma tabela Delta com uma **`LOCATION`** fornecida no diretório da nossa escolha.
-- MAGIC

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Vamos observar a localização dos dados da tabela no diretório de trabalho desta lição.
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Agora, apagamos a tabela.
-- MAGIC

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A definição da tabela não existe mais no metastore, mas os dados subjacentes permanecem intactos.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limpeza  
-- MAGIC Apague ambos os bancos de dados.
-- MAGIC

-- COMMAND ----------

DROP DATABASE ${da.db_name}_default_location CASCADE;
DROP DATABASE ${da.db_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para deletar as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
-- MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>

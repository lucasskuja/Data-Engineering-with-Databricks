-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Databases, Tables, and Views Lab
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC Ao final deste laboratório, você deverá ser capaz de:
-- MAGIC - Criar e explorar interações entre várias entidades relacionais, incluindo:
-- MAGIC   - Bancos de dados
-- MAGIC   - Tabelas (gerenciadas e externas)
-- MAGIC   - Views (views, views temporárias e views globais temporárias)
-- MAGIC
-- MAGIC **Recursos**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Bancos de Dados e Tabelas - Documentação do Databricks</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Tabelas Gerenciadas e Não Gerenciadas</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Criando uma Tabela pela Interface</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Criar uma Tabela Local</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Salvando em Tabelas Persistentes</a>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### Getting Started
-- MAGIC
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.3L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Visão Geral dos Dados
-- MAGIC
-- MAGIC Os dados incluem múltiplas entradas de uma seleção de estações meteorológicas, incluindo temperaturas médias registradas em Fahrenheit ou Celsius. O esquema da tabela é o seguinte:
-- MAGIC
-- MAGIC |Nome da Coluna| Tipo de Dado | Descrição              |
-- MAGIC |--------------|--------------|------------------------|
-- MAGIC |NAME          | string       | Nome da estação        |
-- MAGIC |STATION       | string       | ID único               |
-- MAGIC |LATITUDE      | float        | Latitude               |
-- MAGIC |LONGITUDE     | float        | Longitude              |
-- MAGIC |ELEVATION     | float        | Elevação               |
-- MAGIC |DATE          | date         | Data (AAAA-MM-DD)      |
-- MAGIC |UNIT          | string       | Unidade de temperatura |
-- MAGIC |TAVG          | float        | Temperatura média      |
-- MAGIC
-- MAGIC Esses dados estão armazenados no formato Parquet; visualize os dados com a consulta abaixo.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar um Banco de Dados
-- MAGIC
-- MAGIC Crie um banco de dados no local padrão usando a variável **`da.db_name`** definida no script de configuração.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 1, "Database not present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Mude para o Seu Novo Banco de Dados
-- MAGIC
-- MAGIC Use o comando **`USE`** para utilizar o banco de dados que você acabou de criar.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.db_name, "Not using the correct database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar uma Tabela Gerenciada
-- MAGIC
-- MAGIC Use uma instrução CTAS para criar uma tabela gerenciada chamada **`weather_managed`**.

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar uma Tabela Externa
-- MAGIC
-- MAGIC Lembre-se de que uma tabela externa difere de uma tabela gerenciada pela especificação de um local (LOCATION). Crie uma tabela externa chamada **`weather_external`** abaixo.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_external"), "Table named `weather_external` does not exist"
-- MAGIC assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Examinar os Detalhes da Tabela
-- MAGIC
-- MAGIC Use o comando SQL **`DESCRIBE EXTENDED nome_da_tabela`** para examinar as duas tabelas de clima.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute o código auxiliar a seguir para extrair e comparar os locais das tabelas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC
-- MAGIC     {managedTablePath}
-- MAGIC
-- MAGIC The weather_external table is saved at:
-- MAGIC
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Liste o conteúdo desses diretórios para confirmar que os dados existem em ambos os locais.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Verificar o Conteúdo dos Diretórios Após Excluir o Banco de Dados e Todas as Tabelas
-- MAGIC
-- MAGIC A palavra-chave **`CASCADE`** será usada para isso.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 0, "Database present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Com o banco de dados excluído, os arquivos também terão sido deletados.
-- MAGIC
-- MAGIC Descomente e execute a célula a seguir, que lançará uma **`FileNotFoundException`** como confirmação.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # files = dbutils.fs.ls(managedTablePath)
-- MAGIC # display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(DA.paths.working_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Isso destaca as principais diferenças entre tabelas gerenciadas e externas.**  
-- MAGIC Por padrão, os arquivos associados às tabelas gerenciadas serão armazenados neste local no armazenamento raiz do DBFS vinculado ao workspace, e serão excluídos quando a tabela for removida.
-- MAGIC
-- MAGIC Os arquivos das tabelas externas serão mantidos no local especificado na criação da tabela, impedindo que os usuários excluam inadvertidamente os arquivos subjacentes.  
-- MAGIC **Tabelas externas podem ser facilmente migradas para outros bancos de dados ou renomeadas, mas essas operações com tabelas gerenciadas exigirão a reescrita de TODOS os arquivos subjacentes.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar um Banco de Dados com um Caminho Específico
-- MAGIC
-- MAGIC Supondo que você tenha excluído seu banco de dados na etapa anterior, você pode usar o mesmo nome de **banco de dados**.
-- MAGIC

-- COMMAND ----------

CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Recrie sua tabela **weather_managed** neste novo banco de dados e imprima a localização dessa tabela.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Embora aqui estejamos usando o diretório **working_dir** criado na raiz do DBFS, _qualquer_ armazenamento de objetos pode ser usado como diretório do banco de dados.  
-- MAGIC **Definir diretórios de banco de dados para grupos de usuários pode reduzir muito as chances de exfiltração acidental de dados**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views e seu Escopo
-- MAGIC
-- MAGIC Usando a cláusula **AS** fornecida, registre:  
-- MAGIC - uma view chamada **celsius**  
-- MAGIC - uma view temporária chamada **celsius_temp**  
-- MAGIC - uma view temporária global chamada **celsius_global**
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius"), "Table named `celsius` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora crie uma view temporária.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora registre uma view temporária global.
-- MAGIC

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula abaixo para verificar seu trabalho.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As views serão exibidas junto com as tabelas ao listar o catálogo.
-- MAGIC

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note o seguinte:  
-- MAGIC - A view está associada ao banco de dados atual. Essa view estará disponível para qualquer usuário que tenha acesso a esse banco e persistirá entre sessões.  
-- MAGIC - A view temporária não está associada a nenhum banco de dados. A view temporária é efêmera e só é acessível na SparkSession atual.  
-- MAGIC - A view temporária global não aparece no nosso catálogo. **Views temporárias globais sempre são registradas no banco de dados **`global_temp`**.**  
-- MAGIC O banco **`global_temp`** é efêmero, mas vinculado à vida útil do cluster; no entanto, só é acessível pelos notebooks conectados ao mesmo cluster onde foi criada.
-- MAGIC

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Embora nenhum job seja disparado ao definir essas views, um job é disparado _toda vez_ que uma consulta é executada contra a view.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpeza  
-- MAGIC Remova o banco de dados e todas as tabelas para limpar seu workspace.
-- MAGIC

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Resumo  
-- MAGIC
-- MAGIC Neste laboratório nós:  
-- MAGIC - Criamos e excluímos bancos de dados  
-- MAGIC - Exploramos o comportamento de tabelas gerenciadas e externas  
-- MAGIC - Aprendemos sobre o escopo das views
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
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
-- MAGIC

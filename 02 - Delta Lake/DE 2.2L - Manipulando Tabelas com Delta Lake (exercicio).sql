-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Manipulando Tabelas com Delta Lake
-- MAGIC
-- MAGIC Este notebook oferece uma revisão prática de algumas funcionalidades básicas do Delta Lake.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final deste laboratório, você deverá ser capaz de:  
-- MAGIC - Executar operações padrão para criar e manipular tabelas Delta Lake, incluindo:  
-- MAGIC   - **`CREATE TABLE`**  
-- MAGIC   - **`INSERT INTO`**  
-- MAGIC   - **`SELECT FROM`**  
-- MAGIC   - **`UPDATE`**  
-- MAGIC   - **`DELETE`**  
-- MAGIC   - **`MERGE`**  
-- MAGIC   - **`DROP TABLE`**  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Configuração  
-- MAGIC Execute o script a seguir para configurar as variáveis necessárias e limpar execuções anteriores deste notebook.  
-- MAGIC Observe que, ao reexecutar esta célula, você poderá reiniciar o laboratório do início.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.2L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar uma Tabela
-- MAGIC
-- MAGIC Neste notebook, vamos criar uma tabela para acompanhar nossa coleção de feijões.
-- MAGIC
-- MAGIC Use a célula abaixo para criar uma tabela gerenciada do Delta Lake chamada **`beans`**.
-- MAGIC
-- MAGIC Forneça o seguinte esquema:
-- MAGIC
-- MAGIC | Nome do Campo | Tipo do Campo |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Usaremos Python para executar verificações ocasionalmente ao longo do laboratório.  
-- MAGIC A célula a seguir retornará um erro com uma mensagem indicando o que precisa ser alterado, caso você não tenha seguido as instruções corretamente.  
-- MAGIC A ausência de saída na execução da célula significa que você concluiu esta etapa com sucesso.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Inserir Dados
-- MAGIC
-- MAGIC Execute a célula a seguir para inserir três linhas na tabela.
-- MAGIC

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Revise manualmente o conteúdo da tabela para garantir que os dados foram gravados conforme o esperado.
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Insira os registros adicionais fornecidos abaixo. Certifique-se de executar isso como uma única transação.
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que os dados estão no estado correto.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Atualizar Registros
-- MAGIC
-- MAGIC Um amigo está revisando seu inventário de feijões. Após muita discussão, vocês concordam que jelly beans são deliciosos.
-- MAGIC
-- MAGIC Execute a célula a seguir para atualizar esse registro.
-- MAGIC

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Você percebe que inseriu incorretamente o peso dos seus feijões pinto.
-- MAGIC
-- MAGIC Atualize a coluna **`grams`** desse registro para o peso correto de 1500.
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que isso foi concluído corretamente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Excluir Registros
-- MAGIC
-- MAGIC Você decidiu que deseja acompanhar apenas os feijões deliciosos.
-- MAGIC
-- MAGIC Execute uma consulta para remover todos os feijões que não são deliciosos.
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula a seguir para confirmar que esta operação foi bem-sucedida.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Usando Merge para Inserir ou Atualizar Registros (Upsert)
-- MAGIC
-- MAGIC Seu amigo lhe deu alguns feijões novos. A célula abaixo registra esses dados como uma view temporária.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Na célula abaixo, use a view acima para escrever uma instrução merge que atualize e insira novos registros na sua tabela **`beans`** como uma única transação.
-- MAGIC
-- MAGIC Certifique-se de que sua lógica:  
-- MAGIC - Combine os feijões pelo nome **e** pela cor  
-- MAGIC - Atualize os feijões existentes somando o novo peso ao peso atual  
-- MAGIC - Insira novos feijões apenas se forem deliciosos  
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar seu trabalho.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
-- MAGIC assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC metrics = last_tx.select("operationMetrics").first()[0]
-- MAGIC assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Removendo Tabelas
-- MAGIC
-- MAGIC Ao trabalhar com tabelas gerenciadas do Delta Lake, remover uma tabela resulta na exclusão permanente do acesso à tabela e a todos os arquivos de dados subjacentes.
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Mais adiante no curso, aprenderemos sobre tabelas externas, que tratam as tabelas Delta Lake como uma coleção de arquivos e possuem garantias de persistência diferentes.
-- MAGIC
-- MAGIC Na célula abaixo, escreva uma consulta para remover a tabela **`beans`**.
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que sua tabela não existe mais.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Concluindo
-- MAGIC
-- MAGIC Ao completar este laboratório, você deverá se sentir confortável em:  
-- MAGIC * Executar comandos padrão para criação e manipulação de dados em tabelas Delta Lake
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula a seguir para deletar as tabelas e arquivos associados a esta lição.
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

-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Escrevendo em Tabelas Delta  
-- MAGIC As tabelas Delta Lake fornecem atualizações ACID para tabelas armazenadas em arquivos de dados na nuvem.
-- MAGIC
-- MAGIC Neste notebook, vamos explorar a sintaxe SQL para processar atualizações com Delta Lake. Embora muitas operações sejam SQL padrão, existem pequenas variações para acomodar a execução no Spark e Delta Lake.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final desta lição, você deverá ser capaz de:  
-- MAGIC - Sobrescrever tabelas usando **`INSERT OVERWRITE`**  
-- MAGIC - Acrescentar linhas usando **`INSERT INTO`**  
-- MAGIC - Acrescentar, atualizar e deletar dados usando **`MERGE INTO`**  
-- MAGIC - Ingerir dados incrementalmente usando **`COPY INTO`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Setup  
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para o restante deste notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Sobrescrita Completa  
-- MAGIC Podemos usar sobrescritas para substituir atomicamente todos os dados de uma tabela. Existem múltiplos benefícios ao sobrescrever tabelas em vez de deletar e recriar:  
-- MAGIC - Sobrescrever uma tabela é muito mais rápido porque não precisa listar o diretório recursivamente nem deletar arquivos.  
-- MAGIC - A versão anterior da tabela ainda existe; é possível recuperar dados antigos usando Time Travel.  
-- MAGIC - É uma operação atômica. Consultas concorrentes ainda podem ler a tabela enquanto você sobrescreve.  
-- MAGIC - Devido às garantias ACID, se a sobrescrita falhar, a tabela permanece no estado anterior.
-- MAGIC
-- MAGIC O Spark SQL oferece duas formas simples para fazer sobrescrita completa.
-- MAGIC
-- MAGIC Alguns estudantes podem ter percebido que a lição anterior sobre CTAS usou, na verdade, CRAS para evitar erros se uma célula fosse executada múltiplas vezes.
-- MAGIC
-- MAGIC **`CREATE OR REPLACE TABLE`** (CRAS) substitui totalmente o conteúdo da tabela cada vez que executada.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/events-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Visualizando o histórico da tabela mostra que uma versão anterior foi substituída.

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`** fornece um resultado quase idêntico: os dados da tabela de destino são substituídos pelos dados da consulta.
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**:  
-- MAGIC - Só pode sobrescrever uma tabela existente, não criar uma nova como o CRAS  
-- MAGIC - Só pode sobrescrever com novos registros que correspondam ao esquema atual da tabela — portanto é uma técnica “mais segura” para sobrescrever sem prejudicar consumidores downstream  
-- MAGIC - Pode sobrescrever partições individuais
-- MAGIC

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que as métricas exibidas são diferentes do CRAS; o histórico da tabela registra a operação de forma distinta.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Uma diferença principal está em como o Delta Lake impõe o esquema na escrita.
-- MAGIC
-- MAGIC Enquanto CRAS permite redefinir totalmente o conteúdo da tabela, **`INSERT OVERWRITE`** falhará se tentarmos mudar o esquema (a menos que configuremos opções adicionais).
-- MAGIC
-- MAGIC Descomente e execute a célula abaixo para gerar a mensagem de erro esperada.
-- MAGIC

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/raw/sales-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Acrescentar Linhas  
-- MAGIC Podemos usar **`INSERT INTO`** para adicionar novas linhas a uma tabela Delta existente de forma atômica. Isso permite atualizações incrementais muito mais eficientes do que sobrescrever a tabela inteira.
-- MAGIC
-- MAGIC Acrescente novos registros de vendas na tabela **`sales`** usando **`INSERT INTO`**.
-- MAGIC

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-30m`

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC
-- MAGIC Note que **`INSERT INTO`** não tem garantia embutida para evitar duplicação de registros. Reexecutar a célula acima adicionaria os mesmos registros novamente, gerando duplicatas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Atualizações com Merge  
-- MAGIC
-- MAGIC Você pode fazer upsert de dados de uma tabela, view ou DataFrame fonte para uma tabela Delta alvo usando a operação SQL **`MERGE`**. O Delta Lake suporta inserções, atualizações e exclusões em **`MERGE`**, e possui sintaxe estendida além do padrão SQL para facilitar casos de uso avançados.
-- MAGIC
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Vamos usar **`MERGE`**  para atualizar dados históricos de usuários com emails atualizados e incluir novos usuários.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/raw/users-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Os principais benefícios do **`MERGE`**:  
-- MAGIC * atualizações, inserções e exclusões são concluídas como uma única transação  
-- MAGIC * múltiplas condições podem ser adicionadas além dos campos de correspondência  
-- MAGIC * oferece opções extensas para implementar lógica customizada
-- MAGIC
-- MAGIC Abaixo, vamos atualizar registros apenas se a linha atual tiver um email **`NULL`** e a nova linha não.  
-- MAGIC
-- MAGIC Todos os registros não correspondentes do novo lote serão inseridos.

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que especificamos explicitamente o comportamento desta função tanto para as condições **`MATCHED`** quanto para **`NOT MATCHED`**; o exemplo aqui demonstrado é apenas uma lógica possível, não representa todo o comportamento do **`MERGE`**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Merge somente para inserção e deduplicação
-- MAGIC
-- MAGIC Um caso comum de ETL é coletar logs ou outros datasets que só crescem por anexação numa tabela Delta via várias operações de append.
-- MAGIC
-- MAGIC Muitos sistemas fonte podem gerar registros duplicados. Com **`MERGE`**, é possível evitar a inserção desses duplicados realizando um merge somente para inserção.
-- MAGIC
-- MAGIC Este comando otimizado usa a mesma sintaxe do **`MERGE`**, mas fornece apenas a cláusula **`WHEN NOT MATCHED`**.
-- MAGIC
-- MAGIC Abaixo, usamos isso para garantir que registros com o mesmo **`user_id`** e **`event_timestamp`** não existam já na tabela **`events`**.
-- MAGIC

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Ingestão Incremental
-- MAGIC
-- MAGIC **`COPY INTO`** oferece para engenheiros SQL uma opção idempotente para ingerir dados incrementalmente de sistemas externos.
-- MAGIC
-- MAGIC Note que essa operação tem algumas expectativas:  
-- MAGIC - O esquema dos dados deve ser consistente  
-- MAGIC - Registros duplicados devem ser excluídos ou tratados posteriormente
-- MAGIC
-- MAGIC Esta operação pode ser muito mais barata que escanear a tabela inteira para dados que crescem de forma previsível.
-- MAGIC
-- MAGIC Aqui mostramos execução simples numa pasta estática, mas o real valor está em executar várias vezes ao longo do tempo, capturando novos arquivos automaticamente.
-- MAGIC

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula a seguir para deletar as tabelas e arquivos associados a esta lição.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
-- MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>

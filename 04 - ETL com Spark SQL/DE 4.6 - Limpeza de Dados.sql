-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Limpeza de Dados
-- MAGIC
-- MAGIC A maioria das transformações realizadas com Spark SQL será familiar para desenvolvedores que conhecem SQL.
-- MAGIC
-- MAGIC À medida que inspecionamos e limpamos nossos dados, precisaremos construir várias expressões de colunas e consultas para expressar as transformações que serão aplicadas ao nosso conjunto de dados.
-- MAGIC
-- MAGIC As expressões de colunas são construídas a partir de colunas existentes, operadores e funções internas do Spark SQL. Elas podem ser usadas em instruções **`SELECT`** para expressar transformações que criam novas colunas a partir dos conjuntos de dados.
-- MAGIC
-- MAGIC Além do **`SELECT`**, muitos outros comandos de consulta podem ser usados para expressar transformações no Spark SQL, incluindo **`WHERE`**, **`DISTINCT`**, **`ORDER BY`**, **`GROUP BY`**, etc.
-- MAGIC
-- MAGIC Neste notebook, vamos revisar alguns conceitos que podem ser diferentes de outros sistemas que você esteja acostumado, bem como destacar algumas funções úteis para operações comuns.
-- MAGIC
-- MAGIC Vamos prestar atenção especial aos comportamentos relacionados a valores **`NULL`**, bem como à formatação de strings e campos de data/hora.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Resumir conjuntos de dados e descrever comportamentos de valores nulos
-- MAGIC - Recuperar e remover duplicatas
-- MAGIC - Validar conjuntos de dados para contagens esperadas, valores ausentes e registros duplicados
-- MAGIC - Aplicar transformações comuns para limpar e transformar dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para que o restante deste notebook seja executado.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Trabalharemos com registros de novos usuários na tabela **`users_dirty`** nesta lição.
-- MAGIC

-- COMMAND ----------

SELECT * FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inspecionar Dados
-- MAGIC
-- MAGIC Vamos começar contando os valores em cada campo dos nossos dados.
-- MAGIC

-- COMMAND ----------

SELECT count(user_id), count(user_first_touch_timestamp), count(email), count(updated), count(*)
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que **`count(col)`** ignora valores **`NULL`** ao contar colunas ou expressões específicas.
-- MAGIC
-- MAGIC No entanto, **`count(*)`** é um caso especial que conta o número total de linhas (incluindo linhas que são apenas valores **`NULL`**).
-- MAGIC
-- MAGIC Para contar valores nulos, use a função **`count_if`** ou a cláusula **`WHERE`** para fornecer uma condição que filtre os registros onde o valor **`IS NULL`**.
-- MAGIC

-- COMMAND ----------

SELECT
  count_if(user_id IS NULL) AS missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) AS missing_timestamps, 
  count_if(email IS NULL) AS missing_emails,
  count_if(updated IS NULL) AS missing_updates
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Está claro que há pelo menos alguns valores nulos em todos os nossos campos. Vamos tentar descobrir o que está causando isso.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Registros Distintos
-- MAGIC
-- MAGIC Comece procurando por linhas distintas.
-- MAGIC

-- COMMAND ----------

SELECT count(DISTINCT(*))
FROM users_dirty

-- COMMAND ----------

SELECT count(DISTINCT(user_id))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como **`user_id`** é gerado junto com o **`user_first_touch_timestamp`**, esses campos devem sempre ter paridade nas contagens.
-- MAGIC

-- COMMAND ----------

SELECT count(DISTINCT(user_first_touch_timestamp))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Aqui observamos que, embora existam alguns registros duplicados em relação ao total de linhas, temos um número muito maior de valores distintos.
-- MAGIC
-- MAGIC Vamos em frente e combinar nossas contagens distintas com contagens por coluna para ver esses valores lado a lado.
-- MAGIC

-- COMMAND ----------

SELECT 
  count(user_id) AS total_ids,
  count(DISTINCT user_id) AS unique_ids,
  count(email) AS total_emails,
  count(DISTINCT email) AS unique_emails,
  count(updated) AS total_updates,
  count(DISTINCT(updated)) AS unique_updates,
  count(*) AS total_rows, 
  count(DISTINCT(*)) AS unique_non_null_rows
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Com base no resumo acima, sabemos:
-- MAGIC * Todos os nossos e-mails são únicos
-- MAGIC * Nossos e-mails contêm o maior número de valores nulos
-- MAGIC * A coluna **`updated`** contém apenas 1 valor distinto, mas a maioria não é nula
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Remover Registros Duplicados
-- MAGIC Com base no comportamento acima, o que você espera que aconteça se usarmos **`DISTINCT *`** para tentar remover registros duplicados?
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_deduped AS
  SELECT DISTINCT(*) FROM users_dirty;

SELECT * FROM users_deduped

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note na prévia acima que parecem haver valores nulos, embora nosso **`COUNT(DISTINCT(*))`** tenha ignorado esses nulos.
-- MAGIC
-- MAGIC Quantas linhas você espera que passaram por este comando **`DISTINCT`**?
-- MAGIC

-- COMMAND ----------

SELECT COUNT(*) FROM users_deduped

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe que agora temos um número completamente novo.
-- MAGIC
-- MAGIC O Spark ignora valores nulos ao contar valores em uma coluna ou contar valores distintos de um campo, mas não omite linhas com valores nulos em uma consulta **`DISTINCT`**.
-- MAGIC
-- MAGIC De fato, a razão pela qual estamos vendo um novo número que é 1 maior que as contagens anteriores é porque temos 3 linhas que são todas nulas (aqui incluídas como uma única linha distinta).
-- MAGIC

-- COMMAND ----------

SELECT * FROM users_dirty
WHERE
  user_id IS NULL AND
  user_first_touch_timestamp IS NULL AND
  email IS NULL AND
  updated IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Remover Duplicatas com Base em Colunas Específicas
-- MAGIC
-- MAGIC Lembre-se de que **`user_id`** e **`user_first_touch_timestamp`** devem formar tuplas únicas, pois ambos são gerados quando um determinado usuário é encontrado pela primeira vez.
-- MAGIC
-- MAGIC Podemos ver que temos alguns valores nulos em cada um desses campos; ao excluir os nulos e contar o número distinto de pares desses campos, obteremos a contagem correta de valores distintos na nossa tabela.
-- MAGIC

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Aqui, usaremos esses pares distintos para remover linhas indesejadas dos nossos dados.
-- MAGIC
-- MAGIC O código abaixo usa **`GROUP BY`** para remover registros duplicados com base em **`user_id`** e **`user_first_touch_timestamp`**.
-- MAGIC
-- MAGIC A função agregada **`max()`** é usada na coluna **`email`** como uma estratégia para capturar e-mails não nulos quando múltiplos registros estão presentes; neste lote, todos os valores de **`updated`** eram equivalentes, mas precisamos usar uma função agregada para manter esse valor no resultado do nosso group by.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Validar Conjuntos de Dados
-- MAGIC Confirmamos visualmente que nossas contagens estão conforme o esperado, com base em nossa análise manual.
-- MAGIC
-- MAGIC Abaixo, realizamos programaticamente algumas validações usando filtros simples e cláusulas **`WHERE`**.
-- MAGIC
-- MAGIC Validar se o **`user_id`** de cada linha é único.
-- MAGIC

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Confirmar se cada e-mail está associado a no máximo um **`user_id`**.
-- MAGIC

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Formato de Data e Regex
-- MAGIC Agora que removemos campos nulos e eliminamos duplicatas, podemos desejar extrair mais valor dos dados.
-- MAGIC
-- MAGIC O código abaixo:
-- MAGIC - Escala e converte corretamente o **`user_first_touch_timestamp`** para um timestamp válido
-- MAGIC - Extrai a data do calendário e a hora do relógio para este timestamp em formato legível
-- MAGIC - Usa **`regexp_extract`** para extrair os domínios da coluna de e-mails usando regex
-- MAGIC

-- COMMAND ----------

SELECT *,
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

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
-- MAGIC

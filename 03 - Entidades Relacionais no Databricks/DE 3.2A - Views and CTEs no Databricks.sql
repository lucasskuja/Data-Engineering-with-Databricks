-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Visualizações (Views) e CTEs no Databricks  
-- MAGIC Nesta demonstração, você irá criar e explorar views e expressões de tabela comuns (CTEs).
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final desta lição, você deverá ser capaz de:  
-- MAGIC * Usar DDL do Spark SQL para definir views  
-- MAGIC * Executar consultas que usam expressões de tabela comuns (CTEs)
-- MAGIC
-- MAGIC **Recursos**  
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">Criar View - Documentação Databricks</a>  
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">Expressões de Tabela Comuns - Documentação Databricks</a>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Configuração da Aula  
-- MAGIC O script a seguir limpa execuções anteriores desta demonstração e configura algumas variáveis Hive que serão usadas em nossas consultas SQL.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.2A

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Começamos criando uma tabela de dados que poderemos usar para a demonstração.
-- MAGIC

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path '${da.paths.working_dir}/flight_delays',
  header "true",
  mode "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Para mostrar a lista de tabelas (e views), usamos o comando **`SHOW TABLES`**, demonstrado também abaixo.
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Views, Temp Views & Global Temp Views
-- MAGIC
-- MAGIC Para preparar esta demonstração, primeiro criaremos um de cada tipo de view.
-- MAGIC
-- MAGIC Depois, no próximo notebook, exploraremos as diferenças entre o comportamento de cada uma.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Views  
-- MAGIC Vamos criar uma view que contenha apenas os dados onde a origem é "ABQ" e o destino é "LAX".
-- MAGIC

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que a view **`view_delays_abq_lax`** foi adicionada à lista abaixo:
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Views Temporárias
-- MAGIC
-- MAGIC A seguir, vamos criar uma view temporária.
-- MAGIC
-- MAGIC A sintaxe é muito parecida, mas adiciona **`TEMPORARY`** ao comando.
-- MAGIC

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Agora, se mostrarmos as tabelas novamente, veremos uma tabela e ambas as views.
-- MAGIC
-- MAGIC Observe os valores na coluna **`isTemporary`**.
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Views Temporárias Globais
-- MAGIC
-- MAGIC Por fim, vamos criar uma view temporária global.
-- MAGIC
-- MAGIC Aqui, simplesmente adicionamos **`GLOBAL`** ao comando.
-- MAGIC
-- MAGIC Também observe o qualificador de banco de dados **`global_temp`** na instrução **`SELECT`** subsequente.
-- MAGIC

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Antes de avançarmos, revise mais uma vez as tabelas e views do banco de dados...
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ...e as tabelas e views no banco de dados **`global_temp`**:
-- MAGIC

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A seguir, vamos demonstrar como tabelas e views são mantidas entre múltiplas sessões e como as views temporárias não são.
-- MAGIC
-- MAGIC Para isso, basta abrir o próximo notebook, [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont), e continuar a lição.
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Nota: Existem vários cenários em que uma nova sessão pode ser criada:
-- MAGIC * Reiniciar um cluster
-- MAGIC * Desanexar e reanexar a um cluster
-- MAGIC * Instalar um pacote Python que reinicia o interpretador Python
-- MAGIC * Ou simplesmente abrir um novo notebook
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
-- MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
-- MAGIC

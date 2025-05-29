-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Criando Tabelas Delta
-- MAGIC
-- MAGIC Após extrair dados de fontes externas, carregue os dados no Lakehouse para garantir que todos os benefícios da plataforma Databricks possam ser plenamente aproveitados.
-- MAGIC
-- MAGIC Embora diferentes organizações possam ter políticas variadas sobre como os dados são inicialmente carregados no Databricks, normalmente recomendamos que as primeiras tabelas representem uma versão mais bruta dos dados, e que validação e enriquecimento ocorram em etapas posteriores. Esse padrão garante que, mesmo que os dados não correspondam às expectativas em relação a tipos ou nomes de colunas, nenhum dado será perdido, permitindo intervenção programática ou manual para recuperar dados parcialmente corrompidos ou inválidos.
-- MAGIC
-- MAGIC Esta lição focará principalmente no padrão usado para criar a maioria das tabelas, as declarações **`CREATE TABLE _ AS SELECT`** (CTAS).
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final desta lição, você deverá ser capaz de:  
-- MAGIC - Usar declarações CTAS para criar tabelas Delta Lake  
-- MAGIC - Criar novas tabelas a partir de views ou tabelas existentes  
-- MAGIC - Enriquecer dados carregados com metadados adicionais  
-- MAGIC - Declarar esquema da tabela com colunas geradas e comentários descritivos  
-- MAGIC - Definir opções avançadas para controlar localização dos dados, qualidade e particionamento
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Setup
-- MAGIC
-- MAGIC O script de setup criará os dados e declarará valores necessários para o restante deste notebook ser executado.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC Declarações **`CREATE TABLE AS SELECT`** criam e populam tabelas Delta usando dados recuperados de uma consulta de entrada.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Declarações CTAS inferem automaticamente o esquema a partir dos resultados da consulta e **não** suportam declaração manual de esquema.
-- MAGIC
-- MAGIC Isso significa que CTAS são úteis para ingestão de dados externos de fontes com esquema bem definido, como arquivos Parquet e tabelas.
-- MAGIC
-- MAGIC CTAS também não suportam especificar opções adicionais de arquivo.
-- MAGIC
-- MAGIC Podemos ver como isso apresenta limitações significativas ao tentar ingerir dados de arquivos CSV.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/raw/sales-csv/`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Para ingerir esses dados corretamente em uma tabela Delta Lake, precisaremos usar uma referência aos arquivos que permita especificar opções.
-- MAGIC
-- MAGIC Na lição anterior, mostramos isso registrando uma tabela externa. Aqui, evoluiremos um pouco essa sintaxe para especificar opções numa view temporária, e então usar essa view como fonte para uma declaração CTAS para registrar com sucesso a tabela Delta.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path "${da.paths.datasets}/raw/sales-csv",
  header "true",
  delimiter "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtrando e Renomeando Colunas de Tabelas Existentes
-- MAGIC
-- MAGIC Transformações simples como mudar nomes de colunas ou omitir colunas das tabelas alvo podem ser facilmente feitas durante a criação da tabela.
-- MAGIC
-- MAGIC A declaração abaixo cria uma nova tabela contendo um subconjunto de colunas da tabela **`sales`**.
-- MAGIC
-- MAGIC Aqui, presumimos que estamos intencionalmente omitindo informações que possam identificar usuários ou que forneçam detalhes itemizados de compra. Também renomeamos nossos campos assumindo que um sistema downstream usa convenções de nomes diferentes da fonte.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que poderíamos ter conseguido o mesmo resultado com uma view, como mostrado abaixo.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Esquema com Colunas Geradas
-- MAGIC
-- MAGIC Como mencionado anteriormente, declarações CTAS não suportam declaração de esquema. Notamos que a coluna timestamp parece ser uma variante de timestamp Unix, o que pode não ser útil para analistas extraírem insights. Essa é uma situação onde colunas geradas seriam benéficas.
-- MAGIC
-- MAGIC Colunas geradas são um tipo especial de coluna cujos valores são automaticamente gerados com base numa função definida pelo usuário sobre outras colunas da tabela Delta (introduzidas no DBR 8.3).
-- MAGIC
-- MAGIC O código abaixo demonstra a criação de uma nova tabela enquanto:
-- MAGIC 1. Especifica nomes e tipos de colunas  
-- MAGIC 2. Adiciona uma <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">coluna gerada</a> para calcular a data  
-- MAGIC 3. Fornece um comentário descritivo para a coluna gerada
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como **`date`** é uma coluna gerada, se escrevermos para **`purchase_dates`** sem fornecer valores para a coluna **`date`**, o Delta Lake os calcula automaticamente.
-- MAGIC
-- MAGIC **NOTA**: A célula abaixo configura uma opção para permitir colunas geradas quando usar a declaração Delta Lake **`MERGE`**. Veremos mais dessa sintaxe mais adiante no curso.
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Podemos ver abaixo que todas as datas foram calculadas corretamente conforme os dados foram inseridos, embora nem os dados fonte nem a consulta de inserção tenham especificado valores nesse campo.
-- MAGIC
-- MAGIC Como qualquer fonte Delta Lake, a consulta sempre lê o snapshot mais recente da tabela; não é necessário rodar **`REFRESH TABLE`**.
-- MAGIC

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC É importante notar que se um campo que deveria ser gerado for incluído em um insert na tabela, o insert falhará se o valor fornecido não coincidir exatamente com o valor derivado pela lógica da coluna gerada.
-- MAGIC
-- MAGIC Podemos ver esse erro descomentando e executando a célula abaixo:
-- MAGIC

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Adicionar uma Restrição (Constraint) na Tabela
-- MAGIC
-- MAGIC A mensagem de erro acima refere-se a uma **`CHECK constraint`**. Colunas geradas são uma implementação especial de check constraints.
-- MAGIC
-- MAGIC Como o Delta Lake aplica esquema no momento da escrita, o Databricks suporta cláusulas SQL padrão para gerenciamento de constraints, garantindo qualidade e integridade dos dados na tabela.
-- MAGIC
-- MAGIC Atualmente, Databricks suporta dois tipos de constraints:  
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>  
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>  
-- MAGIC
-- MAGIC Em ambos os casos, você deve garantir que não existam dados violando a constraint antes de defini-la. Após adicionada, dados que violarem a constraint resultarão em falha na escrita.
-- MAGIC
-- MAGIC Abaixo, adicionaremos uma **`CHECK`** constraint à coluna **`date`** da nossa tabela. Note que constraints **`CHECK`** se parecem com cláusulas **`WHERE`** usadas para filtrar datasets.
-- MAGIC

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC As constraints da tabela são exibidas no campo **`TBLPROPERTIES`**.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Enriquecer Tabelas com Opções e Metadados Adicionais
-- MAGIC
-- MAGIC Até agora apenas arranhamos a superfície das opções para enriquecer tabelas Delta Lake.
-- MAGIC
-- MAGIC A seguir, mostramos uma evolução da declaração CTAS para incluir várias configurações e metadados adicionais.
-- MAGIC
-- MAGIC Nossa cláusula **`SELECT`** usa dois comandos Spark SQL úteis para ingestão de arquivos:  
-- MAGIC * **`current_timestamp()`** registra o timestamp da execução da lógica  
-- MAGIC * **`input_file_name()`** registra o arquivo fonte de cada registro na tabela  
-- MAGIC
-- MAGIC Também incluímos lógica para criar uma nova coluna de data derivada dos dados timestamp da fonte.
-- MAGIC
-- MAGIC A cláusula **`CREATE TABLE`** contém várias opções:  
-- MAGIC * Um **`COMMENT`** para facilitar a descoberta do conteúdo da tabela  
-- MAGIC * Um **`LOCATION`** especificado, resultando em uma tabela externa (não gerenciada)  
-- MAGIC * A tabela é **`PARTITIONED BY`** pela coluna de data; isso significa que os dados de cada data existirão em seu próprio diretório na localização de armazenamento alvo  
-- MAGIC
-- MAGIC **NOTA**: Particionamento é mostrado aqui principalmente para demonstrar sintaxe e impacto. A maioria das tabelas Delta Lake (especialmente para dados pequenos e médios) não se beneficia do particionamento. Como particionamento separa fisicamente arquivos, isso pode causar problemas com arquivos pequenos e impedir compactação e saltos eficientes de arquivos. Benefícios observados em Hive ou HDFS não se aplicam ao Delta Lake. Consulte um arquiteto experiente antes de particionar tabelas.
-- MAGIC
-- MAGIC **Como prática recomendada, use tabelas não particionadas para a maioria dos casos com Delta Lake.**
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Os campos de metadados adicionados à tabela fornecem informações úteis para entender quando registros foram inseridos e de onde vieram. Isso pode ser especialmente útil para resolver problemas na fonte dos dados.
-- MAGIC
-- MAGIC Todos os comentários e propriedades de uma tabela podem ser revisados usando **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC
-- MAGIC **NOTA**: Delta Lake adiciona automaticamente várias propriedades na criação da tabela.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Listar a localização usada pela tabela revela que os valores únicos da coluna de particionamento **`first_touch_date`** são usados para criar diretórios de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Clonagem de Tabelas Delta Lake
-- MAGIC
-- MAGIC Delta Lake oferece duas opções para copiar tabelas eficientemente.
-- MAGIC
-- MAGIC **`DEEP CLONE`** copia completamente dados e metadados da tabela fonte para a tabela destino. Essa cópia é incremental, permitindo sincronizar alterações da fonte para o destino executando o comando novamente.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como todos os arquivos de dados precisam ser copiados, isso pode levar bastante tempo para grandes volumes.
-- MAGIC
-- MAGIC Se quiser criar uma cópia rápida da tabela para testar alterações sem risco de modificar a tabela atual, **`SHALLOW CLONE`** é uma boa opção. Shallow clones copiam apenas os logs de transação Delta, sem mover dados.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Em ambos os casos, modificações feitas na tabela clonada são armazenadas separadamente da fonte. Clonagem é ótima para preparar tabelas para testes de código SQL ainda em desenvolvimento.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Resumo
-- MAGIC
-- MAGIC Neste notebook, focamos principalmente na DDL e na sintaxe para criar tabelas Delta Lake. No próximo notebook, exploraremos opções para escrever atualizações em tabelas.
-- MAGIC

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
-- MAGIC

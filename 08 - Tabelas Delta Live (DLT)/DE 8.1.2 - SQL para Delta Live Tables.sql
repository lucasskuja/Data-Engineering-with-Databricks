-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # SQL para Delta Live Tables
-- MAGIC
-- MAGIC Na última lição, vimos o processo de agendar este notebook como um pipeline do Delta Live Tables (DLT). Agora vamos explorar o conteúdo deste notebook para entender melhor a sintaxe usada pelo DLT.
-- MAGIC
-- MAGIC Este notebook usa SQL para declarar tabelas Delta Live Tables que, juntas, implementam uma arquitetura *multi-hop* simples com base em um conjunto de dados de exemplo fornecido pela Databricks, carregado por padrão nos workspaces do Databricks.
-- MAGIC
-- MAGIC De forma simples, você pode pensar em SQL para DLT como uma leve modificação das instruções tradicionais CTAS. Tabelas e visualizações no DLT sempre são precedidas pela palavra-chave **`LIVE`**.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Definir tabelas e visualizações com Delta Live Tables
-- MAGIC * Usar SQL para ingerir dados brutos incrementalmente com Auto Loader
-- MAGIC * Realizar leituras incrementais em tabelas Delta usando SQL
-- MAGIC * Atualizar o código e reimplantar um pipeline
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Tabelas da Camada Bronze
-- MAGIC
-- MAGIC Abaixo, declaramos duas tabelas que implementam a camada bronze. Esta representa os dados em sua forma mais bruta, mas capturados em um formato que pode ser retido indefinidamente e consultado com o desempenho e os benefícios oferecidos pelo Delta Lake.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### sales_orders_raw
-- MAGIC
-- MAGIC **`sales_orders_raw`** ingere dados JSON incrementalmente a partir do conjunto de dados de exemplo localizado em */databricks-datasets/retail-org/sales_orders/*.
-- MAGIC
-- MAGIC O processamento incremental via <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> (que usa o mesmo modelo de processamento do Structured Streaming), requer a adição da palavra-chave **`STREAMING`** na declaração, como mostrado abaixo. O método **`cloud_files()`** permite usar o Auto Loader diretamente com SQL. Este método recebe os seguintes parâmetros posicionais:
-- MAGIC * O local de origem, como mencionado acima
-- MAGIC * O formato dos dados de origem, que neste caso é JSON
-- MAGIC * Um array de tamanho arbitrário com opções opcionais de leitura. Neste caso, definimos **`cloudFiles.inferColumnTypes`** como **`true`**
-- MAGIC
-- MAGIC A declaração a seguir também demonstra a definição de metadados adicionais da tabela (um comentário e propriedades, neste caso), que ficam visíveis para qualquer pessoa que explore o catálogo de dados.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### customers
-- MAGIC
-- MAGIC **`customers`** apresenta dados de clientes em formato CSV localizados em */databricks-datasets/retail-org/customers/*. Esta tabela será usada em uma operação de junção para buscar dados de clientes com base nos registros de vendas.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Tabelas da Camada Silver
-- MAGIC
-- MAGIC Agora declaramos as tabelas que implementam a camada silver. Esta camada representa uma cópia refinada dos dados da camada bronze, com o objetivo de otimizar aplicações a jusante. Nesta etapa, aplicamos operações como limpeza e enriquecimento de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### sales_orders_cleaned
-- MAGIC
-- MAGIC Aqui declaramos nossa primeira tabela silver, que enriquece os dados de transações de vendas com informações de clientes, além de implementar controle de qualidade ao rejeitar registros com número de pedido nulo.
-- MAGIC
-- MAGIC Essa declaração introduz diversos novos conceitos.
-- MAGIC
-- MAGIC #### Controle de Qualidade
-- MAGIC
-- MAGIC A palavra-chave **`CONSTRAINT`** introduz o controle de qualidade. Similar a uma cláusula **`WHERE`** tradicional, **`CONSTRAINT`** se integra ao DLT, permitindo que ele colete métricas sobre violações da restrição. Restrições fornecem uma cláusula opcional **`ON VIOLATION`**, especificando a ação a ser tomada em registros que violem a regra. Os três modos atualmente suportados pelo DLT são:
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Comportamento |
-- MAGIC |--------------------|---------------|
-- MAGIC | **`FAIL UPDATE`**  | Falha no pipeline se a restrição for violada |
-- MAGIC | **`DROP ROW`**     | Descarta os registros que violarem a restrição |
-- MAGIC | Omitido            | Registros violadores serão incluídos (mas a violação será registrada nas métricas) |
-- MAGIC
-- MAGIC #### Referência a Tabelas e Visualizações do DLT
-- MAGIC
-- MAGIC Referências a outras tabelas e visualizações do DLT sempre incluem o prefixo **`live.`**. Um nome de banco de dados de destino será automaticamente substituído em tempo de execução, permitindo a fácil migração de pipelines entre ambientes DEV/QA/PROD.
-- MAGIC
-- MAGIC #### Referência a Tabelas de Streaming
-- MAGIC
-- MAGIC Referências a tabelas de streaming do DLT usam **`STREAM()`**, fornecendo o nome da tabela como argumento.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Declarar Tabela Gold
-- MAGIC
-- MAGIC No nível mais refinado da arquitetura, declaramos uma tabela que fornece uma agregação com valor de negócio — neste caso, um conjunto de dados de pedidos de vendas de uma região específica. Ao agregar, o relatório gera contagens e totais de pedidos por data e por cliente.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Explorar Resultados
-- MAGIC
-- MAGIC Explore o DAG (grafo acíclico direcionado) que representa as entidades envolvidas no pipeline e os relacionamentos entre elas. Clique em cada entidade para ver um resumo, que inclui:
-- MAGIC * Status da execução
-- MAGIC * Resumo dos metadados
-- MAGIC * Esquema
-- MAGIC * Métricas de qualidade dos dados
-- MAGIC
-- MAGIC Consulte este <a href="$./DE 8.3 - Pipeline Results" target="_blank">notebook complementar</a> para inspecionar tabelas e logs.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Atualizar o Pipeline
-- MAGIC
-- MAGIC Descomente a célula a seguir para declarar outra tabela gold. De forma semelhante à declaração anterior, esta filtra pela **`city`** de Chicago.
-- MAGIC
-- MAGIC Execute novamente seu pipeline para examinar os resultados atualizados.
-- MAGIC
-- MAGIC A execução ocorre como esperado?
-- MAGIC
-- MAGIC Você consegue identificar algum problema?
-- MAGIC

-- COMMAND ----------

-- TODO
-- CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
-- COMMENT "Sales orders in Chicago."
-- AS
--   SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
--          sum(ordered_products_explode.price) as sales, 
--          sum(ordered_products_explode.qty) as quantity, 
--          count(ordered_products_explode.id) as product_count
--   FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
--         FROM sales_orders_cleaned 
--         WHERE city = 'Chicago')
--   GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
-- MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
-- MAGIC

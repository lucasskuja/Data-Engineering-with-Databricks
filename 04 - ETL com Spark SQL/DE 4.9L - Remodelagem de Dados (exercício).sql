-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Laboratório de Remodelagem de Dados
-- MAGIC
-- MAGIC Neste laboratório, você criará uma tabela **`clickpaths`** que agrega o número de vezes que cada usuário realizou uma determinada ação na tabela **`events`**, e então juntará essas informações com a visualização achatada da tabela **`transactions`** criada no notebook anterior.
-- MAGIC
-- MAGIC Você também explorará uma nova função de ordem superior para sinalizar itens registrados em **`sales`** com base em informações extraídas dos nomes dos itens.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizado
-- MAGIC Ao final deste laboratório, você deverá ser capaz de:
-- MAGIC - Realizar pivot e join em tabelas para criar caminhos de cliques (clickpaths) para cada usuário
-- MAGIC - Aplicar funções de ordem superior para identificar tipos de produtos comprados
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para que o restante deste notebook seja executado.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.9L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Remodelar Conjuntos de Dados para Criar Caminhos de Cliques
-- MAGIC
-- MAGIC Essa operação juntará dados das tabelas **`events`** e **`transactions`** para criar um registro de todas as ações que um usuário realizou no site e como foi seu pedido final.
-- MAGIC
-- MAGIC A tabela **`clickpaths`** deve conter todos os campos da tabela **`transactions`**, bem como uma contagem de cada **`event_name`** em sua própria coluna. Cada usuário que concluiu uma compra deve ter uma única linha na tabela final. Vamos começar realizando o pivot da tabela **`events`** para obter as contagens de cada **`event_name`**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 1. Fazer pivot na tabela **`events`** para contar ações por usuário
-- MAGIC
-- MAGIC Queremos agregar o número de vezes que cada usuário realizou um evento específico, especificado na coluna **`event_name`**. Para isso, agrupe por **`user`** e faça pivot em **`event_name`** para fornecer uma contagem de cada tipo de evento em sua própria coluna, resultando no seguinte esquema:
-- MAGIC
-- MAGIC | campo | tipo | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC
-- MAGIC Uma lista dos nomes de eventos é fornecida abaixo.
-- MAGIC

-- COMMAND ----------

-- TODO
CREATE OR REPLACE VIEW events_pivot
<FILL_IN>
("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Usaremos Python para realizar verificações pontuais ao longo do laboratório. As funções auxiliares abaixo retornarão um erro com uma mensagem indicando o que precisa ser alterado se você não tiver seguido as instruções. Nenhuma saída significa que você completou essa etapa corretamente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, column_names, num_rows):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert spark.table(table_name).columns == column_names, "Please name the columns in the order provided above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que a visualização foi criada corretamente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_columns = ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium']
-- MAGIC check_table_results("events_pivot", event_columns, 204586)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 2. Juntar contagens de eventos e transações de todos os usuários
-- MAGIC
-- MAGIC Em seguida, junte **`events_pivot`** com **`transactions`** para criar a tabela **`clickpaths`**. Esta tabela deve conter as mesmas colunas de nomes de eventos da tabela **`events_pivot`** criada acima, seguidas pelas colunas da tabela **`transactions`**, conforme mostrado abaixo:
-- MAGIC
-- MAGIC | campo | tipo | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |
-- MAGIC

-- COMMAND ----------

-- TODO
CREATE OR REPLACE VIEW clickpaths AS
<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que a tabela foi criada corretamente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clickpath_columns = event_columns + ['user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K']
-- MAGIC check_table_results("clickpaths", clickpath_columns, 9085)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Sinalizar Tipos de Produtos Comprados
-- MAGIC
-- MAGIC Aqui, você usará a função de ordem superior **`EXISTS`** para criar as colunas booleanas **`mattress`** e **`pillow`**, que indicam se o item comprado era um produto do tipo colchão (mattress) ou travesseiro (pillow).
-- MAGIC
-- MAGIC Por exemplo, se o **`item_name`** da coluna **`items`** termina com a string **`"Mattress"`**, o valor da coluna **`mattress`** deve ser **`true`** e o valor da coluna **`pillow`** deve ser **`false`**. Veja alguns exemplos de itens e os valores resultantes:
-- MAGIC
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC Veja a documentação da função <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a>.  
-- MAGIC Você pode usar a expressão condicional **`item_name LIKE "%Mattress"`** para verificar se a string **`item_name`** termina com a palavra "Mattress".
-- MAGIC

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE sales_product_flags AS
<FILL_IN>
EXISTS <FILL_IN>.item_name LIKE "%Mattress"
EXISTS <FILL_IN>.item_name LIKE "%Pillow"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que a tabela foi criada corretamente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", ['items', 'mattress', 'pillow'], 10539)
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 10015, 'num_pillow': 1386}, "There should be 10015 rows where mattress is true, and 1386 where pillow is true"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
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

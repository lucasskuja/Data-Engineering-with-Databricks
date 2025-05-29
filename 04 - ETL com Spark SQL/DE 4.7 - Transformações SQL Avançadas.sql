-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Transformações SQL Avançadas
-- MAGIC
-- MAGIC Consultar dados tabulares armazenados no data lakehouse com Spark SQL é fácil, eficiente e rápido.
-- MAGIC
-- MAGIC Isso se torna mais complicado conforme a estrutura dos dados se torna menos regular, quando muitas tabelas precisam ser usadas em uma única consulta, ou quando o formato dos dados precisa mudar drasticamente. Este notebook apresenta várias funções presentes no Spark SQL para ajudar engenheiros a realizar até mesmo as transformações mais complexas.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Usar a sintaxe **`.`** e **`:`** para consultar dados aninhados
-- MAGIC - Trabalhar com JSON
-- MAGIC - "Achatar" e desempacotar arrays e structs
-- MAGIC - Combinar conjuntos de dados usando joins e operadores de conjunto
-- MAGIC - Reformular dados usando tabelas dinâmicas (pivot)
-- MAGIC - Usar funções de ordem superior para trabalhar com arrays
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para o restante deste notebook ser executado.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.7

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Interagindo com Dados JSON
-- MAGIC
-- MAGIC A tabela **`events_raw`** foi registrada a partir de dados representando um payload do Kafka.
-- MAGIC
-- MAGIC Na maioria dos casos, dados do Kafka são valores JSON codificados em binário. Vamos converter os campos **`key`** e **`value`** em strings abaixo para visualizá-los em um formato legível.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS
  SELECT string(key), string(value) 
  FROM events_raw;
  
SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O Spark SQL possui funcionalidades embutidas para interagir diretamente com dados JSON armazenados como strings. Podemos usar a sintaxe **`:`** para navegar em estruturas de dados aninhadas.
-- MAGIC

-- COMMAND ----------

SELECT value:device, value:geo:city 
FROM events_strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O Spark SQL também possui a capacidade de analisar objetos JSON em tipos struct (um tipo nativo do Spark com atributos aninhados).
-- MAGIC
-- MAGIC No entanto, a função **`from_json`** requer um schema. Para derivar o schema dos nossos dados atuais, começaremos executando uma consulta que sabemos que retornará um valor JSON sem campos nulos.
-- MAGIC

-- COMMAND ----------

SELECT value 
FROM events_strings 
WHERE value:event_name = "finalize" 
ORDER BY key
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O Spark SQL também possui a função **`schema_of_json`** para derivar o schema JSON a partir de um exemplo. Aqui, copiamos e colamos um JSON de exemplo na função e a encadeamos com a função **`from_json`** para converter nosso campo **`value`** em um tipo struct.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Uma vez que uma string JSON é convertida para o tipo struct, o Spark oferece suporte ao uso de **`*`** (asterisco) para "achatar" os campos em colunas.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Explorando Estruturas de Dados
-- MAGIC
-- MAGIC O Spark SQL tem uma sintaxe robusta para trabalhar com tipos de dados complexos e aninhados.
-- MAGIC
-- MAGIC Comece visualizando os campos da tabela **`events`**.
-- MAGIC

-- COMMAND ----------

DESCRIBE events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O campo **`ecommerce`** é um struct que contém um double e dois longs.
-- MAGIC
-- MAGIC Podemos interagir com os subcampos desse campo usando a sintaxe padrão **`.`**, semelhante à navegação em JSON.
-- MAGIC

-- COMMAND ----------

SELECT ecommerce.purchase_revenue_in_usd 
FROM events
WHERE ecommerce.purchase_revenue_in_usd IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Explodir Arrays
-- MAGIC
-- MAGIC O campo **`items`** na tabela **`events`** é um array de structs.
-- MAGIC
-- MAGIC O Spark SQL possui várias funções específicas para lidar com arrays.
-- MAGIC
-- MAGIC A função **`explode`** permite colocar cada elemento de um array em sua própria linha.
-- MAGIC

-- COMMAND ----------

SELECT user_id, event_timestamp, event_name, explode(items) AS item 
FROM events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Coletar Arrays
-- MAGIC
-- MAGIC A função **`collect_set`** pode coletar valores únicos de um campo, inclusive campos dentro de arrays.
-- MAGIC
-- MAGIC A função **`flatten`** permite combinar vários arrays em um único array.
-- MAGIC
-- MAGIC A função **`array_distinct`** remove elementos duplicados de um array.
-- MAGIC
-- MAGIC Aqui, combinamos essas consultas para criar uma tabela simples que mostra a coleção única de ações e os itens no carrinho de um usuário.
-- MAGIC

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Junções de Tabelas
-- MAGIC
-- MAGIC O Spark SQL suporta operações padrão de junção (inner, outer, left, right, anti, cross, semi).
-- MAGIC
-- MAGIC Aqui encadeamos uma junção com uma tabela de consulta a uma operação **`explode`** para obter o nome impresso padrão do item.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_enriched AS
SELECT *
FROM (
  SELECT *, explode(items) AS item 
  FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Operadores de Conjunto
-- MAGIC
-- MAGIC O Spark SQL suporta os operadores de conjunto **`UNION`**, **`MINUS`** e **`INTERSECT`**.
-- MAGIC
-- MAGIC **`UNION`** retorna a união de dois conjuntos de resultados.
-- MAGIC
-- MAGIC A consulta abaixo retorna os mesmos resultados que se inseríssemos nosso **`new_events_final`** na tabela **`events`**.
-- MAGIC

-- COMMAND ----------

SELECT * FROM events 
UNION 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **`INTERSECT`** retorna todas as linhas encontradas em ambas as relações.
-- MAGIC

-- COMMAND ----------

SELECT * FROM events 
INTERSECT 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A consulta acima não retorna resultados porque nossos dois conjuntos de dados não têm valores em comum.
-- MAGIC
-- MAGIC **`MINUS`** retorna todas as linhas encontradas em um conjunto de dados mas não no outro; vamos pular essa execução aqui, já que a consulta anterior demonstra que não temos valores em comum.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Tabelas Dinâmicas (Pivot)
-- MAGIC
-- MAGIC A cláusula **`PIVOT`** é usada para mudar a perspectiva dos dados. Podemos obter valores agregados com base em valores de uma coluna específica, que serão transformados em múltiplas colunas na cláusula **`SELECT`**. A cláusula **`PIVOT`** pode ser especificada após o nome da tabela ou uma subconsulta.
-- MAGIC
-- MAGIC **`SELECT * FROM ()`**: A instrução **`SELECT`** dentro dos parênteses é a entrada para essa tabela.
-- MAGIC
-- MAGIC **`PIVOT`**: O primeiro argumento na cláusula é uma função agregada e a coluna a ser agregada. Depois, especificamos a coluna de pivot na subcláusula **`FOR`**. O operador **`IN`** contém os valores da coluna de pivot.
-- MAGIC
-- MAGIC Aqui usamos **`PIVOT`** para criar uma nova tabela **`transactions`** que achata as informações contidas na tabela **`sales`**.
-- MAGIC
-- MAGIC Esse formato de dados achatado pode ser útil para dashboards, mas também útil para aplicar algoritmos de machine learning para inferência ou previsão.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    email,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    item.item_id AS item_id,
    item.quantity AS quantity
  FROM sales_enriched
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Funções de Ordem Superior
-- MAGIC
-- MAGIC Funções de ordem superior no Spark SQL permitem trabalhar diretamente com tipos de dados complexos. Ao trabalhar com dados hierárquicos, os registros frequentemente são armazenados como objetos do tipo array ou map. Funções de ordem superior permitem transformar dados preservando sua estrutura original.
-- MAGIC
-- MAGIC Funções de ordem superior incluem:
-- MAGIC - **`FILTER`** filtra um array usando a função lambda fornecida.
-- MAGIC - **`EXIST`** testa se uma condição é verdadeira para um ou mais elementos do array.
-- MAGIC - **`TRANSFORM`** usa a função lambda fornecida para transformar todos os elementos de um array.
-- MAGIC - **`REDUCE`** usa duas funções lambda para reduzir os elementos de um array a um único valor, mesclando os elementos em um buffer e aplicando uma função final ao buffer.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filter
-- MAGIC
-- MAGIC Remova itens que não são king-size de todos os registros da nossa coluna **`items`**. Podemos usar a função **`FILTER`** para criar uma nova coluna que exclui esse valor de cada array.
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC Na instrução acima:
-- MAGIC - **`FILTER`**: nome da função de ordem superior  
-- MAGIC - **`items`**: nome do array de entrada  
-- MAGIC - **`i`**: nome da variável iteradora. Você escolhe esse nome e o utiliza na função lambda. Ela percorre o array, passando cada valor para a função uma de cada vez.  
-- MAGIC - **`->`**: indica o início da função  
-- MAGIC - **`i.item_id LIKE "%K"`**: esta é a função. Cada valor é verificado para ver se termina com a letra maiúscula K. Se for o caso, é filtrado para a nova coluna **`king_items`**
-- MAGIC

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Você pode acabar criando muitos arrays vazios na nova coluna. Quando isso acontecer, pode ser útil usar uma cláusula **`WHERE`** para mostrar apenas os valores não vazios no array da coluna retornada.
-- MAGIC
-- MAGIC Neste exemplo, fazemos isso usando uma subconsulta (uma consulta dentro de outra). Elas são úteis para realizar uma operação em múltiplas etapas. Neste caso, estamos usando-a para criar a coluna nomeada que usaremos com a cláusula **`WHERE`**.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transform
-- MAGIC
-- MAGIC Funções embutidas são projetadas para operar sobre um único tipo de dado simples dentro de uma célula; elas não conseguem processar valores de array. **`TRANSFORM`** pode ser particularmente útil quando você deseja aplicar uma função existente a cada elemento de um array.
-- MAGIC
-- MAGIC Calcule a receita total de itens king-size por pedido.
-- MAGIC
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC
-- MAGIC Na instrução acima, para cada valor no array de entrada, extraímos o valor da receita do item, multiplicamos por 100 e convertemos o resultado para inteiro. Note que estamos usando o mesmo tipo de referência do comando anterior, mas nomeamos o iterador com uma nova variável, **`k`**.
-- MAGIC

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Resumo
-- MAGIC
-- MAGIC O Spark SQL oferece um conjunto abrangente de funcionalidades nativas para interagir e manipular dados altamente aninhados.
-- MAGIC
-- MAGIC Embora alguma sintaxe possa parecer incomum para usuários de SQL, aproveitar funções embutidas como as de ordem superior pode evitar que engenheiros precisem recorrer a lógica personalizada ao lidar com estruturas de dados altamente complexas.
-- MAGIC

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

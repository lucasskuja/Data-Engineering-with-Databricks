-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # UDFs em SQL e Fluxo de Controle
-- MAGIC
-- MAGIC O Databricks adicionou suporte para Funções Definidas pelo Usuário (UDFs) registradas nativamente em SQL a partir do DBR 9.1.
-- MAGIC
-- MAGIC Esse recurso permite que os usuários registrem combinações personalizadas de lógica SQL como funções em um banco de dados, tornando esses métodos reutilizáveis em qualquer lugar onde SQL possa ser executado no Databricks. Essas funções utilizam diretamente o Spark SQL, mantendo todas as otimizações do Spark ao aplicar sua lógica personalizada a grandes conjuntos de dados.
-- MAGIC
-- MAGIC Neste notebook, teremos primeiro uma introdução simples a esses métodos e, em seguida, exploraremos como essa lógica pode ser combinada com cláusulas **`CASE`** / **`WHEN`** para fornecer lógica personalizada de fluxo de controle reutilizável.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Definir e registrar UDFs em SQL
-- MAGIC * Descrever o modelo de segurança usado para compartilhamento de UDFs em SQL
-- MAGIC * Usar instruções **`CASE`** / **`WHEN`** em código SQL
-- MAGIC * Aplicar instruções **`CASE`** / **`WHEN`** em UDFs SQL para controle de fluxo personalizado
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Configuração
-- MAGIC
-- MAGIC Execute a célula a seguir para configurar seu ambiente.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar um Conjunto de Dados Simples
-- MAGIC
-- MAGIC Para este notebook, vamos considerar o seguinte conjunto de dados, registrado aqui como uma visualização temporária.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW foods(food) AS VALUES
("beef"),
("beans"),
("potatoes"),
("bread");

SELECT * FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## UDFs em SQL
-- MAGIC
-- MAGIC No mínimo, uma UDF em SQL exige um nome de função, parâmetros opcionais, o tipo a ser retornado e alguma lógica personalizada.
-- MAGIC
-- MAGIC Abaixo, uma função simples chamada **`yelling`** recebe um parâmetro chamado **`text`**. Ela retorna uma string em letras maiúsculas com três pontos de exclamação adicionados ao final.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe que essa função é aplicada a todos os valores da coluna de forma paralela dentro do mecanismo de processamento do Spark. UDFs em SQL são uma maneira eficiente de definir lógica personalizada otimizada para execução no Databricks.
-- MAGIC

-- COMMAND ----------

SELECT yelling(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Escopo e Permissões de UDFs SQL
-- MAGIC
-- MAGIC Observe que as UDFs SQL persistem entre ambientes de execução (que podem incluir notebooks, consultas do DBSQL e jobs).
-- MAGIC
-- MAGIC Podemos descrever a função para ver onde ela foi registrada e informações básicas sobre os parâmetros esperados e o que ela retorna.
-- MAGIC

-- COMMAND ----------

DESCRIBE FUNCTION yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Descrevendo com o modo estendido (**`DESCRIBE FUNCTION EXTENDED`**), podemos obter ainda mais informações.
-- MAGIC
-- MAGIC Observe que o campo **`Body`** na parte inferior da descrição da função mostra a lógica SQL usada dentro da função.
-- MAGIC

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC As UDFs SQL existem como objetos no metastore e são governadas pelas mesmas ACLs de Tabelas que bancos de dados, tabelas ou visualizações.
-- MAGIC
-- MAGIC Para usar uma UDF SQL, o usuário deve ter permissões de **`USAGE`** e **`SELECT`** na função.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## CASE/WHEN
-- MAGIC
-- MAGIC A construção sintática padrão **`CASE`** / **`WHEN`** em SQL permite a avaliação de múltiplas condições com resultados alternativos com base no conteúdo das tabelas.
-- MAGIC
-- MAGIC Mais uma vez, tudo é avaliado nativamente no Spark, e, portanto, é otimizado para execução paralela.
-- MAGIC

-- COMMAND ----------

SELECT *,
  CASE 
    WHEN food = "beans" THEN "I love beans"
    WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
    WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
    ELSE concat("I don't eat ", food)
  END
FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Funções de Fluxo de Controle Simples
-- MAGIC
-- MAGIC Combinar UDFs SQL com fluxo de controle na forma de cláusulas **`CASE`** / **`WHEN`** fornece uma execução otimizada para fluxos de controle dentro de workloads SQL.
-- MAGIC
-- MAGIC Aqui, demonstramos como encapsular a lógica anterior em uma função que será reutilizável em qualquer lugar onde possamos executar SQL.
-- MAGIC

-- COMMAND ----------

CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Usar esse método em nossos dados fornece o resultado desejado.
-- MAGIC

-- COMMAND ----------

SELECT foods_i_like(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Embora o exemplo fornecido aqui use métodos simples com strings, esses mesmos princípios básicos podem ser usados para adicionar cálculos e lógica personalizados com execução nativa no Spark SQL.
-- MAGIC
-- MAGIC Especialmente para empresas que possam estar migrando usuários de sistemas com muitos procedimentos definidos ou fórmulas personalizadas, as UDFs SQL permitem que um pequeno grupo de usuários defina a lógica complexa necessária para consultas analíticas e de relatórios comuns.
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

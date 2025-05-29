-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Gerenciando Delta Tables
-- MAGIC
-- MAGIC Se você conhece algum tipo de SQL, já possui grande parte do conhecimento necessário para trabalhar de forma eficaz no data lakehouse.
-- MAGIC
-- MAGIC Neste notebook, exploraremos manipulação básica de dados e tabelas com SQL no Databricks.
-- MAGIC
-- MAGIC Observe que o Delta Lake é o formato padrão para todas as tabelas criadas no Databricks; se você já tem executado comandos SQL no Databricks, provavelmente já está trabalhando com Delta Lake.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC Ao final desta aula, você deverá ser capaz de:
-- MAGIC * Criar tabelas Delta Lake
-- MAGIC * Consultar dados de tabelas Delta Lake
-- MAGIC * Inserir, atualizar e deletar registros em tabelas Delta Lake
-- MAGIC * Escrever comandos de upsert com Delta Lake
-- MAGIC * Excluir tabelas Delta Lake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração
-- MAGIC A primeira coisa que vamos fazer é executar um script de configuração. Ele definirá um nome de usuário, diretório pessoal (userhome) e banco de dados que são específicos para cada usuário.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criando uma Tabela Delta
-- MAGIC
-- MAGIC Não é necessário escrever muito código para criar uma tabela com Delta Lake. Existem várias formas de criar tabelas Delta Lake que veremos ao longo do curso. Começaremos com um dos métodos mais simples: registrar uma tabela Delta Lake vazia.
-- MAGIC
-- MAGIC Precisamos de:  
-- MAGIC - Uma instrução **`CREATE TABLE`**  
-- MAGIC - Um nome para a tabela (abaixo usamos **`students`**)  
-- MAGIC - Um esquema (schema)  
-- MAGIC

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Se tentarmos voltar e executar essa célula novamente... ela gerará um erro! Isso é esperado — porque a tabela já existe, recebemos um erro.
-- MAGIC
-- MAGIC Podemos adicionar um argumento adicional, **`IF NOT EXISTS`**, que verifica se a tabela existe. Isso evitará o erro.
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Inserindo Dados
-- MAGIC
-- MAGIC Na maioria das vezes, os dados serão inseridos em tabelas como resultado de uma consulta de outra fonte.
-- MAGIC
-- MAGIC No entanto, assim como no SQL padrão, você também pode inserir valores diretamente, como mostrado aqui.
-- MAGIC

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Na célula acima, completamos três instruções separadas de **`INSERT`**. Cada uma delas é processada como uma transação separada com suas próprias garantias ACID. Na maioria das vezes, inseriremos muitos registros em uma única transação.
-- MAGIC

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que o Databricks não possui a palavra-chave **`COMMIT`**; as transações são executadas assim que são iniciadas e são confirmadas conforme são concluídas com sucesso.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Consultando uma Tabela Delta
-- MAGIC
-- MAGIC Provavelmente não será surpresa que consultar uma tabela Delta Lake seja tão fácil quanto usar uma instrução padrão **`SELECT`**.
-- MAGIC

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O que pode te surpreender é que o Delta Lake garante que qualquer leitura feita em uma tabela **sempre** retornará a versão mais recente da tabela, e que você nunca encontrará um estado de deadlock devido a operações em andamento.
-- MAGIC
-- MAGIC Para reforçar: leituras em tabelas nunca entram em conflito com outras operações, e a versão mais nova dos seus dados está imediatamente disponível para todos os clientes que podem consultar seu lakehouse. Como todas as informações de transação são armazenadas no armazenamento de objetos na nuvem junto com seus arquivos de dados, as leituras concorrentes em tabelas Delta Lake são limitadas apenas pelos limites rígidos do armazenamento de objetos dos provedores de nuvem. (**OBS**: não é infinito, mas são pelo menos milhares de leituras por segundo.)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Atualizando Registros
-- MAGIC
-- MAGIC Atualizar registros também oferece garantias atômicas: realizamos uma leitura instantânea (snapshot) da versão atual da tabela, encontramos todos os campos que correspondem à cláusula **`WHERE`** e então aplicamos as alterações conforme descrito.
-- MAGIC
-- MAGIC Abaixo, encontramos todos os estudantes cujo nome começa com a letra **T** e adicionamos 1 ao valor da coluna **`value`**.
-- MAGIC

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Consulte a tabela novamente para ver essas alterações aplicadas.
-- MAGIC

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Deletando Registros
-- MAGIC
-- MAGIC As deleções também são atômicas, então não há risco de falha parcial ao remover dados do seu data lakehouse.
-- MAGIC
-- MAGIC Uma instrução **`DELETE`** pode remover um ou vários registros, mas sempre resultará em uma única transação.
-- MAGIC

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Usando Merge
-- MAGIC
-- MAGIC Alguns sistemas SQL possuem o conceito de upsert, que permite executar atualizações, inserções e outras manipulações de dados como um único comando.
-- MAGIC
-- MAGIC O Databricks utiliza a palavra-chave **`MERGE`** para realizar essa operação.
-- MAGIC
-- MAGIC Considere a seguinte view temporária, que contém 4 registros que podem ser provenientes de um feed de Change Data Capture (CDC).
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Usando a sintaxe que vimos até agora, poderíamos filtrar essa view pelo campo tipo para escrever 3 comandos separados, um para inserir, outro para atualizar e outro para deletar registros. Mas isso resultaria em 3 transações distintas; se alguma dessas transações falhasse, poderia deixar nossos dados em um estado inválido.
-- MAGIC
-- MAGIC Em vez disso, combinamos essas ações em uma única transação atômica, aplicando os 3 tipos de alterações juntos.
-- MAGIC
-- MAGIC Comandos **`MERGE`** devem ter pelo menos um campo para fazer o match, e cada cláusula **`WHEN MATCHED`** ou **`WHEN NOT MATCHED`** pode conter qualquer número de condições adicionais.
-- MAGIC
-- MAGIC Aqui, fazemos o match pelo campo **`id`** e então filtramos pelo campo **`type`** para atualizar, deletar ou inserir os registros conforme apropriado.
-- MAGIC

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que apenas 3 registros foram impactados pela nossa instrução **`MERGE`**; um dos registros na tabela de atualizações não tinha um **`id`** correspondente na tabela de estudantes, mas estava marcado como um **`update`**. Baseados na nossa lógica personalizada, ignoramos esse registro em vez de inseri-lo.
-- MAGIC
-- MAGIC Como você modificaria a instrução acima para incluir registros não correspondentes marcados como **`update`** na cláusula final de **`INSERT`**?
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Excluindo uma Tabela
-- MAGIC
-- MAGIC Assumindo que você tenha as permissões adequadas na tabela alvo, você pode deletar permanentemente dados no lakehouse usando o comando **`DROP TABLE`**.
-- MAGIC
-- MAGIC **OBS**: Mais adiante no curso, discutiremos Listas de Controle de Acesso (ACLs) para tabelas e permissões padrão. Em um lakehouse devidamente configurado, os usuários **não** devem conseguir deletar tabelas de produção.
-- MAGIC

-- COMMAND ----------

DROP TABLE students

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

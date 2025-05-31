-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Recursos Avançados do Delta Lake
-- MAGIC
-- MAGIC Agora que você está confortável em executar tarefas básicas de dados com o Delta Lake, podemos discutir alguns recursos únicos do Delta Lake.
-- MAGIC
-- MAGIC Observe que, embora algumas das palavras-chave usadas aqui não façam parte do SQL ANSI padrão, todas as operações do Delta Lake podem ser executadas no Databricks usando SQL.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Usar **`OPTIMIZE`** para compactar arquivos pequenos
-- MAGIC * Usar **`ZORDER`** para indexar tabelas
-- MAGIC * Descrever a estrutura de diretórios dos arquivos do Delta Lake
-- MAGIC * Revisar o histórico de transações da tabela
-- MAGIC * Consultar e reverter para uma versão anterior da tabela
-- MAGIC * Limpar arquivos de dados obsoletos com **`VACUUM`**
-- MAGIC
-- MAGIC **Recursos**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Documentação Databricks</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Documentação Databricks</a>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração  
-- MAGIC A primeira coisa que faremos é executar um script de configuração. Ele definirá um nome de usuário, diretório pessoal e banco de dados com escopo para cada usuário.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criando uma Tabela Delta com Histórico
-- MAGIC
-- MAGIC A célula abaixo condensa todas as transações da lição anterior em uma única célula. (Exceto pelo **`DROP TABLE`**!)
-- MAGIC
-- MAGIC Enquanto espera essa consulta rodar, veja se consegue identificar o número total de transações sendo executadas.
-- MAGIC

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Examinar Detalhes da Tabela
-- MAGIC
-- MAGIC O Databricks usa por padrão um metastore Hive para registrar bancos de dados, tabelas e views.
-- MAGIC
-- MAGIC Usar **`DESCRIBE EXTENDED`** nos permite ver metadados importantes sobre nossa tabela.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** é outro comando que nos permite explorar os metadados da tabela.
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe o campo **`Location`**.
-- MAGIC
-- MAGIC Embora até agora tenhamos pensado em nossa tabela apenas como uma entidade relacional dentro de um banco de dados, uma tabela Delta Lake é na verdade suportada por uma coleção de arquivos armazenados em um armazenamento de objetos na nuvem.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Explorar os Arquivos do Delta Lake
-- MAGIC
-- MAGIC Podemos ver os arquivos que suportam nossa tabela Delta Lake usando uma função das Utilitárias do Databricks.
-- MAGIC
-- MAGIC **NOTA**: Não é importante agora entender tudo sobre esses arquivos para trabalhar com Delta Lake, mas isso ajudará você a ter uma maior apreciação de como a tecnologia é implementada.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que nosso diretório contém vários arquivos Parquet e um diretório chamado **`_delta_log`**.
-- MAGIC
-- MAGIC Os registros nas tabelas Delta Lake são armazenados como dados em arquivos Parquet.
-- MAGIC
-- MAGIC As transações nas tabelas Delta Lake são registradas no **`_delta_log`**.
-- MAGIC
-- MAGIC Podemos dar uma olhada dentro do **`_delta_log`** para ver mais detalhes.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Cada transação resulta em um novo arquivo JSON sendo escrito no log de transações do Delta Lake. Aqui, podemos ver que há 8 transações totais nessa tabela (Delta Lake é indexado a partir de 0).
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Raciocinando sobre os Arquivos de Dados
-- MAGIC
-- MAGIC Acabamos de ver muitos arquivos de dados para uma tabela obviamente muito pequena.
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** nos permite ver alguns outros detalhes sobre nossa tabela Delta, incluindo o número de arquivos.
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Aqui vemos que nossa tabela atualmente contém 4 arquivos de dados em sua versão atual. Então, o que estão fazendo todos esses outros arquivos Parquet no diretório da tabela?
-- MAGIC
-- MAGIC Em vez de sobrescrever ou excluir imediatamente os arquivos que contêm dados alterados, o Delta Lake usa o log de transações para indicar se os arquivos são válidos ou não na versão atual da tabela.
-- MAGIC
-- MAGIC Aqui, vamos analisar o log de transações correspondente à instrução **`MERGE`** acima, onde registros foram inseridos, atualizados e excluídos.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A coluna **`add`** contém uma lista de todos os novos arquivos escritos em nossa tabela; a coluna **`remove`** indica os arquivos que não devem mais ser incluídos em nossa tabela.
-- MAGIC
-- MAGIC Quando consultamos uma tabela Delta Lake, o mecanismo de consulta usa os logs de transações para resolver todos os arquivos válidos na versão atual e ignora todos os demais arquivos de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Compactação de Arquivos Pequenos e Indexação
-- MAGIC
-- MAGIC Arquivos pequenos podem ocorrer por várias razões; no nosso caso, realizamos diversas operações onde apenas um ou alguns registros foram inseridos.
-- MAGIC
-- MAGIC Os arquivos serão combinados para um tamanho ideal (escalonado com base no tamanho da tabela) usando o comando **`OPTIMIZE`**.
-- MAGIC
-- MAGIC O comando **`OPTIMIZE`** substituirá os arquivos de dados existentes combinando registros e reescrevendo os resultados.
-- MAGIC
-- MAGIC Ao executar **`OPTIMIZE`**, os usuários podem opcionalmente especificar um ou vários campos para indexação **`ZORDER`**. Embora a matemática específica do Z-order não seja importante, ele acelera a recuperação de dados ao filtrar pelos campos fornecidos, colocalizando dados com valores similares dentro dos arquivos.
-- MAGIC

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Dado o quão pequenos nossos dados são, o **`ZORDER`** não oferece nenhum benefício, mas podemos ver todas as métricas que resultam dessa operação.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Revisando Transações do Delta Lake
-- MAGIC
-- MAGIC Como todas as mudanças na tabela Delta Lake são armazenadas no log de transações, podemos facilmente revisar o <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">histórico da tabela</a>.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como esperado, o **`OPTIMIZE`** criou outra versão da nossa tabela, o que significa que a versão 8 é a nossa versão mais atual.
-- MAGIC
-- MAGIC Lembra de todos aqueles arquivos extras de dados que foram marcados como removidos no nosso log de transações? Eles nos dão a capacidade de consultar versões anteriores da nossa tabela.
-- MAGIC
-- MAGIC Essas consultas de viagem no tempo podem ser feitas especificando-se o número inteiro da versão ou um timestamp.
-- MAGIC
-- MAGIC **NOTA**: Na maioria dos casos, você usará um timestamp para recriar dados em um momento de interesse. Para nossa demonstração, usaremos a versão, pois isso é determinístico (enquanto você pode estar executando essa demonstração a qualquer momento no futuro).
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O que é importante notar sobre a viagem no tempo é que não estamos recriando um estado anterior da tabela desfazendo transações na versão atual; na verdade, estamos apenas consultando todos aqueles arquivos de dados que foram indicados como válidos na versão especificada.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Reverter Versões
-- MAGIC
-- MAGIC Suponha que você esteja digitando uma consulta para deletar manualmente alguns registros de uma tabela e acidentalmente execute essa consulta no estado a seguir.
-- MAGIC

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que quando vemos um **`-1`** para o número de linhas afetadas por um delete, isso significa que um diretório inteiro de dados foi removido.
-- MAGIC
-- MAGIC Vamos confirmar isso abaixo.
-- MAGIC

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Deletar todos os registros da sua tabela provavelmente não é um resultado desejado. Felizmente, podemos simplesmente reverter esse commit.
-- MAGIC

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note que um comando **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">é registrado como uma transação</a>; você não poderá esconder completamente o fato de que deletou todos os registros da tabela acidentalmente, mas poderá desfazer a operação e restaurar a tabela para um estado desejado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limpando Arquivos Obsoletos
-- MAGIC
-- MAGIC O Databricks irá automaticamente limpar arquivos obsoletos em tabelas Delta Lake.
-- MAGIC
-- MAGIC Embora o versionamento e a viagem no tempo do Delta Lake sejam ótimos para consultar versões recentes e reverter consultas, manter os arquivos de dados de todas as versões de tabelas grandes de produção indefinidamente é muito caro (e pode levar a problemas de conformidade caso haja dados pessoais — PII).
-- MAGIC
-- MAGIC Se você desejar apagar manualmente arquivos antigos, isso pode ser feito com a operação **`VACUUM`**.
-- MAGIC
-- MAGIC Descomente a célula abaixo e execute-a com uma retenção de **`0 HOURS`** para manter somente a versão atual:
-- MAGIC

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Por padrão, o **`VACUUM`** impede que você exclua arquivos com menos de 7 dias para garantir que nenhuma operação de longa duração esteja referenciando arquivos que serão deletados. Se você rodar **`VACUUM`** em uma tabela Delta, perderá a capacidade de fazer viagem no tempo para versões anteriores ao período de retenção de dados especificado. Nas nossas demonstrações, você pode ver o Databricks executando código com retenção de **`0 HOURS`**. Isso serve apenas para demonstrar a funcionalidade e normalmente não é usado em produção.
-- MAGIC
-- MAGIC Na célula seguinte, nós:
-- MAGIC 1. Desligamos uma checagem que impede a exclusão prematura de arquivos
-- MAGIC 2. Garantimos que o registro dos comandos **`VACUUM`** esteja habilitado
-- MAGIC 3. Usamos a versão **`DRY RUN`** do **`VACUUM`** para imprimir todos os registros que serão deletados
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Ao executar o **`VACUUM`** e deletar os 10 arquivos acima, removeremos permanentemente o acesso às versões da tabela que precisam desses arquivos para serem materializadas.
-- MAGIC

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Verifique o diretório da tabela para confirmar que os arquivos foram deletados com sucesso.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

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

-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Versionamento, Otimização e Limpeza (Vacuum) no Delta Lake
-- MAGIC
-- MAGIC Este notebook oferece uma revisão prática de alguns dos recursos mais avançados que o Delta Lake traz para o data lakehouse.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final deste laboratório, você deverá ser capaz de:  
-- MAGIC - Revisar o histórico de uma tabela  
-- MAGIC - Consultar versões anteriores da tabela e reverter a tabela para uma versão específica  
-- MAGIC - Realizar compactação de arquivos e indexação Z-order  
-- MAGIC - Visualizar arquivos marcados para exclusão permanente e executar essas exclusões  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Configuração  
-- MAGIC Execute o script a seguir para configurar as variáveis necessárias e limpar execuções anteriores deste notebook. Note que executar essa célula novamente permitirá reiniciar o laboratório.  
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.4L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Recriar o Histórico da Sua Coleção de Feijões  
-- MAGIC
-- MAGIC Este laboratório retoma de onde o último parou. A célula abaixo condensa todas as operações do último laboratório em uma única célula (exceto pelo comando final **`DROP TABLE`**).  
-- MAGIC
-- MAGIC Para referência rápida, o esquema da tabela **`beans`** criada é:
-- MAGIC
-- MAGIC | Nome do Campo | Tipo do Campo |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Revisar o Histórico da Tabela  
-- MAGIC
-- MAGIC O log de transações do Delta Lake armazena informações sobre cada transação que modifica o conteúdo ou configurações de uma tabela.  
-- MAGIC
-- MAGIC Revise abaixo o histórico da tabela **`beans`**.  
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Se todas as operações anteriores foram concluídas conforme descrito, você deve ver 7 versões da tabela (**NOTA**: o versionamento do Delta Lake começa em 0, então o número máximo da versão será 6).  
-- MAGIC
-- MAGIC As operações devem ser as seguintes:
-- MAGIC
-- MAGIC | versão | operação |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC
-- MAGIC A coluna **`operationsParameters`** permite revisar os predicados usados para updates, deletes e merges. A coluna **`operationMetrics`** indica quantas linhas e arquivos foram adicionados em cada operação.  
-- MAGIC
-- MAGIC Reserve um tempo para analisar o histórico do Delta Lake para entender qual versão da tabela corresponde a cada transação.  
-- MAGIC
-- MAGIC **NOTA**: A coluna **`version`** designa o estado da tabela após a conclusão de uma transação. A coluna **`readVersion`** indica a versão da tabela contra a qual a operação foi executada. Neste demo simples (sem transações concorrentes), essa relação deve sempre incrementar em 1.  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Consultar uma Versão Específica  
-- MAGIC
-- MAGIC Após revisar o histórico da tabela, você decide visualizar o estado da sua tabela logo após o primeiro dado ter sido inserido.  
-- MAGIC
-- MAGIC Execute a consulta abaixo para visualizar isso.  
-- MAGIC

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC E agora revise o estado atual dos seus dados.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Você quer revisar o peso dos seus feijões antes de deletar qualquer registro.
-- MAGIC
-- MAGIC Complete a instrução abaixo para registrar uma view temporária da versão logo antes dos dados terem sido deletados, e depois execute a célula seguinte para consultar essa view.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
<FILL-IN>

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar se você capturou a versão correta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Restaurar uma Versão Anterior
-- MAGIC
-- MAGIC Aparentemente houve um mal-entendido; os feijões que seu amigo lhe deu e que você fez merge na sua coleção não eram para você ficar.
-- MAGIC
-- MAGIC Reverta sua tabela para a versão antes da conclusão da instrução **`MERGE`**.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Revise o histórico da sua tabela. Note que restaurar para uma versão anterior adiciona outra versão da tabela.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Compactação de Arquivos
-- MAGIC
-- MAGIC Analisando as métricas da transação durante sua reversão, você se surpreende com a quantidade de arquivos para uma coleção de dados tão pequena.
-- MAGIC
-- MAGIC Embora o índice em uma tabela deste tamanho provavelmente não melhore a performance, você decide adicionar um índice Z-order no campo **`name`** antecipando que sua coleção de feijões cresça exponencialmente com o tempo.
-- MAGIC
-- MAGIC Use a célula abaixo para realizar a compactação dos arquivos e a indexação Z-order.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Seus dados devem ter sido compactados em um único arquivo; confirme manualmente executando a célula seguinte.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula abaixo para verificar se você otimizou e indexou sua tabela com sucesso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpando Arquivos de Dados Obsoletos
-- MAGIC
-- MAGIC Você sabe que, embora todos os seus dados agora residam em 1 arquivo de dados, os arquivos de dados das versões anteriores da sua tabela ainda estão armazenados junto a ele. Você deseja remover esses arquivos e remover o acesso às versões anteriores da tabela executando **`VACUUM`** na tabela.
-- MAGIC
-- MAGIC Executar **`VACUUM`** realiza uma limpeza de lixo no diretório da tabela. Por padrão, um limite de retenção de 7 dias será aplicado.
-- MAGIC
-- MAGIC A célula abaixo modifica algumas configurações do Spark. O primeiro comando ignora a verificação do limite de retenção para nos permitir demonstrar a remoção permanente dos dados.
-- MAGIC
-- MAGIC **NOTA**: Executar vacuum em uma tabela de produção com um limite de retenção curto pode levar à corrupção de dados e/ou falha em consultas de longa duração. Isso é apenas para fins de demonstração e deve-se ter extremo cuidado ao desabilitar essa configuração.
-- MAGIC
-- MAGIC O segundo comando configura **`spark.databricks.delta.vacuum.logging.enabled`** para **`true`** para garantir que a operação **`VACUUM`** seja registrada no log de transações.
-- MAGIC
-- MAGIC **NOTA**: Devido a pequenas diferenças nos protocolos de armazenamento nas várias nuvens, o registro das operações de **`VACUUM`** não é habilitado por padrão para algumas nuvens a partir da versão DBR 9.1.
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Antes de excluir permanentemente os arquivos de dados, revise-os manualmente usando a opção **`DRY RUN`**.
-- MAGIC

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Todos os arquivos de dados que não fazem parte da versão atual da tabela serão exibidos na prévia acima.
-- MAGIC
-- MAGIC Execute o comando novamente, mas sem **`DRY RUN`**, para excluir permanentemente esses arquivos.
-- MAGIC
-- MAGIC **NOTA**: Todas as versões anteriores da tabela não estarão mais acessíveis.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Como o comando **`VACUUM`** pode ser uma ação destrutiva para conjuntos de dados importantes, é sempre uma boa prática reativar a verificação do tempo de retenção. Execute a célula abaixo para reativar essa configuração.
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que o histórico da tabela indicará o usuário que executou a operação **`VACUUM`**, o número de arquivos deletados e registrará que a verificação do tempo de retenção estava desabilitada durante essa operação.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Consulte sua tabela novamente para confirmar que você ainda tem acesso à versão atual.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Porque o Delta Cache armazena cópias dos arquivos consultados na sessão atual em volumes de armazenamento alocados no cluster ativo, você pode temporariamente ainda conseguir acessar versões anteriores da tabela (embora sistemas **não devam** ser projetados esperando esse comportamento).
-- MAGIC
-- MAGIC Reiniciar o cluster garantirá que esses arquivos em cache sejam permanentemente removidos.
-- MAGIC
-- MAGIC Você pode ver um exemplo disso descomentando e executando a célula seguinte, que pode falhar ou não (dependendo do estado do cache).
-- MAGIC

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ao concluir este laboratório, você deverá se sentir confortável para:
-- MAGIC * Completar comandos padrão de criação e manipulação de tabelas Delta Lake
-- MAGIC * Revisar metadados da tabela, incluindo o histórico da tabela
-- MAGIC * Aproveitar o versionamento do Delta Lake para consultas snapshot e rollback
-- MAGIC * Compactar pequenos arquivos e indexar tabelas
-- MAGIC * Usar **`VACUUM`** para revisar arquivos marcados para exclusão e confirmar essas exclusões
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula seguinte para deletar as tabelas e arquivos associados a esta lição.

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

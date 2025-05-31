-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Fornecendo Opções para Fontes Externas  
-- MAGIC Enquanto consultar arquivos diretamente funciona bem para formatos auto-descritivos, muitas fontes de dados exigem configurações adicionais ou declaração de esquema para ingerir corretamente os registros.
-- MAGIC
-- MAGIC Nesta lição, criaremos tabelas usando fontes de dados externas. Embora essas tabelas ainda não sejam armazenadas no formato Delta Lake (e, portanto, não sejam otimizadas para o Lakehouse), essa técnica ajuda a facilitar a extração de dados de sistemas externos diversos.
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem  
-- MAGIC Ao final desta lição, você deverá ser capaz de:  
-- MAGIC - Usar Spark SQL para configurar opções para extração de dados de fontes externas  
-- MAGIC - Criar tabelas em fontes de dados externas para vários formatos de arquivo  
-- MAGIC - Descrever o comportamento padrão ao consultar tabelas definidas contra fontes externas  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração  
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para o restante deste notebook executar.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Quando Consultas Diretas Não Funcionam  
-- MAGIC Embora views possam ser usadas para persistir consultas diretas a arquivos entre sessões, essa abordagem tem utilidade limitada.
-- MAGIC
-- MAGIC Arquivos CSV são um dos formatos de arquivo mais comuns, mas uma consulta direta contra esses arquivos raramente retorna os resultados desejados.
-- MAGIC

-- COMMAND ----------

SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Podemos observar que:  
-- MAGIC 1. A linha de cabeçalho está sendo extraída como uma linha da tabela  
-- MAGIC 2. Todas as colunas estão sendo carregadas como uma única coluna  
-- MAGIC 3. O arquivo é delimitado por pipe (**`|`**)  
-- MAGIC 4. A última coluna parece conter dados aninhados que estão sendo truncados  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Registrando Tabelas em Dados Externos com Opções de Leitura  
-- MAGIC Enquanto o Spark extrai algumas fontes de dados auto-descritivas eficientemente usando configurações padrão, muitos formatos exigem declaração de esquema ou outras opções.
-- MAGIC
-- MAGIC Embora existam muitas <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">configurações adicionais</a> que podem ser definidas ao criar tabelas em fontes externas, a sintaxe abaixo demonstra o essencial para extrair dados da maioria dos formatos.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE identificador_da_tabela (nome_col1 tipo_col1, ...)<br/>
-- MAGIC USING fonte_de_dados<br/>
-- MAGIC OPTIONS (chave1 = valor1, chave2 = valor2, ...)<br/>
-- MAGIC LOCATION = caminho<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Observe que as opções são passadas com as chaves sem aspas e os valores entre aspas. O Spark suporta muitas <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">fontes de dados</a> com opções customizadas, e sistemas adicionais podem ter suporte não oficial via <a href="https://docs.databricks.com/libraries/index.html" target="_blank">bibliotecas externas</a>.
-- MAGIC
-- MAGIC **NOTA**: Dependendo das configurações do seu workspace, você pode precisar de assistência do administrador para carregar bibliotecas e configurar as permissões de segurança necessárias para algumas fontes de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC A célula abaixo demonstra o uso do DDL do Spark SQL para criar uma tabela contra uma fonte CSV externa, especificando:  
-- MAGIC 1. Os nomes e tipos das colunas  
-- MAGIC 2. O formato do arquivo  
-- MAGIC 3. O delimitador usado para separar os campos  
-- MAGIC 4. A presença do cabeçalho  
-- MAGIC 5. O caminho onde esses dados estão armazenados  
-- MAGIC

-- COMMAND ----------

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que nenhum dado foi movido durante a declaração da tabela. Semelhante ao que fizemos ao consultar diretamente os arquivos e criar uma view, estamos apenas apontando para arquivos armazenados em um local externo.
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que os dados estão sendo carregados corretamente.
-- MAGIC

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Todos os metadados e opções passados durante a declaração da tabela serão persistidos no metastore, garantindo que os dados na localização sejam sempre lidos com essas opções.
-- MAGIC
-- MAGIC **NOTA**: Ao trabalhar com CSVs como fonte de dados, é importante garantir que a ordem das colunas não mude se arquivos adicionais forem adicionados ao diretório fonte. Por não ter forte aplicação de esquema, o Spark carregará as colunas e aplicará nomes e tipos na ordem especificada durante a declaração da tabela.
-- MAGIC
-- MAGIC Executar **`DESCRIBE EXTENDED`** em uma tabela mostrará todos os metadados associados à definição da tabela.
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitações das Tabelas com Fontes Externas  
-- MAGIC Se você já fez outros cursos da Databricks ou revisou alguma literatura da empresa, talvez tenha ouvido falar do Delta Lake e do Lakehouse. Note que sempre que definimos tabelas ou consultas contra fontes externas, **não podemos** esperar as garantias de desempenho associadas ao Delta Lake e Lakehouse.
-- MAGIC
-- MAGIC Por exemplo: enquanto tabelas Delta Lake garantem que você sempre consulte a versão mais recente dos seus dados, tabelas registradas contra outras fontes podem representar versões antigas em cache.
-- MAGIC
-- MAGIC A célula abaixo executa uma lógica que podemos pensar como representando um sistema externo atualizando diretamente os arquivos subjacentes à nossa tabela.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("sales_csv")
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(f"{DA.paths.working_dir}/sales-csv"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Se olharmos para a contagem atual de registros em nossa tabela, o número que vemos não refletirá essas novas linhas inseridas.
-- MAGIC

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC No momento em que consultamos essa fonte de dados anteriormente, o Spark armazenou automaticamente em cache os dados subjacentes no armazenamento local. Isso garante que em consultas subsequentes, o Spark ofereça desempenho otimizado consultando apenas esse cache local.
-- MAGIC
-- MAGIC Nossa fonte de dados externa não está configurada para informar ao Spark que deve atualizar esses dados.
-- MAGIC
-- MAGIC Nós **podemos** atualizar manualmente o cache dos nossos dados executando o comando **`REFRESH TABLE`**.
-- MAGIC

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe que atualizar nossa tabela invalidará nosso cache, significando que precisaremos reexaminar nossa fonte de dados original e recarregar todos os dados para a memória.
-- MAGIC
-- MAGIC Para conjuntos de dados muito grandes, isso pode levar um tempo significativo.
-- MAGIC

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Extraindo Dados de Bancos de Dados SQL  
-- MAGIC Bancos de dados SQL são uma fonte de dados extremamente comum, e o Databricks possui um driver JDBC padrão para conexão com várias versões de SQL.
-- MAGIC
-- MAGIC A sintaxe geral para criar essas conexões é:
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{tipoDoServidorBanco}://{hostJdbc}:{portaJdbc}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{bancoJdbc}.tabela",<br/>
-- MAGIC &nbsp; &nbsp; user = "{usuarioJdbc}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{senhaJdbc}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC No exemplo de código abaixo, vamos conectar com <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>.
-- MAGIC
-- MAGIC **NOTA:** O SQLite usa um arquivo local para armazenar um banco de dados e não requer porta, usuário ou senha.
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Agora podemos consultar essa tabela como se ela estivesse definida localmente.
-- MAGIC

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Analisar os metadados da tabela revela que capturamos a informação do esquema do sistema externo. Propriedades de armazenamento (que incluiriam o nome de usuário e senha da conexão) são automaticamente ocultadas.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Embora a tabela esteja listada como **`MANAGED`**, listar o conteúdo da localização especificada confirma que nenhum dado está sendo persistido localmente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbc_users_path = f"{DA.paths.user_db}/users_jdbc/"
-- MAGIC print(jdbc_users_path)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(jdbc_users_path)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Note que alguns sistemas SQL, como data warehouses, terão drivers personalizados. O Spark interage com vários bancos de dados externos de forma diferente, mas as duas abordagens básicas podem ser resumidas como:
-- MAGIC
-- MAGIC 1. Mover a tabela(s) fonte inteira para o Databricks e então executar a lógica no cluster ativo  
-- MAGIC 2. Empurrar a consulta para o banco de dados SQL externo e transferir somente os resultados de volta ao Databricks
-- MAGIC
-- MAGIC Em ambos os casos, trabalhar com conjuntos de dados muito grandes em bancos SQL externos pode causar sobrecarga significativa devido a:
-- MAGIC
-- MAGIC 1. Latência de transferência pela rede associada ao movimento de todos os dados pela internet pública  
-- MAGIC 2. Execução da lógica da consulta em sistemas fonte que não são otimizados para consultas de big data
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

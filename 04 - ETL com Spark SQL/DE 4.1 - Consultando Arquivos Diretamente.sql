-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Extraindo Dados Diretamente de Arquivos
-- MAGIC
-- MAGIC Neste notebook, você aprenderá a extrair dados diretamente de arquivos usando Spark SQL no Databricks.
-- MAGIC
-- MAGIC Diversos formatos de arquivo suportam essa opção, mas é mais útil para formatos de dados auto-descritivos (como parquet e JSON).
-- MAGIC
-- MAGIC ## Objetivos de Aprendizagem
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Usar Spark SQL para consultar diretamente arquivos de dados
-- MAGIC - Utilizar os métodos **`text`** e **`binaryFile`** para revisar conteúdos brutos dos arquivos
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Executar Configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Visão Geral dos Dados
-- MAGIC
-- MAGIC Neste exemplo, vamos trabalhar com uma amostra de dados brutos do Kafka gravados como arquivos JSON.
-- MAGIC
-- MAGIC Cada arquivo contém todos os registros consumidos durante um intervalo de 5 segundos, armazenados com o esquema completo do Kafka como um arquivo JSON de múltiplos registros.
-- MAGIC
-- MAGIC | campo      | tipo    | descrição                                                                                  |
-- MAGIC |------------|---------|--------------------------------------------------------------------------------------------|
-- MAGIC | key        | BINARY  | O campo **`user_id`** é usado como chave; é um campo alfanumérico único que corresponde a informações de sessão/cookie |
-- MAGIC | value      | BINARY  | Este é o payload completo dos dados (será discutido mais tarde), enviado como JSON         |
-- MAGIC | topic      | STRING  | Embora o serviço Kafka hospede múltiplos tópicos, apenas os registros do tópico **`clickstream`** estão incluídos aqui |
-- MAGIC | partition  | INTEGER | A implementação atual do Kafka usa apenas 2 partições (0 e 1)                              |
-- MAGIC | offset     | LONG    | Este é um valor único, monotonicamente crescente para cada partição                        |
-- MAGIC | timestamp  | LONG    | Este timestamp é registrado em milissegundos desde a época Unix, representando o momento em que o produtor adiciona um registro a uma partição |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe que nosso diretório de origem contém muitos arquivos JSON.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = f"{DA.paths.datasets}/raw/events-kafka"
-- MAGIC print(dataset_path)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Aqui, usaremos caminhos relativos para os dados que foram gravados na raiz do DBFS.
-- MAGIC
-- MAGIC A maioria dos fluxos de trabalho exigirá que os usuários acessem dados de locais externos de armazenamento em nuvem.
-- MAGIC
-- MAGIC Na maioria das empresas, um administrador do workspace será responsável por configurar o acesso a esses locais de armazenamento.
-- MAGIC
-- MAGIC Instruções para configurar e acessar esses locais podem ser encontradas nos cursos auto-dirigidos específicos de fornecedores de nuvem intitulados "Arquitetura em Nuvem & Integração de Sistemas".
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Consultar um Único Arquivo
-- MAGIC
-- MAGIC Para consultar os dados contidos em um único arquivo, execute a consulta com o seguinte padrão:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM formato_de_arquivo.`/caminho/para/o/arquivo`</code></strong>
-- MAGIC
-- MAGIC Preste atenção especial ao uso de crases (back-ticks) — não aspas simples — em torno do caminho.
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Observe que nossa pré-visualização exibe todas as 321 linhas do arquivo de origem.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Consultar um Diretório de Arquivos
-- MAGIC
-- MAGIC Assumindo que todos os arquivos em um diretório tenham o mesmo formato e esquema, todos os arquivos podem ser consultados simultaneamente especificando o caminho do diretório em vez de um arquivo individual.
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Por padrão, essa consulta mostrará apenas as primeiras 1000 linhas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Criar Referências para Arquivos
-- MAGIC
-- MAGIC Essa capacidade de consultar arquivos e diretórios diretamente significa que lógica adicional do Spark pode ser encadeada às consultas contra arquivos.
-- MAGIC
-- MAGIC Quando criamos uma view a partir de uma consulta contra um caminho, podemos referenciar essa view em consultas posteriores. Aqui, criaremos uma view temporária, mas você também pode criar uma referência permanente com uma view normal.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;

SELECT * FROM events_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Extrair Arquivos de Texto como Strings Brutas
-- MAGIC
-- MAGIC Ao trabalhar com arquivos baseados em texto (incluindo JSON, CSV, TSV e TXT), você pode usar o formato **`text`** para carregar cada linha do arquivo como uma linha com uma coluna string chamada **`value`**. Isso pode ser útil quando as fontes de dados são propensas à corrupção e funções personalizadas de parsing serão usadas para extrair valores dos campos de texto.
-- MAGIC

-- COMMAND ----------

SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Extrair os Bytes Brutos e Metadados de um Arquivo
-- MAGIC
-- MAGIC Alguns fluxos de trabalho podem requerer o trabalho com arquivos inteiros, como ao lidar com imagens ou dados não estruturados. Usar **`binaryFile`** para consultar um diretório fornecerá metadados do arquivo junto com a representação binária do conteúdo do arquivo.
-- MAGIC
-- MAGIC Especificamente, os campos criados indicarão o **`path`**, **`modificationTime`**, **`length`**, e o **`content`**.
-- MAGIC

-- COMMAND ----------

SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`

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

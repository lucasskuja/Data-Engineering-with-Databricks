# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Ingestão de Dados Incremental com Auto Loader
# MAGIC
# MAGIC ETL incremental é importante pois permite lidar apenas com dados novos que chegaram desde a última ingestão. Processar somente os dados novos de forma confiável reduz o processamento redundante e ajuda empresas a escalarem seus pipelines de dados com segurança.
# MAGIC
# MAGIC O primeiro passo para qualquer implementação bem-sucedida de data lakehouse é ingerir dados do armazenamento em nuvem em uma tabela Delta Lake.
# MAGIC
# MAGIC Historicamente, ingerir arquivos de um data lake para um banco de dados era um processo complicado.
# MAGIC
# MAGIC O Auto Loader do Databricks fornece um mecanismo fácil de usar para processar de forma incremental e eficiente novos arquivos de dados assim que eles chegam no armazenamento de arquivos em nuvem. Neste notebook, você verá o Auto Loader em ação.
# MAGIC
# MAGIC Devido aos benefícios e à escalabilidade que o Auto Loader oferece, o Databricks recomenda seu uso como **melhor prática** geral ao ingerir dados de armazenamento de objetos em nuvem.
# MAGIC
# MAGIC ## Objetivos de Aprendizado
# MAGIC Ao final desta lição, você será capaz de:
# MAGIC * Executar código com Auto Loader para ingerir dados incrementalmente do armazenamento em nuvem para o Delta Lake
# MAGIC * Descrever o que acontece quando um novo arquivo chega em um diretório configurado para o Auto Loader
# MAGIC * Consultar uma tabela alimentada por uma consulta contínua do Auto Loader
# MAGIC
# MAGIC ## Conjunto de Dados Utilizado
# MAGIC Esta demonstração usa dados médicos simplificados e artificialmente gerados, representando registros de batimentos cardíacos no formato JSON.
# MAGIC
# MAGIC | Campo | Tipo |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Primeiros Passos
# MAGIC
# MAGIC Execute a célula a seguir para reiniciar a demonstração, configurar variáveis necessárias e funções auxiliares.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Usando o Auto Loader
# MAGIC
# MAGIC Na célula abaixo, é definida uma função para demonstrar o uso do Auto Loader do Databricks com a API PySpark. Esse código inclui tanto a leitura quanto a escrita com Structured Streaming.
# MAGIC
# MAGIC O próximo notebook fornecerá uma visão mais robusta do Structured Streaming. Se quiser saber mais sobre as opções do Auto Loader, consulte a <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentação</a>.
# MAGIC
# MAGIC Observe que ao usar o Auto Loader com <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">inferência e evolução automática de schema</a>, os 4 argumentos abaixo devem permitir a ingestão da maioria dos conjuntos de dados. Estes argumentos estão explicados a seguir:
# MAGIC
# MAGIC | argumento | o que é | como é usado |
# MAGIC | --- | --- | --- |
# MAGIC | **`data_source`** | Diretório com os dados de origem | O Auto Loader detecta novos arquivos assim que eles chegam neste local e os adiciona à fila para ingestão; passado ao método **`.load()`** |
# MAGIC | **`source_format`** | Formato dos dados de origem | Embora o formato de todas as consultas do Auto Loader seja **`cloudFiles`**, o formato dos dados deve ser especificado na opção **`cloudFiles.format`** |
# MAGIC | **`table_name`** | Nome da tabela de destino | O Structured Streaming do Spark permite escrita direta em tabelas Delta Lake ao passar o nome da tabela como string no método **`.table()`**. É possível criar uma nova tabela ou adicionar dados a uma existente |
# MAGIC | **`checkpoint_directory`** | Local de armazenamento dos metadados do stream | Esse argumento é passado para as opções **`checkpointLocation`** e **`cloudFiles.schemaLocation`**. Checkpoints rastreiam o progresso da stream, enquanto a localização do schema acompanha atualizações nos campos do dataset |
# MAGIC
# MAGIC **NOTA**: O código abaixo foi simplificado para demonstrar a funcionalidade do Auto Loader. Veremos em lições futuras que transformações adicionais podem ser aplicadas aos dados antes de salvá-los no Delta Lake.
# MAGIC

# COMMAND ----------


def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .load(data_source)
        .writeStream.option("checkpointLocation", checkpoint_directory)
        .option("mergeSchema", "true")
        .table(table_name)
    )
    return query


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Na próxima célula, usamos a função definida anteriormente e algumas variáveis de caminho configuradas no script de preparação para iniciar uma stream com Auto Loader.
# MAGIC
# MAGIC Aqui, estamos lendo de um diretório com arquivos JSON.
# MAGIC

# COMMAND ----------

query = autoload_to_table(
    data_source=f"{DA.paths.working_dir}/tracker",
    source_format="json",
    table_name="target_table",
    checkpoint_directory=f"{DA.paths.checkpoints}/target_table",
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Como o Auto Loader usa o Structured Streaming do Spark para carregar dados de forma incremental, o código acima não parece terminar sua execução.
# MAGIC
# MAGIC Podemos pensar nisso como uma **consulta continuamente ativa**. Isso significa que, assim que novos dados chegam à origem, eles são processados pela lógica e carregados na tabela de destino. Vamos explorar isso a seguir.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Função Auxiliar para Lições com Streaming
# MAGIC
# MAGIC Nossas lições em notebooks combinam funções de streaming com consultas batch e de streaming sobre os resultados dessas operações. Esses notebooks são para fins instrucionais e destinados à execução interativa célula por célula. Esse padrão não é recomendado para produção.
# MAGIC
# MAGIC Abaixo, definimos uma função auxiliar que impede que a próxima célula do notebook seja executada imediatamente, garantindo tempo suficiente para os dados serem gravados pela stream. Esse código não é necessário em um job de produção.
# MAGIC

# COMMAND ----------


def block_until_stream_is_ready(query, min_batches=2):
    import time

    while len(query.recentProgress) < min_batches:
        time.sleep(5)  # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")


block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Consultar a Tabela de Destino
# MAGIC
# MAGIC Uma vez que os dados são ingeridos para o Delta Lake com Auto Loader, os usuários podem interagir com eles como fariam com qualquer outra tabela.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM target_table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Observe que a coluna **`_rescued_data`** é adicionada automaticamente pelo Auto Loader para capturar dados malformados que não se encaixam no schema da tabela.
# MAGIC
# MAGIC Embora o Auto Loader tenha capturado corretamente os nomes dos campos, ele codificou todos os campos como tipo **`STRING`**. Como JSON é um formato baseado em texto, esse é o tipo mais seguro e permissivo, garantindo que a menor quantidade possível de dados seja descartada na ingestão devido a incompatibilidade de tipo.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE target_table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use a célula abaixo para definir uma view temporária que resume os registros da tabela de destino.
# MAGIC
# MAGIC Vamos usar essa view para demonstrar como novos dados são automaticamente ingeridos com o Auto Loader.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts AS
# MAGIC   SELECT device_id, count(*) total_recordings
# MAGIC   FROM target_table
# MAGIC   GROUP BY device_id;
# MAGIC
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inserir Novos Dados
# MAGIC
# MAGIC Como mencionado anteriormente, o Auto Loader é configurado para processar arquivos incrementalmente de um diretório no armazenamento de objetos para uma tabela Delta Lake.
# MAGIC
# MAGIC Já configuramos e estamos executando uma consulta para processar arquivos JSON a partir do caminho especificado por **`source_path`** para uma tabela chamada **`target_table`**. Vamos revisar o conteúdo do diretório **`source_path`**.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Neste momento, você deverá ver um único arquivo JSON listado neste local.
# MAGIC
# MAGIC O método na célula abaixo foi configurado no script de preparação para modelar um sistema externo gravando dados nesse diretório. Cada vez que você executar a célula abaixo, um novo arquivo será adicionado ao diretório **`source_path`**.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Liste novamente o conteúdo do **`source_path`** usando a célula abaixo. Você deverá ver um arquivo JSON adicional para cada vez que executou a célula anterior.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Acompanhando o Progresso da Ingestão
# MAGIC
# MAGIC Historicamente, muitos sistemas eram configurados para reprocessar todos os registros em um diretório de origem para calcular os resultados atuais ou exigiam lógica customizada para identificar novos dados desde a última atualização da tabela.
# MAGIC
# MAGIC Com o Auto Loader, sua tabela já foi atualizada.
# MAGIC
# MAGIC Execute a consulta abaixo para confirmar que novos dados foram ingeridos.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A consulta com Auto Loader que configuramos anteriormente detecta e processa automaticamente os registros do diretório de origem para a tabela de destino. Há um pequeno atraso na ingestão dos registros, mas uma consulta com Auto Loader usando a configuração padrão de streaming deve atualizar os resultados em quase tempo real.
# MAGIC
# MAGIC A consulta abaixo mostra o histórico da tabela. Uma nova versão da tabela deve ser indicada para cada **`STREAMING UPDATE`**. Esses eventos de atualização coincidem com novos lotes de dados chegando à origem.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpeza
# MAGIC
# MAGIC Sinta-se à vontade para continuar inserindo novos dados e explorando os resultados na tabela com as células acima.
# MAGIC
# MAGIC Quando terminar, execute a célula abaixo para interromper todos os streams ativos e remover os recursos criados antes de continuar.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Multi-Hop Incremental no Lakehouse
# MAGIC
# MAGIC Agora que entendemos melhor como trabalhar com processamento incremental de dados combinando APIs de Structured Streaming e Spark SQL, podemos explorar a integração estreita entre Structured Streaming e Delta Lake.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta lição, você será capaz de:
# MAGIC * Descrever as tabelas Bronze, Silver e Gold
# MAGIC * Criar um pipeline multi-hop com Delta Lake
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Atualizações Incrementais no Lakehouse
# MAGIC
# MAGIC Delta Lake permite aos usuários combinar facilmente cargas de trabalho em streaming e batch em um pipeline multi-hop unificado. Cada etapa do pipeline representa um estado dos dados valioso para impulsionar casos de uso essenciais para o negócio. Como todos os dados e metadados estão armazenados em storage na nuvem, vários usuários e aplicações podem acessá-los em tempo quase real, permitindo que analistas visualizem os dados mais atualizados enquanto são processados.
# MAGIC
# MAGIC ![](https://media.licdn.com/dms/image/v2/D5612AQEaJSEd3Qx2Yg/article-cover_image-shrink_720_1280/article-cover_image-shrink_720_1280/0/1708813244075?e=2147483647&v=beta&t=b580qPpxtp6itFDK1lG4TFCDBwcIOfhsrZO5AS3w4-g)
# MAGIC
# MAGIC - Tabelas **Bronze** contêm dados brutos ingeridos de várias fontes (arquivos JSON, dados de bancos relacionais, dados de IoT, entre outros).
# MAGIC
# MAGIC - Tabelas **Silver** oferecem uma visão mais refinada dos dados. Podemos fazer joins entre tabelas bronze para enriquecer registros em streaming, ou atualizar o status de contas com base em atividades recentes.
# MAGIC
# MAGIC - Tabelas **Gold** fornecem agregações de nível de negócio, usadas com frequência para relatórios e dashboards. Isso inclui métricas como usuários ativos diários, vendas semanais por loja ou receita bruta trimestral por departamento.
# MAGIC
# MAGIC Os resultados finais são insights acionáveis, dashboards e relatórios de métricas de negócio.
# MAGIC
# MAGIC Ao considerar a lógica de negócio em todas as etapas do pipeline ETL, garantimos a otimização de custos de armazenamento e computação ao reduzir duplicação desnecessária de dados e limitar consultas ad hoc sobre todo o histórico.
# MAGIC
# MAGIC Cada etapa pode ser configurada como job em batch ou streaming, e as transações ACID garantem sucesso ou falha total da operação.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conjuntos de Dados Utilizados
# MAGIC
# MAGIC Esta demonstração usa dados médicos simplificados e gerados artificialmente. O esquema de nossos dois conjuntos de dados está representado abaixo. Observe que modificaremos esses esquemas ao longo das etapas.
# MAGIC
# MAGIC #### Registros
# MAGIC O conjunto principal usa gravações de batimentos cardíacos coletados por dispositivos médicos no formato JSON.
# MAGIC
# MAGIC | Campo | Tipo |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC
# MAGIC #### PII
# MAGIC Estes dados serão posteriormente combinados com uma tabela estática de informações de pacientes armazenada em um sistema externo para identificar pacientes pelo nome.
# MAGIC
# MAGIC | Campo | Tipo |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Iniciando
# MAGIC
# MAGIC Execute a célula a seguir para configurar o ambiente do laboratório.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Simulador de Dados
# MAGIC
# MAGIC O Auto Loader do Databricks pode processar automaticamente arquivos à medida que eles chegam ao seu storage em nuvem.
# MAGIC
# MAGIC Para simular esse processo, você será solicitado a executar a seguinte operação várias vezes ao longo do curso.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tabela Bronze: Ingestão de Registros JSON Brutos
# MAGIC
# MAGIC Abaixo, configuramos a leitura de uma fonte JSON bruta usando o Auto Loader com inferência de esquema.
# MAGIC
# MAGIC Observe que, embora você precise usar a API DataFrame do Spark para configurar uma leitura incremental, uma vez configurada, é possível registrar uma visualização temporária para aplicar transformações em streaming com Spark SQL.
# MAGIC
# MAGIC **NOTA**: Para fontes JSON, o Auto Loader infere cada coluna como string por padrão. Aqui, mostramos como especificar o tipo da coluna **`time`** usando a opção **`cloudFiles.schemaHints`**. Especificar tipos incorretos resultará em valores nulos.
# MAGIC

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aqui, vamos enriquecer os dados brutos com metadados adicionais, como o nome do arquivo de origem e o horário da ingestão. Esses metadados podem ser ignorados nas etapas seguintes, mas são úteis para depuração em caso de erros nos dados.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O código abaixo envia os dados brutos enriquecidos de volta à API PySpark para realizar uma gravação incremental em uma tabela Delta Lake.
# MAGIC

# COMMAND ----------

(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Dispare a chegada de outro arquivo com a célula a seguir, e você verá as alterações sendo detectadas imediatamente pela consulta de streaming configurada.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Carregar Tabela de Consulta Estática
# MAGIC
# MAGIC As garantias ACID do Delta Lake são aplicadas no nível da tabela, assegurando que apenas commits bem-sucedidos estejam visíveis. Se for necessário mesclar esses dados com outras fontes, verifique como essas fontes versionam dados e os tipos de garantias de consistência que fornecem.
# MAGIC
# MAGIC Neste exemplo simplificado, carregaremos um arquivo CSV estático com dados de pacientes. Em produção, poderíamos usar o <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> para manter uma visão atualizada desses dados no Delta Lake.
# MAGIC

# COMMAND ----------

(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.data_source}/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tabela Silver: Dados de Registros Enriquecidos
# MAGIC
# MAGIC Como segundo estágio na camada silver, faremos os seguintes enriquecimentos e validações:
# MAGIC - Faremos um join dos dados de gravações com a tabela PII para adicionar os nomes dos pacientes
# MAGIC - A coluna `time` será convertida para o formato **`'yyyy-MM-dd HH:mm:ss'`** legível por humanos
# MAGIC - Serão excluídos batimentos cardíacos com valor <= 0, pois isso indica ausência do paciente ou erro na transmissão
# MAGIC

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Dispare a chegada de mais um arquivo e aguarde a propagação por ambas as consultas anteriores.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tabela Gold: Médias Diárias
# MAGIC
# MAGIC Aqui lemos uma stream de **`recordings_enriched`** e escrevemos outra stream para criar uma tabela Delta de agregados com as médias diárias por paciente.
# MAGIC

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Observe que utilizamos **`.trigger(availableNow=True)`** abaixo. Isso nos permite manter os benefícios do structured streaming, acionando esse job apenas uma vez para processar todos os dados disponíveis em micro-batches. Esses benefícios incluem:
# MAGIC - processamento com tolerância a falhas com exatamente uma execução
# MAGIC - detecção automática de alterações nas fontes de dados
# MAGIC
# MAGIC Se soubermos a taxa aproximada de crescimento dos dados, podemos ajustar o tamanho do cluster para garantir eficiência e custo-benefício. O cliente poderá avaliar o custo de atualização dessa visualização agregada final e decidir com que frequência ela deve ser atualizada.
# MAGIC
# MAGIC Processos downstream que consomem essa tabela não precisarão repetir agregações caras. Os arquivos apenas serão desserializados e consultas com filtros adequados poderão ser empurradas até a origem agregada.
# MAGIC

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Considerações Importantes sobre Output `complete` com Delta
# MAGIC
# MAGIC Ao usar o modo de saída **`complete`**, reescrevemos todo o estado da tabela a cada execução. Isso é ideal para calcular agregações, mas **não** podemos ler uma stream a partir desse diretório, pois o Structured Streaming espera apenas acréscimos nos dados upstream.
# MAGIC
# MAGIC **NOTA**: Algumas opções podem ser definidas para alterar esse comportamento, mas com limitações. Para mais detalhes, consulte:  
# MAGIC <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignorando Atualizações e Exclusões</a>.
# MAGIC
# MAGIC A tabela gold registrada fará uma leitura estática do estado atual dos dados a cada execução da consulta.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A tabela acima inclui todos os dias para todos os usuários. Se os filtros de nossas consultas ad hoc forem compatíveis com os dados aqui agregados, conseguimos empurrar essas condições até os arquivos de origem e gerar rapidamente visões agregadas mais restritas.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Processar Registros Restantes
# MAGIC
# MAGIC A célula a seguir adicionará arquivos com os dados restantes de 2020 no diretório de origem. Eles serão processados pelas três primeiras tabelas Delta, mas você precisará reexecutar a última consulta para atualizar a tabela **`daily_patient_avg`**, pois ela usa o gatilho `availableNow`.
# MAGIC

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Finalizando
# MAGIC
# MAGIC Por fim, certifique-se de que todas as streams estejam paradas.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resumo
# MAGIC
# MAGIC Delta Lake e Structured Streaming se combinam para oferecer acesso analítico quase em tempo real aos dados no lakehouse.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tópicos e Recursos Adicionais
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Leituras e Escritas com Streaming em Tabelas Delta</a>  
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Guia de Programação do Structured Streaming</a>  
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">Deep Dive em Structured Streaming</a>, por Tathagata Das  
# MAGIC * <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>  
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Modelos de Data Warehouse</a>  
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Criar uma Fonte Kafka em Streaming</a>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

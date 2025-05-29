# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Raciocinando sobre Dados Incrementais
# MAGIC
# MAGIC O Spark Structured Streaming estende a funcionalidade do Apache Spark para permitir uma configuração simplificada e controle de estado ao processar conjuntos de dados incrementais. No passado, grande parte do foco do streaming com big data foi na redução de latência para fornecer insights analíticos em tempo quase real. Embora o Structured Streaming ofereça desempenho excepcional nesse aspecto, esta lição se concentrará mais nas aplicações do processamento incremental de dados.
# MAGIC
# MAGIC Embora o processamento incremental não seja absolutamente necessário para trabalhar com sucesso no data lakehouse, nossa experiência ajudando algumas das maiores empresas do mundo a obter insights a partir dos maiores conjuntos de dados do mundo levou à conclusão de que muitas cargas de trabalho podem se beneficiar substancialmente de uma abordagem de processamento incremental. Muitos dos recursos principais no coração do Databricks foram otimizados especificamente para lidar com esses conjuntos de dados em constante crescimento.
# MAGIC
# MAGIC Considere os seguintes conjuntos de dados e casos de uso:
# MAGIC * Cientistas de dados precisam de acesso seguro, desidentificado e versionado a registros frequentemente atualizados em um banco de dados operacional
# MAGIC * Transações com cartão de crédito precisam ser comparadas ao comportamento passado do cliente para identificar e sinalizar fraudes
# MAGIC * Uma varejista multinacional busca oferecer recomendações personalizadas de produtos com base no histórico de compras
# MAGIC * Arquivos de log de sistemas distribuídos precisam ser analisados para detectar e responder a instabilidades
# MAGIC * Dados de cliques de milhões de compradores online precisam ser utilizados para testes A/B de UX
# MAGIC
# MAGIC Esses são apenas alguns exemplos de conjuntos de dados que crescem de forma incremental e infinita com o tempo.
# MAGIC
# MAGIC Nesta lição, vamos explorar os fundamentos do trabalho com Spark Structured Streaming para permitir o processamento incremental de dados. Na próxima lição, falaremos mais sobre como esse modelo de processamento incremental simplifica o processamento de dados no data lakehouse.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Descrever o modelo de programação usado pelo Spark Structured Streaming
# MAGIC * Configurar as opções necessárias para realizar uma leitura de streaming de uma fonte
# MAGIC * Descrever os requisitos para tolerância a falhas de ponta a ponta
# MAGIC * Configurar as opções necessárias para realizar uma gravação de streaming em um destino
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Começando
# MAGIC
# MAGIC Execute a célula a seguir para configurar nosso "ambiente de aula".
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tratando Dados Infinitos como uma Tabela
# MAGIC
# MAGIC A mágica por trás do Spark Structured Streaming é que ele permite aos usuários interagir com fontes de dados em constante crescimento como se fossem apenas uma tabela estática de registros.
# MAGIC
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" width="800"/>
# MAGIC
# MAGIC No gráfico acima, um **fluxo de dados** descreve qualquer fonte de dados que cresce com o tempo. Novos dados em um fluxo de dados podem corresponder a:
# MAGIC * Um novo arquivo JSON chegando no armazenamento em nuvem
# MAGIC * Atualizações em um banco de dados capturadas em um feed CDC
# MAGIC * Eventos enfileirados em um serviço de mensagens pub/sub
# MAGIC * Um arquivo CSV de vendas fechadas no dia anterior
# MAGIC
# MAGIC Muitas organizações tradicionalmente adotaram a abordagem de reprocessar todo o conjunto de dados de origem cada vez que querem atualizar os resultados. Outra abordagem seria escrever uma lógica personalizada para capturar apenas os arquivos ou registros que foram adicionados desde a última atualização.
# MAGIC
# MAGIC O Structured Streaming nos permite definir uma consulta contra a fonte de dados e detectar automaticamente novos registros, propagando-os por meio da lógica previamente definida.
# MAGIC
# MAGIC **O Spark Structured Streaming é otimizado no Databricks para integração próxima com Delta Lake e Auto Loader.**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conceitos Básicos
# MAGIC
# MAGIC - O desenvolvedor define uma **tabela de entrada** ao configurar uma leitura de streaming contra uma **fonte**. A sintaxe para isso é semelhante à de trabalho com dados estáticos.
# MAGIC - Uma **consulta** é definida contra a tabela de entrada. As APIs de DataFrames e Spark SQL podem ser usadas para definir facilmente transformações e ações contra a tabela de entrada.
# MAGIC - Essa consulta lógica na tabela de entrada gera a **tabela de resultados**. A tabela de resultados contém o estado incremental do fluxo.
# MAGIC - A **saída** de um pipeline de streaming persistirá as atualizações da tabela de resultados escrevendo para um **destino** externo. Geralmente, esse destino será um sistema durável como arquivos ou um barramento de mensagens pub/sub.
# MAGIC - Novas linhas são adicionadas à tabela de entrada a cada **intervalo de disparo** (*trigger interval*). Essas novas linhas são essencialmente análogas a transações em micro-batch e serão automaticamente propagadas da tabela de resultados ao destino.
# MAGIC
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" width="800"/>
# MAGIC
# MAGIC Para mais informações, consulte a seção análoga no <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts" target="_blank">Guia de Programação do Structured Streaming</a> (de onde várias imagens foram emprestadas).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tolerância a Falhas de Ponta a Ponta
# MAGIC
# MAGIC O Structured Streaming garante tolerância a falhas com exatamente uma vez (*exactly-once*) de ponta a ponta por meio de _checkpointing_ (discutido abaixo) e <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC
# MAGIC Fontes, destinos e o mecanismo de execução subjacente do Structured Streaming trabalham juntos para rastrear o progresso do processamento do stream. Se uma falha ocorrer, o mecanismo de streaming tentará reiniciar e/ou reprocessar os dados.
# MAGIC
# MAGIC Para boas práticas na recuperação de uma consulta de streaming com falha veja <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">a documentação</a>.
# MAGIC
# MAGIC Essa abordagem **só** funciona se a fonte de streaming for reproduzível; fontes reproduzíveis incluem armazenamento em nuvem e serviços de mensagens pub/sub.
# MAGIC
# MAGIC Em alto nível, o mecanismo de streaming subjacente depende de duas abordagens:
# MAGIC
# MAGIC * Primeiro, o Structured Streaming usa checkpointing e write-ahead logs para registrar o intervalo de offset dos dados processados durante cada intervalo de disparo.
# MAGIC * Depois, os destinos de streaming são projetados para serem _idempotentes_ — ou seja, múltiplas gravações dos mesmos dados (identificados pelo offset) **não** resultam em duplicatas no destino.
# MAGIC
# MAGIC Combinando fontes reproduzíveis e destinos idempotentes, o Structured Streaming garante **semântica de exatamente uma vez de ponta a ponta**, mesmo em condições de falha.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lendo um Stream
# MAGIC
# MAGIC O método **`spark.readStream()`** retorna um **`DataStreamReader`** usado para configurar e consultar o stream.
# MAGIC
# MAGIC Na lição anterior, vimos um código configurado para leitura incremental com o Auto Loader. Aqui, mostraremos como é fácil ler incrementalmente uma tabela Delta Lake.
# MAGIC
# MAGIC O código usa a API PySpark para ler incrementalmente uma tabela Delta Lake chamada **`bronze`** e registrar uma visualização temporária de streaming chamada **`streaming_tmp_vw`**.
# MAGIC
# MAGIC **NOTA**: Várias configurações opcionais (não mostradas aqui) podem ser definidas ao configurar leituras incrementais — a mais importante delas permite <a href="https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate" target="_blank">limitar a taxa de entrada</a>.
# MAGIC

# COMMAND ----------

(spark.readStream
    .table("bronze")
    .createOrReplaceTempView("streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Quando executamos uma consulta em uma visualização temporária de streaming, os resultados serão continuamente atualizados conforme novos dados chegam na origem.
# MAGIC
# MAGIC Pense em uma consulta executada contra uma visualização temporária de streaming como uma **consulta incremental sempre ativa**.
# MAGIC
# MAGIC **NOTA**: De forma geral, a menos que um humano esteja ativamente monitorando a saída da consulta durante o desenvolvimento ou em um dashboard ao vivo, não retornamos os resultados de streaming para o notebook.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Você reconhecerá os dados como sendo os mesmos da tabela Delta escrita na lição anterior.
# MAGIC
# MAGIC Antes de continuar, clique em **`Stop Execution`** no topo do notebook, **`Cancel`** logo abaixo da célula, ou execute a célula a seguir para interromper todas as consultas de streaming ativas.
# MAGIC

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Trabalhando com Dados de Streaming
# MAGIC
# MAGIC Podemos executar a maioria das transformações em visualizações temporárias de streaming da mesma forma que faríamos com dados estáticos. Aqui, vamos executar uma agregação simples para obter a contagem de registros para cada **`device_id`**.
# MAGIC
# MAGIC Como estamos consultando uma visualização temporária de streaming, isso se torna uma consulta de streaming que executa indefinidamente, em vez de ser finalizada após recuperar um conjunto único de resultados. Para consultas de streaming como esta, os Notebooks do Databricks incluem dashboards interativos que permitem monitorar a performance do streaming. Explore isso abaixo.
# MAGIC
# MAGIC Um ponto importante sobre este exemplo: estamos apenas exibindo uma agregação dos dados de entrada vistos pelo stream. **Nenhum desses registros está sendo persistido em lugar algum neste momento.**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(device_id) AS total_recordings
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY device_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Antes de continuar, clique em **`Stop Execution`** no topo do notebook, **`Cancel`** logo abaixo da célula, ou execute a célula a seguir para interromper todas as consultas de streaming ativas.
# MAGIC

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Operações Não Suportadas
# MAGIC
# MAGIC A maioria das operações em um DataFrame de streaming é idêntica às de um DataFrame estático. Existem <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">algumas exceções</a>.
# MAGIC
# MAGIC Considere o modelo de dados como uma tabela em constante crescimento. A ordenação (sorting) é uma das poucas operações que são muito complexas ou logicamente impossíveis de realizar com dados de streaming.
# MAGIC
# MAGIC Uma discussão completa dessas exceções está fora do escopo deste curso. Note que métodos avançados de streaming, como *windowing* e *watermarking*, podem ser usados para adicionar funcionalidades adicionais a cargas incrementais.
# MAGIC
# MAGIC Descomente e execute a célula a seguir para ver como essa falha pode aparecer:
# MAGIC

# COMMAND ----------

# %sql
# SELECT * 
# FROM streaming_tmp_vw
# ORDER BY time

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persistindo Resultados de Streaming
# MAGIC
# MAGIC Para persistir resultados incrementais, precisamos retornar nossa lógica para a API de DataFrames de Structured Streaming do PySpark.
# MAGIC
# MAGIC Acima, criamos uma visualização temporária a partir de um DataFrame de streaming do PySpark. Se criarmos outra visualização temporária a partir dos resultados de uma consulta contra uma visualização temporária de streaming, teremos novamente uma visualização temporária de streaming.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts_tmp_vw AS (
# MAGIC   SELECT device_id, COUNT(device_id) AS total_recordings
# MAGIC   FROM streaming_tmp_vw
# MAGIC   GROUP BY device_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gravando um Stream
# MAGIC
# MAGIC Para persistir os resultados de uma consulta de streaming, precisamos gravá-los em um armazenamento durável. O método **`DataFrame.writeStream`** retorna um **`DataStreamWriter`** usado para configurar a saída.
# MAGIC
# MAGIC Ao gravar em tabelas Delta Lake, geralmente nos preocupamos com 3 configurações principais, discutidas a seguir.
# MAGIC
# MAGIC ### Checkpointing
# MAGIC
# MAGIC O Databricks cria checkpoints armazenando o estado atual do seu job de streaming no armazenamento em nuvem.
# MAGIC
# MAGIC Checkpointing é combinado com logs de escrita antecipada (*write-ahead logs*) para permitir que um stream encerrado seja reiniciado e continue de onde parou.
# MAGIC
# MAGIC Checkpoints **não podem ser compartilhados** entre streams separados. Um checkpoint é necessário para cada gravação de streaming para garantir as garantias de processamento.
# MAGIC
# MAGIC ### Modos de Saída (*Output Modes*)
# MAGIC
# MAGIC Jobs de streaming possuem modos de saída similares a workloads batch/estáticas. <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes" target="_blank">Mais detalhes aqui</a>.
# MAGIC
# MAGIC | Modo   | Exemplo | Observações |
# MAGIC | ------------- | ----------- | --- |
# MAGIC | **Append** | **`.outputMode("append")`**     | **Este é o padrão.** Somente linhas novas são adicionadas incrementalmente à tabela de destino em cada micro-lote |
# MAGIC | **Complete** | **`.outputMode("complete")`** | A tabela de resultados é recalculada a cada escrita; a tabela de destino é sobrescrita a cada micro-lote |
# MAGIC
# MAGIC ### Intervalos de Trigger
# MAGIC
# MAGIC Ao definir uma gravação de streaming, o método **`trigger`** especifica quando o sistema deve processar o próximo conjunto de dados.
# MAGIC
# MAGIC | Tipo de Trigger                  | Exemplo | Comportamento |
# MAGIC |----------------------------------|---------|---------------|
# MAGIC | Não especificado                 |         | **Este é o padrão.** Equivale a **`processingTime="500ms"`** |
# MAGIC | Micro-batches com intervalo fixo | **`.trigger(processingTime="2 minutes")`** | A consulta será executada em micro-lotes disparados nos intervalos definidos |
# MAGIC | Micro-batch único                | **`.trigger(once=True)`** | Executa um único micro-lote para processar todos os dados disponíveis e encerra |
# MAGIC | Micro-batches disponíveis        | **`.trigger(availableNow=True)`** | Executa múltiplos micro-lotes para processar todos os dados disponíveis e então encerra |
# MAGIC
# MAGIC Triggers são especificados ao definir como os dados serão gravados em um destino, controlando a frequência dos micro-batches. Por padrão, o Spark detectará e processará automaticamente todos os dados adicionados desde o último trigger.
# MAGIC
# MAGIC **NOTA:** O tipo de trigger **`Trigger.AvailableNow`** está disponível a partir do DBR 10.1 para Scala e DBR 10.2 e superior para Python e Scala.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unindo Tudo
# MAGIC
# MAGIC O código abaixo demonstra como usar **`spark.table()`** para carregar dados de uma visualização temporária de streaming de volta para um DataFrame. Note que o Spark sempre carregará visualizações de streaming como DataFrames de streaming, e visualizações estáticas como DataFrames estáticos (ou seja, o processamento incremental precisa ser definido na lógica de leitura para permitir escrita incremental).
# MAGIC
# MAGIC Nesta primeira consulta, vamos usar **`trigger(availableNow=True)`** para realizar um processamento incremental em batch.
# MAGIC

# COMMAND ----------

(spark.table("device_counts_tmp_vw")                               
    .writeStream                                                
    .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("device_counts")
    .awaitTermination() # This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Abaixo, mudamos o método de trigger para transformar esta consulta de um batch incremental acionado para uma consulta contínua acionada a cada 4 segundos.
# MAGIC
# MAGIC **NOTA**: Ao iniciar essa consulta, ainda não existem novos registros na nossa tabela de origem. Adicionaremos novos dados em breve.
# MAGIC

# COMMAND ----------

query = (spark.table("device_counts_tmp_vw")                               
              .writeStream                                                
              .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
              .outputMode("complete")
              .trigger(processingTime='4 seconds')
              .table("device_counts"))

# Like before, wait until our stream has processed some data
DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Consultando a Saída
# MAGIC
# MAGIC Agora vamos consultar a saída que escrevemos usando SQL. Como o resultado é uma tabela, só precisamos desserializar os dados para retornar os resultados.
# MAGIC
# MAGIC Como agora estamos consultando uma **tabela** (e não um DataFrame de streaming), o seguinte **não será** uma consulta de streaming.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inserindo Novos Dados
# MAGIC
# MAGIC Como na lição anterior, configuramos uma função auxiliar para inserir novos registros na nossa tabela de origem.
# MAGIC
# MAGIC Execute a célula abaixo para inserir mais um lote de dados.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Consulte novamente a tabela de destino para ver as contagens atualizadas para cada **`device_id`**.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpeza
# MAGIC
# MAGIC Sinta-se à vontade para continuar inserindo novos dados e explorando os resultados da tabela com as células acima.
# MAGIC
# MAGIC Quando terminar, execute a célula a seguir para parar todos os streams ativos e remover os recursos criados antes de continuar.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ETL de ponta a ponta no Lakehouse
# MAGIC
# MAGIC Neste notebook, você vai integrar os conceitos aprendidos ao longo do curso para completar um exemplo de pipeline de dados.
# MAGIC
# MAGIC A seguir está uma lista não exaustiva de habilidades e tarefas necessárias para completar este exercício com sucesso:
# MAGIC * Utilizar notebooks do Databricks para escrever consultas em SQL e Python
# MAGIC * Criar e modificar bancos de dados, tabelas e views
# MAGIC * Usar Auto Loader e Spark Structured Streaming para processamento incremental de dados em uma arquitetura multi-hop
# MAGIC * Usar a sintaxe SQL do Delta Live Tables
# MAGIC * Configurar um pipeline Delta Live Tables para processamento contínuo
# MAGIC * Utilizar Databricks Jobs para orquestrar tarefas a partir de notebooks armazenados em Repos
# MAGIC * Definir agendamento cronológico para Databricks Jobs
# MAGIC * Definir consultas no Databricks SQL
# MAGIC * Criar visualizações no Databricks SQL
# MAGIC * Definir dashboards no Databricks SQL para revisar métricas e resultados

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar Setup
# MAGIC
# MAGIC Execute a célula a seguir para redefinir todos os bancos de dados e diretórios associados a este laboratório.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Carregar Dados Iniciais
# MAGIC
# MAGIC Popule a zona de aterrissagem com alguns dados antes de prosseguir.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e Configurar um Pipeline DLT
# MAGIC
# MAGIC **NOTA**: A principal diferença entre as instruções aqui e em laboratórios anteriores com DLT é que neste caso configuraremos nosso pipeline para execução **Contínua** em modo **Produção**.

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Passos para criar pipeline:
# MAGIC
# MAGIC 1. Clique no botão **Jobs** na barra lateral.
# MAGIC 2. Selecione a aba **Delta Live Tables**.
# MAGIC 3. Clique em **Create Pipeline**.
# MAGIC 4. Preencha um **Nome para o Pipeline** – como esses nomes devem ser únicos, sugerimos usar o **Nome do Pipeline** fornecido na célula acima.
# MAGIC 5. Para **Notebook Libraries**, navegue e selecione o notebook **DE 12.2.2L - DLT Task**.
# MAGIC     * Alternativamente, copie o **Caminho do Notebook** especificado acima e cole no campo correspondente.
# MAGIC 6. Configure a Fonte (Source):
# MAGIC     * Clique em **`Add configuration`**.
# MAGIC     * No campo **Key**, digite **`source`**.
# MAGIC     * No campo **Value**, digite o valor de **Source** especificado acima.
# MAGIC 7. No campo **Target**, informe o nome do banco de dados impresso ao lado de **Target** na célula acima.
# MAGIC     Deve seguir o padrão **`dbacademy_<usuário>_dewd_cap_12`**.
# MAGIC 8. No campo **Storage location**, copie o diretório indicado acima.
# MAGIC 9. Para **Pipeline Mode**, selecione **Continuous**.
# MAGIC 10. Desmarque a opção **Enable autoscaling**.
# MAGIC 11. Defina o número de workers para **`1`**.
# MAGIC 12. Clique em **Create**.
# MAGIC 13. Após a interface atualizar, altere o modo de **Development** para **Production**.
# MAGIC
# MAGIC Isso iniciará o deploy da infraestrutura.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendar um Job para Notebook
# MAGIC
# MAGIC Nosso pipeline DLT está configurado para processar dados assim que eles chegarem.
# MAGIC
# MAGIC Vamos agendar um notebook para inserir novos lotes de dados a cada minuto para vermos essa funcionalidade em ação.
# MAGIC
# MAGIC Antes de começar, execute a célula a seguir para obter os valores usados neste passo.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Passos para criar o Job:
# MAGIC
# MAGIC 1. Navegue até a interface de Jobs usando a barra lateral esquerda do Databricks.
# MAGIC 2. Clique no botão azul **Create Job**.
# MAGIC 3. Configure a tarefa:
# MAGIC     1. Informe **Land-Data** como nome da tarefa.
# MAGIC     2. Selecione o notebook **DE 12.2.3L - Land New Data** usando o seletor de notebooks.
# MAGIC     3. No dropdown de **Cluster**, sob **Existing All Purpose Cluster**, selecione seu cluster.
# MAGIC     4. Clique em **Create**.
# MAGIC 4. No canto superior esquerdo, renomeie o job (não a tarefa) de **`Land-Data`** (valor padrão) para o **Nome do Job** fornecido na célula anterior.
# MAGIC
# MAGIC > **Nota:** Ao selecionar seu cluster all purpose, aparecerá um aviso sobre custo. Jobs de produção devem ser sempre agendados em clusters job clusters adequados para a carga, pois isso reduz custos.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definir um Agendamento Cronológico para o Job
# MAGIC
# MAGIC Passos:
# MAGIC
# MAGIC 1. Navegue até a interface de **Jobs** e clique no job que você acabou de criar.
# MAGIC 2. Localize a seção **Schedule** no painel lateral direito.
# MAGIC 3. Clique no botão **Edit schedule** para ver opções de agendamento.
# MAGIC 4. Mude o campo **Schedule type** de **Manual** para **Scheduled**, que abrirá a interface para configuração cron.
# MAGIC 5. Configure para atualizar **A cada 2**, **Minutos** a partir do minuto **00**.
# MAGIC 6. Clique em **Save**.
# MAGIC
# MAGIC **Nota:** Se desejar, clique em **Run now** para disparar a primeira execução, ou aguarde o próximo minuto para garantir que o agendamento funcionou.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Registrar Métricas de Eventos DLT para Consulta no DBSQL
# MAGIC
# MAGIC A célula a seguir imprime instruções SQL para registrar os logs de eventos DLT no seu banco de dados destino para consultas no Databricks SQL.
# MAGIC
# MAGIC Execute o código gerado no editor de consultas DBSQL para registrar essas tabelas e views.
# MAGIC
# MAGIC Explore cada uma delas e observe as métricas registradas.
# MAGIC

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definir Consulta na Tabela Gold
# MAGIC
# MAGIC A tabela **daily_patient_avg** é atualizada automaticamente cada vez que um novo lote é processado pelo pipeline DLT. Ao executar consultas nessa tabela, o DBSQL verifica se existe uma versão mais recente e materializa os resultados da versão mais atual.
# MAGIC
# MAGIC Execute a célula abaixo para imprimir uma consulta formatada com seu nome de banco de dados. Salve essa consulta no DBSQL.

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adicionar Visualização do Tipo Linha (Line Plot)
# MAGIC
# MAGIC Para acompanhar tendências na média de pacientes ao longo do tempo, crie um gráfico de linha e adicione-o a um dashboard novo.
# MAGIC
# MAGIC Configure o gráfico com:
# MAGIC * **Coluna X**: **`date`**
# MAGIC * **Coluna Y**: **`avg_heartrate`**
# MAGIC * **Agrupar por**: **`name`**
# MAGIC
# MAGIC Adicione esta visualização ao seu dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Acompanhar Progresso do Processamento dos Dados
# MAGIC
# MAGIC O código abaixo extrai **`flow_name`**, **`timestamp`** e **`num_output_rows`** dos logs de eventos DLT.
# MAGIC
# MAGIC Salve esta consulta no DBSQL, então defina uma visualização do tipo gráfico de barras (bar plot) mostrando:
# MAGIC * **Coluna X**: **`timestamp`**
# MAGIC * **Coluna Y**: **`num_output_rows`**
# MAGIC * **Agrupar por**: **`flow_name`**
# MAGIC
# MAGIC Adicione esta visualização ao seu dashboard.

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Atualizar Dashboard e Monitorar Resultados
# MAGIC
# MAGIC O notebook **Land-Data** agendado com Jobs acima contém 12 lotes de dados, cada um representando um mês de registros para nossa pequena amostra de pacientes. Conforme configurado, levará pouco mais de 20 minutos para que todos esses lotes sejam processados (agendamos o job para rodar a cada 2 minutos, e os lotes são rapidamente processados após ingestão).
# MAGIC
# MAGIC Atualize seu dashboard e revise suas visualizações para ver quantos lotes foram processados. (Se seguiu as instruções corretamente, deve haver 12 atualizações distintas de fluxo rastreadas pelas métricas DLT.)
# MAGIC
# MAGIC Caso algum lote não tenha sido processado ainda, volte para a UI de Jobs do Databricks e dispare manualmente mais lotes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Com tudo configurado, você pode agora continuar para a parte final do laboratório no notebook [DE 12.2.4L - Final Steps]($./DE 12.2.4L - Final Steps)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

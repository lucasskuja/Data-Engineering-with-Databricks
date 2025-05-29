# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Orquestrando Jobs no Databricks
# MAGIC
# MAGIC Novas atualizações na interface de **Databricks Jobs** adicionaram a capacidade de agendar várias tarefas como parte de um job, permitindo que Jobs assuma toda a orquestração da maior parte das cargas de trabalho de produção.
# MAGIC
# MAGIC Aqui, começaremos revisando os passos para agendar um notebook como job isolado (acionado manualmente) e, depois, adicionaremos uma tarefa dependente usando um pipeline DLT.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Agendar um notebook como um Databricks Job
# MAGIC * Descrever as opções de agendamento e as diferenças entre tipos de cluster
# MAGIC * Revisar **Job Runs** para acompanhar o progresso e ver resultados
# MAGIC * Agendar um pipeline DLT como Databricks Job
# MAGIC * Configurar dependências lineares entre tarefas usando a UI de Jobs
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-9.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e configurar um pipeline
# MAGIC O pipeline criado aqui é quase idêntico ao da unidade anterior e será usado como parte de um job agendado nesta lição.
# MAGIC
# MAGIC Execute a célula a seguir para imprimir os valores que serão usados nos passos de configuração.
# MAGIC

# COMMAND ----------

print_pipeline_config()    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e configurar um pipeline
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão **Jobs** na barra lateral.  
# MAGIC 2. Selecione a guia **Delta Live Tables**.  
# MAGIC 3. Clique em **Create Pipeline**.  
# MAGIC 4. Informe um **Pipeline Name** — como os nomes precisam ser únicos, use o valor exibido na célula anterior.  
# MAGIC 5. Em **Notebook Libraries**, localize e selecione o notebook **DE 9.1.3 ‑ DLT Job** (ou cole o **Notebook Path**).  
# MAGIC 6. No campo **Target**, especifique o banco de dados mostrado ao lado de **Target** (padrão **`dbacademy_<username>_dewd_dlt_demo_91`**).  
# MAGIC 7. No campo **Storage location**, cole o diretório exibido acima.  
# MAGIC 8. Defina **Pipeline Mode** como **Triggered**.  
# MAGIC 9. Desmarque **Enable autoscaling**.  
# MAGIC 10. Defina **workers** como **1**.  
# MAGIC 11. Clique em **Create**.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: não executaremos este pipeline diretamente — ele será disparado pelo job mais adiante — mas, se quiser testar rapidamente, clique em **Start** agora.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendar um Job de Notebook
# MAGIC
# MAGIC Ao orquestrar um fluxo com várias tarefas na UI de Jobs, sempre começamos agendando uma única tarefa.
# MAGIC
# MAGIC Execute a célula abaixo para obter os valores que serão usados neste passo.
# MAGIC

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aqui agendaremos o próximo notebook.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Navegue até **Jobs** pela barra lateral esquerda do Databricks.  
# MAGIC 2. Clique no botão azul **Create Job**.  
# MAGIC 3. Configure a tarefa:  
# MAGIC    1. Nome da tarefa: **`reset`**  
# MAGIC    2. Notebook: **`DE 9.1.2 ‑ Reset`** (seletor de notebooks)  
# MAGIC    3. **Cluster**: escolha seu cluster em **Existing All Purpose Clusters**  
# MAGIC    4. Clique em **Create**  
# MAGIC 4. No canto superior‑esquerdo, renomeie o job (não a tarefa) de **`reset`** para o **Job Name** gerado anteriormente.  
# MAGIC 5. Clique em **Run now** (canto superior‑direito) para iniciar o job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: usar um cluster “all‑purpose” gera um aviso de cobrança. Jobs de produção devem usar **job clusters** dedicados, que têm custo menor.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendamento Cron em Databricks Jobs
# MAGIC
# MAGIC Na direita da UI de Jobs, abaixo de **Job Details**, há a seção **Schedule**.
# MAGIC
# MAGIC Clique em **Edit schedule** para explorar as opções.  
# MAGIC Altere **Schedule type** de **Manual** para **Scheduled** para abrir a interface de cron.  
# MAGIC Ela gera expressões cron automaticamente e permite ajustes manuais se necessário.
# MAGIC
# MAGIC Por enquanto, manteremos o job em **Manual**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar a Execução
# MAGIC
# MAGIC Com uma única tarefa, temos a mesma experiência do Jobs UI “legado”.
# MAGIC
# MAGIC Para revisar:
# MAGIC 1. Clique na aba **Runs** (canto superior‑esquerdo).  
# MAGIC 2. Localize a execução em **Active runs** (se ainda ativa) ou **Completed runs**.  
# MAGIC 3. Clique no carimbo de **Start time** para abrir detalhes.  
# MAGIC 4. Veja o painel à direita: **`Pending`**, **`Running`**, **`Succeeded`** ou **`Failed`**.  
# MAGIC
# MAGIC O notebook usa o comando mágico **`%run`** para chamar outro notebook por caminho relativo.  
# MAGIC O efeito final é resetar o ambiente para o novo job e pipeline.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendar um Pipeline DLT como Tarefa
# MAGIC
# MAGIC Agora adicionaremos o pipeline DLT para ser executado após a tarefa inicial.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Na parte superior‑esquerda, troque para a aba **Tasks**.  
# MAGIC 2. Clique no círculo azul **+** no rodapé para nova tarefa.  
# MAGIC    1. **Task name**: **`dlt`**  
# MAGIC    2. **Type**: **Delta Live Tables pipeline**  
# MAGIC    3. **Pipeline**: selecione o pipeline criado (começa com **Jobs‑Demo‑91**).  
# MAGIC    4. **Depends on** já deve apontar para a tarefa **reset** (pode estar renomeada).  
# MAGIC    5. Clique em **Create task**.
# MAGIC
# MAGIC Você verá dois blocos conectados por seta: **reset** → **dlt**.  
# MAGIC Clique em **Run now** para executar o job.
# MAGIC
# MAGIC **OBS:** aguarde alguns minutos para provisionamento da infraestrutura.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar Resultados de Execução Multi‑Task
# MAGIC
# MAGIC Volte à aba **Runs** e abra a execução mais recente.  
# MAGIC O diagrama de tarefas se atualiza em tempo real, indicando estados e falhas.
# MAGIC
# MAGIC Clicar em uma caixa abre o notebook agendado ou redireciona para a UI do pipeline DLT.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: tarefas DLT não mostram detalhes diretamente na aba Runs; você será redirecionado à interface do Pipeline DLT correspondente.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

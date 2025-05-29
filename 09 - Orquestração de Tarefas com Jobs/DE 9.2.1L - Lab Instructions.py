# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Laboratório: Orquestração de Jobs com o Databricks
# MAGIC
# MAGIC Neste laboratório, você irá configurar um job com várias tarefas, composto por:
# MAGIC * Um notebook que insere um novo lote de dados em um diretório de armazenamento
# MAGIC * Um pipeline Delta Live Tables que processa esses dados por meio de uma série de tabelas
# MAGIC * Um notebook que consulta a tabela gold produzida por esse pipeline, bem como várias métricas geradas pelo DLT
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você será capaz de:
# MAGIC * Agendar um notebook como um Job no Databricks
# MAGIC * Agendar um pipeline DLT como um Job no Databricks
# MAGIC * Configurar dependências lineares entre tarefas usando a interface de Jobs do Databricks
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-9.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inserir Dados Iniciais
# MAGIC Insira dados iniciais na zona de *landing* antes de continuar. Você executará novamente este comando para inserir dados adicionais posteriormente.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e Configurar um Pipeline
# MAGIC
# MAGIC O pipeline que criaremos aqui é quase idêntico ao da unidade anterior.
# MAGIC
# MAGIC Nós o utilizaremos como parte de um job agendado nesta lição.
# MAGIC
# MAGIC Execute a célula a seguir para exibir os valores que serão usados nas etapas de configuração.
# MAGIC

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Clique no botão **Jobs** na barra lateral.
# MAGIC 1. Selecione a guia **Delta Live Tables**.
# MAGIC 1. Clique em **Create Pipeline**.
# MAGIC 1. Preencha um **Nome do Pipeline** – como os nomes devem ser únicos, sugerimos usar o **Pipeline Name** fornecido na célula acima.
# MAGIC 1. Em **Notebook Libraries**, use o navegador para localizar e selecionar o notebook chamado **DE 9.2.3L - DLT Job**.
# MAGIC     * Alternativamente, você pode copiar o **Notebook Path** especificado acima e colá-lo no campo apropriado.
# MAGIC 1. Configurar a Fonte:
# MAGIC     * Clique em **`Add configuration`**
# MAGIC     * No campo **Key**, insira **`source`**
# MAGIC     * No campo **Value**, insira o valor de **Source** especificado acima
# MAGIC 1. No campo **Target**, insira o nome do banco de dados exibido ao lado de **Target** na célula anterior.<br/>
# MAGIC Esse nome deve seguir o padrão **`dbacademy_<username>_dewd_jobs_lab_92`**
# MAGIC 1. No campo **Storage location**, copie o diretório exibido anteriormente.
# MAGIC 1. Em **Pipeline Mode**, selecione **Triggered**
# MAGIC 1. Desmarque a opção **Enable autoscaling**
# MAGIC 1. Defina o número de trabalhadores para **1**
# MAGIC 1. Clique em **Create**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Nota**: não executaremos esse pipeline diretamente, pois ele será executado pelo job que configuraremos a seguir.<br/>
# MAGIC Mas se quiser testá-lo rapidamente, você pode clicar no botão **Start** agora.
# MAGIC

# COMMAND ----------

# MAGIC %md --i18n-f98768ac-cbcc-42a2-8c51-ffdc3778aa11
# MAGIC
# MAGIC ## Agendar um Job de Notebook
# MAGIC
# MAGIC Ao usar a interface de Jobs para orquestrar uma carga de trabalho com várias tarefas, você sempre começará agendando uma única tarefa.
# MAGIC
# MAGIC Antes de começar, execute a célula a seguir para obter os valores usados nesta etapa.
# MAGIC

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aqui, começaremos agendando o notebook de carga em lote.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Acesse a interface de Jobs usando a barra de navegação lateral esquerda do Databricks.
# MAGIC 1. Clique no botão azul **Create Job**
# MAGIC 1. Configure a tarefa:
# MAGIC     1. Insira **Batch-Job** como nome da tarefa
# MAGIC     1. Selecione o notebook **DE 9.2.2L - Batch Job** usando o seletor de notebooks
# MAGIC     1. No menu **Cluster**, em **Existing All Purpose Cluster**, selecione seu cluster
# MAGIC     1. Clique em **Create**
# MAGIC 1. No canto superior esquerdo da tela, renomeie o job (não a tarefa) de **`Batch-Job`** (valor padrão) para o **Job Name** fornecido na célula anterior.
# MAGIC 1. Clique no botão azul **Run now** no canto superior direito para testar o job rapidamente.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Nota**: Ao selecionar seu cluster de uso geral, você verá um aviso indicando que a cobrança será feita como *all purpose compute*. Jobs de produção devem sempre ser executados em clusters criados especialmente para execução de jobs, pois esses têm custo reduzido.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendar um Pipeline DLT como uma Tarefa
# MAGIC
# MAGIC Nesta etapa, adicionaremos um pipeline DLT para ser executado após o sucesso da tarefa configurada anteriormente.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. No canto superior esquerdo da tela, clique na guia **Tasks**, se ainda não estiver selecionada.
# MAGIC 1. Clique no grande círculo azul com um **+** no centro inferior da tela para adicionar uma nova tarefa:
# MAGIC     1. Defina **DLT-Pipeline** como nome da tarefa
# MAGIC     1. Em **Type**, selecione **`Delta Live Tables pipeline`**
# MAGIC     1. No campo **Pipeline**, selecione o pipeline DLT configurado anteriormente<br/>
# MAGIC     Nota: O nome do pipeline começará com **Jobs-Labs-92** e terminará com seu e-mail.
# MAGIC     1. O campo **Depends on** será preenchido automaticamente com a tarefa definida anteriormente, mas pode ter sido renomeado de **reset** para algo como **Jobs-Lab-92-seuemail**.
# MAGIC     1. Clique no botão azul **Create task**
# MAGIC
# MAGIC Agora você verá uma tela com 2 caixas e uma seta apontando para baixo entre elas.
# MAGIC
# MAGIC Sua tarefa **`Batch-Job`** (possivelmente renomeada) estará no topo, 
# MAGIC seguida pela tarefa **`DLT-Pipeline`**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Agendar uma Tarefa Adicional de Notebook
# MAGIC
# MAGIC Foi fornecido um notebook adicional que consulta algumas das métricas do DLT e a tabela gold definida no pipeline.
# MAGIC
# MAGIC Vamos adicioná-lo como a tarefa final do nosso job.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. No canto superior esquerdo da tela, clique na guia **Tasks**, se ainda não estiver selecionada.
# MAGIC 1. Clique no grande círculo azul com um **+** no centro inferior da tela para adicionar uma nova tarefa:
# MAGIC     1. Defina **Query-Results** como nome da tarefa
# MAGIC     1. Mantenha **Notebook** como tipo
# MAGIC     1. Selecione o notebook **DE 9.2.4L - Query Results Job** usando o seletor
# MAGIC     1. O campo **Depends on** será automaticamente preenchido com **DLT-Pipeline**
# MAGIC     1. No menu **Cluster**, em **Existing All Purpose Cluster**, selecione seu cluster
# MAGIC     1. Clique no botão azul **Create task**
# MAGIC
# MAGIC Clique no botão azul **Run now** no canto superior direito da tela para executar o job.
# MAGIC
# MAGIC Na guia **Runs**, você poderá clicar no horário de início da execução atual na seção **Active runs** e acompanhar visualmente o progresso das tarefas.
# MAGIC
# MAGIC Depois que todas as tarefas forem concluídas com sucesso, revise o conteúdo de cada uma para confirmar o comportamento esperado.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

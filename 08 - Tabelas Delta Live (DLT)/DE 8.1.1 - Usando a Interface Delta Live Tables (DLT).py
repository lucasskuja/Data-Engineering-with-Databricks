# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Usando a Interface Delta Live Tables (DLT)
# MAGIC
# MAGIC Esta demonstração explora a interface do Delta Live Tables (DLT).
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Implantar um pipeline DLT
# MAGIC * Explorar o DAG resultante
# MAGIC * Executar uma atualização do pipeline
# MAGIC * Visualizar métricas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar Configuração
# MAGIC
# MAGIC A célula a seguir está configurada para redefinir esta demonstração.
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para imprimir os valores que serão usados durante as etapas de configuração a seguir.
# MAGIC

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e Configurar um Pipeline
# MAGIC
# MAGIC Nesta seção, você criará um pipeline usando um notebook fornecido com o material do curso. Vamos explorar o conteúdo do notebook na próxima lição.
# MAGIC
# MAGIC 1. Clique no botão **Jobs** na barra lateral.
# MAGIC 2. Selecione a guia **Delta Live Tables**.
# MAGIC 3. Clique em **Create Pipeline**.
# MAGIC 4. Mantenha **Product Edition** como **Advanced**.
# MAGIC 5. Preencha o campo **Pipeline Name** – como os nomes devem ser únicos, sugerimos usar o **`Pipeline Name`** fornecido pela célula anterior.
# MAGIC 6. Em **Notebook Libraries**, use o navegador para localizar e selecionar o notebook chamado **DE 8.1.2 - SQL for Delta Live Tables**.
# MAGIC    * Alternativamente, você pode copiar o **`Notebook Path`** fornecido anteriormente e colar no campo apropriado.
# MAGIC    * Embora este documento seja um notebook padrão do Databricks, a sintaxe SQL usada é especializada para declarações de tabelas DLT.
# MAGIC    * Vamos explorar essa sintaxe no exercício a seguir.
# MAGIC 7. No campo **Target**, especifique o nome do banco de dados impresso ao lado de **`Target`** na célula anterior.<br/>
# MAGIC    Este nome deve seguir o padrão **`dbacademy_<seu-usuário>_dewd_dlt_demo_81`**.
# MAGIC    * Este campo é opcional; se não for especificado, as tabelas não serão registradas no metastore, mas ainda estarão disponíveis no DBFS. Consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentação</a> para mais informações.
# MAGIC 8. No campo **Storage location**, copie o caminho de **`Storage Location`** impresso anteriormente.
# MAGIC    * Este campo opcional permite especificar um local para armazenar logs, tabelas e outras informações relacionadas à execução do pipeline.
# MAGIC    * Se não for especificado, o DLT irá gerar automaticamente um diretório.
# MAGIC 9. Em **Pipeline Mode**, selecione **Triggered**.
# MAGIC    * Este campo especifica como o pipeline será executado.
# MAGIC    * Pipelines **Triggered** executam uma vez e são encerrados até a próxima atualização manual ou agendada.
# MAGIC    * Pipelines **Continuous** executam continuamente, ingerindo novos dados assim que chegam. Escolha o modo com base nos requisitos de latência e custo.
# MAGIC 10. Desmarque a opção **Enable autoscaling** e defina o número de workers para **`1`** (um).
# MAGIC     * **Enable autoscaling**, **Min Workers** e **Max Workers** controlam a configuração de workers para o cluster que processa o pipeline. Observe a estimativa de DBU fornecida, similar à exibida ao configurar clusters interativos.
# MAGIC 11. Clique em **Create**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar um Pipeline
# MAGIC
# MAGIC Com o pipeline criado, agora você irá executá-lo.
# MAGIC
# MAGIC 1. Selecione **Development** para executar o pipeline no modo de desenvolvimento.
# MAGIC    * O modo de desenvolvimento acelera o processo iterativo reutilizando o cluster (em vez de criar um novo para cada execução) e desabilitando as tentativas automáticas de reexecução, facilitando a identificação e correção de erros.
# MAGIC    * Consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentação</a> para mais informações sobre este recurso.
# MAGIC 2. Clique em **Start**.
# MAGIC
# MAGIC A execução inicial levará alguns minutos enquanto um cluster é provisionado.
# MAGIC
# MAGIC As execuções subsequentes serão consideravelmente mais rápidas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explorando o DAG
# MAGIC
# MAGIC À medida que o pipeline é executado, o fluxo de execução é exibido graficamente.
# MAGIC
# MAGIC Ao selecionar as tabelas, é possível revisar os detalhes.
# MAGIC
# MAGIC Selecione **sales_orders_cleaned**. Observe os resultados na seção **Data Quality**. Como esse fluxo declara expectativas de dados, essas métricas são monitoradas aqui. Nenhum registro é descartado porque a restrição é declarada de forma que permite incluir registros que violem a expectativa na saída. Isso será abordado com mais detalhes no próximo exercício.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

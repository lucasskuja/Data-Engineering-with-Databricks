# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ETL de Ponta a Ponta no Lakehouse
# MAGIC ## Etapas Finais
# MAGIC
# MAGIC Estamos retomando a partir do primeiro notebook deste laboratório, [DE 12.2.1L - Instruções e Configuração]($./DE 12.2.1L - Instructions and Configuration)
# MAGIC
# MAGIC Se tudo foi configurado corretamente, você deve ter:
# MAGIC * Um Pipeline DLT em execução no modo **Continuous**
# MAGIC * Um job que alimenta esse pipeline com novos dados a cada 2 minutos
# MAGIC * Uma série de consultas Databricks SQL analisando as saídas desse pipeline
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Executar uma Consulta para Reparar Dados Inválidos
# MAGIC
# MAGIC Revise o código que define a tabela **`recordings_enriched`** para identificar o filtro aplicado no controle de qualidade.
# MAGIC
# MAGIC Na célula abaixo, escreva uma consulta que retorne todos os registros da tabela **`recordings_bronze`** que foram rejeitados por esse controle de qualidade.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC --<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Para fins da nossa demonstração, vamos supor que uma revisão manual detalhada dos dados e sistemas mostrou que, ocasionalmente, leituras de frequência cardíaca válidas são retornadas com valores negativos.
# MAGIC
# MAGIC Execute a consulta a seguir para examinar essas mesmas linhas com o sinal negativo removido.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Para completar nosso conjunto de dados, queremos inserir esses registros corrigidos na tabela silver **`recordings_enriched`**.
# MAGIC
# MAGIC Use a célula abaixo para atualizar a consulta usada no pipeline DLT e executar esse reparo.
# MAGIC
# MAGIC **NOTA**: Certifique‑se de atualizar o código para processar somente os registros que foram rejeitados anteriormente pelo controle de qualidade.
# MAGIC

# COMMAND ----------

# TODO
# CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
#   (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
# AS SELECT 
#   CAST(a.device_id AS INTEGER) device_id, 
#   CAST(a.mrn AS LONG) mrn, 
#   CAST(a.heartrate AS DOUBLE) heartrate, 
#   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
#   b.name
#   FROM STREAM(live.recordings_bronze) a
#   INNER JOIN STREAM(live.pii) b
#   ON a.mrn = b.mrn

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Use a célula abaixo para confirmar — manual ou programaticamente — que essa atualização foi bem‑sucedida.
# MAGIC
# MAGIC (O número total de registros em **`recordings_bronze`** agora deve ser igual ao total de registros em **`recordings_enriched`**).
# MAGIC

# COMMAND ----------

# TODO
#<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Considerar Permissões de Dados em Produção
# MAGIC
# MAGIC Embora nosso reparo manual tenha sido bem‑sucedido, como proprietários desses conjuntos de dados temos, por padrão, permissão para modificar ou excluir esses dados de qualquer lugar onde executemos código.
# MAGIC
# MAGIC Em outras palavras: nossas permissões atuais permitem alterar ou remover permanentemente tabelas de produção se uma consulta SQL incorreta for executada acidentalmente com as permissões do usuário atual (ou se outros usuários receberem permissões semelhantes).
# MAGIC
# MAGIC Para produção, é mais seguro usar <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">service principals</a> ao agendar Jobs e Pipelines DLT, evitando modificações acidentais nos dados.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Encerrar a Infraestrutura de Produção
# MAGIC
# MAGIC Jobs do Databricks, Pipelines DLT e consultas/dashboards agendados no DBSQL são projetados para execução contínua de código de produção. Neste demo de ponta a ponta, você configurou um Job e um Pipeline para processamento contínuo de dados. Para impedir que essas cargas continuem a executar, **Pause** seu Job do Databricks e **Stop** seu Pipeline DLT. Excluir esses ativos também garante que a infraestrutura de produção seja encerrada.
# MAGIC
# MAGIC **NOTA**: Todas as instruções de agendamento de ativos DBSQL em lições anteriores definiram o término das atualizações para amanhã. Você pode voltar e cancelar esses agendamentos para evitar que endpoints DBSQL permaneçam ativos até lá.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

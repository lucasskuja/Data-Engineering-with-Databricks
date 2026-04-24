# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Laboratório: Conclusão
# MAGIC Execute a célula seguinte para configurar o ambiente do laboratório:
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.3L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exibir Resultados
# MAGIC
# MAGIC Supondo que seu pipeline tenha sido executado com sucesso, exiba o conteúdo da tabela gold.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Como especificamos um valor para **Target**, as tabelas são publicadas no banco de dados especificado. Sem a especificação de **Target**, precisaríamos consultar a tabela com base no local subjacente no DBFS (relativo ao **Storage Location**).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Dispare a chegada de outro arquivo com a célula a seguir.
# MAGIC
# MAGIC Sinta-se à vontade para executá-la algumas vezes, se desejar.
# MAGIC
# MAGIC Depois disso, execute o pipeline novamente e veja os resultados.
# MAGIC
# MAGIC Você também pode reexecutar a célula acima para obter uma visão atualizada da tabela **`daily_patient_avg`**.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Encerramento
# MAGIC
# MAGIC Certifique-se de excluir seu pipeline pela interface do DLT e execute a próxima célula para limpar os arquivos e tabelas que foram criados durante a configuração e execução do laboratório.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Resumo
# MAGIC
# MAGIC Neste laboratório, você aprendeu a converter um pipeline de dados existente para um pipeline Delta Live Tables em SQL, e implantou esse pipeline usando a interface do DLT.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tópicos e Recursos Adicionais
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Documentação Delta Live Tables</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Demonstração Delta Live Tables</a>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

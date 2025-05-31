# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Laboratório: Migrando Notebooks SQL para Delta Live Tables
# MAGIC
# MAGIC Este notebook descreve a estrutura geral do exercício do laboratório, configura o ambiente para o laboratório, fornece dados simulados em streaming e realiza a limpeza ao final. Normalmente, um notebook assim não é necessário em pipelines de produção.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC * Converter pipelines de dados existentes para Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conjuntos de Dados Utilizados
# MAGIC
# MAGIC Esta demonstração usa dados médicos artificiais simplificados. O esquema dos dois conjuntos de dados está representado abaixo. Note que manipularemos esses esquemas durante as etapas.
# MAGIC
# MAGIC #### Recordings (Gravações)
# MAGIC
# MAGIC O conjunto principal usa gravações de frequência cardíaca de dispositivos médicos entregues no formato JSON.
# MAGIC
# MAGIC | Campo     | Tipo   |
# MAGIC |-----------|--------|
# MAGIC | device_id | int    |
# MAGIC | mrn       | long   |
# MAGIC | time      | double |
# MAGIC | heartrate | double |
# MAGIC
# MAGIC #### PII (Informações Pessoais Identificáveis)
# MAGIC
# MAGIC Esses dados serão combinados com uma tabela estática de informações dos pacientes armazenada em um sistema externo para identificar os pacientes pelo nome.
# MAGIC
# MAGIC | Campo | Tipo   |
# MAGIC |-------|--------|
# MAGIC | mrn   | long   |
# MAGIC | name  | string |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Começando
# MAGIC
# MAGIC Comece executando a célula a seguir para configurar o ambiente do laboratório.
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingerir Dados Iniciais
# MAGIC
# MAGIC Alimente a zona de aterrissagem com alguns dados antes de prosseguir. Você irá executar este comando novamente para adicionar mais dados depois.
# MAGIC

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula abaixo para imprimir os valores que serão usados nas próximas etapas de configuração.
# MAGIC

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar e Configurar um Pipeline
# MAGIC
# MAGIC 1. Clique no botão **Jobs** na barra lateral e selecione a aba **Delta Live Tables**.
# MAGIC 2. Clique em **Create Pipeline**.
# MAGIC 3. Mantenha a **Product Edition** como **Advanced**.
# MAGIC 4. Preencha um **Pipeline Name** — como esses nomes devem ser únicos, sugerimos usar o nome do pipeline fornecido na célula acima.
# MAGIC 5. Em **Notebook Libraries**, use o navegador para localizar e selecionar o notebook **`DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab`**.
# MAGIC 6. Configure a Fonte (Source)
# MAGIC     * Clique em **`Add configuration`**
# MAGIC     * Digite a palavra **`source`** no campo **Key**
# MAGIC     * Digite o valor **Source** especificado acima no campo **Value**
# MAGIC 7. No campo **Target**, insira o nome do banco de dados impresso ao lado de **`Target`** abaixo.
# MAGIC 8. No campo **Storage Location**, insira o local impresso ao lado de **`Storage Location`** abaixo.
# MAGIC 9. Defina o **Pipeline Mode** para **Triggered**.
# MAGIC 10. Desative o autoscaling.
# MAGIC 11. Defina o número de **`workers`** para **`1`** (um).
# MAGIC 12. Clique em **Create**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Abrir e Completar o Notebook do Pipeline DLT
# MAGIC
# MAGIC Você realizará o trabalho no notebook companheiro [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab),<br/>
# MAGIC que será implantado como um pipeline.
# MAGIC
# MAGIC Abra o notebook e, seguindo as instruções fornecidas, preencha as células indicadas para implementar uma arquitetura multi-hop semelhante àquela trabalhada na seção anterior.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar seu Pipeline
# MAGIC
# MAGIC Selecione o modo **Development**, que acelera o ciclo de desenvolvimento ao reutilizar o mesmo cluster entre execuções.<br/>
# MAGIC Também desliga as tentativas automáticas em caso de falha do job.
# MAGIC
# MAGIC Clique em **Start** para iniciar a primeira atualização da sua tabela.
# MAGIC
# MAGIC O Delta Live Tables implantará automaticamente toda a infraestrutura necessária e resolverá as dependências entre os conjuntos de dados.
# MAGIC
# MAGIC **OBS:** A primeira atualização da tabela pode levar alguns minutos enquanto as dependências são resolvidas e a infraestrutura é implantada.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Depuração do Código em Modo de Desenvolvimento
# MAGIC
# MAGIC Não se desespere se seu pipeline falhar na primeira vez. O Delta Live Tables está em desenvolvimento ativo, e as mensagens de erro estão sempre melhorando.
# MAGIC
# MAGIC Como os relacionamentos entre tabelas são mapeados como um DAG (grafo acíclico direcionado), mensagens de erro frequentemente indicam que um conjunto de dados não foi encontrado.
# MAGIC
# MAGIC Considere nosso DAG abaixo:
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/dlt-dag.png">
# MAGIC
# MAGIC Se a mensagem de erro **`Dataset not found: 'recordings_parsed'`** aparecer, pode haver várias causas:
# MAGIC 1. A lógica que define **`recordings_parsed`** está incorreta
# MAGIC 2. Há um erro ao ler de **`recordings_bronze`**
# MAGIC 3. Existe um erro de digitação em **`recordings_parsed`** ou **`recordings_bronze`**
# MAGIC
# MAGIC A forma mais segura de identificar o problema é adicionando as definições de tabelas/views iterativamente de volta ao seu DAG, começando pelas tabelas de ingestão inicial. Você pode simplesmente comentar as definições posteriores e descomentar essas entre execuções.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

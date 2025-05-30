# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Criar e Gerenciar Clusters Interativos
# MAGIC
# MAGIC Um cluster do Databricks é um conjunto de recursos computacionais e configurações nos quais você executa cargas de trabalho de engenharia de dados, ciência de dados e análise de dados, como pipelines de ETL em produção, análises de streaming, análises ad-hoc e aprendizado de máquina.
# MAGIC Você executa essas cargas de trabalho como um conjunto de comandos em um notebook ou como um trabalho automatizado.
# MAGIC
# MAGIC O Databricks faz uma distinção entre clusters de uso geral e clusters de trabalho (job clusters).
# MAGIC - Você usa **clusters de uso geral** para analisar dados de forma colaborativa utilizando notebooks interativos.
# MAGIC - Você usa **clusters de trabalho** para executar trabalhos automatizados de forma rápida e robusta.
# MAGIC
# MAGIC Esta demonstração abordará a criação e o gerenciamento de clusters de uso geral no Databricks, usando o ambiente **Data Science & Engineering Workspace**.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC
# MAGIC - Usar a interface de Clusters para configurar e implantar um cluster
# MAGIC - Editar, encerrar, reiniciar e excluir clusters

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar Cluster
# MAGIC
# MAGIC Dependendo do workspace em que você está trabalhando, você pode ou não ter privilégios para criar clusters.
# MAGIC
# MAGIC As instruções desta seção assumem que você **tem** privilégios para criação de cluster e que precisa implantar um novo cluster para executar as lições deste curso.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Verifique com seu instrutor ou com um administrador da plataforma se você deve criar um novo cluster ou se deve se conectar a um cluster que já foi implantado. As políticas de cluster podem afetar suas opções de configuração.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Use a barra lateral esquerda para navegar até a página **Compute**, clicando no ícone ![compute](https://files.training.databricks.com/images/clusters-icon.png)
# MAGIC 1. Clique no botão azul **Create Cluster**
# MAGIC 1. Para o **Nome do cluster**, use seu nome para que possa encontrá-lo facilmente e para que o instrutor possa identificá-lo caso você tenha problemas
# MAGIC 1. Defina o **Modo do cluster** como **Single Node** (esse modo é necessário para executar este curso)
# MAGIC 1. Use a **versão do runtime do Databricks** recomendada para este curso
# MAGIC 1. Deixe as opções padrão marcadas em **Autopilot Options**
# MAGIC 1. Clique no botão azul **Create Cluster**
# MAGIC
# MAGIC **OBSERVAÇÃO:** Clusters podem levar alguns minutos para serem implantados. Após finalizar a implantação do cluster, sinta-se à vontade para explorar a interface de criação de clusters.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### <img src="https://cdn-icons-png.flaticon.com/512/6897/6897039.png"> Cluster de Nó Único Obrigatório para Este Curso
# MAGIC **IMPORTANTE:** Este curso exige que você execute os notebooks em um cluster de nó único.
# MAGIC
# MAGIC Siga as instruções acima para criar um cluster com o **Modo do cluster** definido como **`Single Node`**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gerenciar Clusters
# MAGIC
# MAGIC Depois que o cluster for criado, volte para a página **Compute** para visualizar o cluster.
# MAGIC
# MAGIC Selecione um cluster para revisar a configuração atual.
# MAGIC
# MAGIC Clique no botão **Edit**. Observe que a maioria das configurações pode ser modificada (caso você tenha permissões suficientes). Alterar a maioria das configurações exigirá a reinicialização do cluster em execução.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Usaremos nosso cluster na próxima lição. Reiniciar, encerrar ou excluir seu cluster pode causar atrasos enquanto você espera pela implantação de novos recursos.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reiniciar, Encerrar e Excluir
# MAGIC
# MAGIC Embora os botões **Reiniciar** (Restart), **Encerrar** (Terminate) e **Excluir** (Delete) tenham efeitos diferentes, todos começam com um evento de encerramento do cluster. (Clusters também serão encerrados automaticamente por inatividade, caso essa configuração esteja habilitada.)
# MAGIC
# MAGIC Quando um cluster é encerrado, todos os recursos em nuvem atualmente em uso são excluídos. Isso significa:
# MAGIC
# MAGIC * As máquinas virtuais associadas e a memória operacional serão liberadas
# MAGIC * O armazenamento em disco anexado será excluído
# MAGIC * As conexões de rede entre os nós serão removidas
# MAGIC
# MAGIC Em resumo, todos os recursos anteriormente associados ao ambiente de computação serão completamente removidos. Isso significa que **quaisquer resultados que precisem ser preservados devem ser salvos em um local permanente**. Observe que você **não perderá seu código**, nem **os arquivos de dados que foram salvos corretamente**.
# MAGIC
# MAGIC O botão **Reiniciar** nos permite reiniciar manualmente nosso cluster. Isso pode ser útil se precisarmos limpar completamente o cache do cluster ou redefinir todo o ambiente de computação.
# MAGIC
# MAGIC O botão **Encerrar** permite parar nosso cluster. As configurações do cluster são mantidas, e podemos usar o botão **Reiniciar** para implantar um novo conjunto de recursos em nuvem com a mesma configuração.
# MAGIC
# MAGIC O botão **Excluir** irá parar o cluster e **remover a configuração do cluster**.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

# COMMAND ----------

print("OLA MUNDO")

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ETL da Última Milha com Databricks SQL
# MAGIC
# MAGIC Antes de continuarmos, vamos relembrar algumas coisas que aprendemos até agora:
# MAGIC 1. O workspace do Databricks contém um conjunto de ferramentas para simplificar o ciclo de vida do desenvolvimento de engenharia de dados
# MAGIC 1. Os notebooks Databricks permitem que usuários misturem SQL com outras linguagens de programação para definir workloads de ETL
# MAGIC 1. O Delta Lake oferece transações ACID e facilita o processamento incremental de dados no Lakehouse
# MAGIC 1. O Delta Live Tables estende a sintaxe SQL para suportar muitos padrões de design no Lakehouse e simplifica o deploy da infraestrutura
# MAGIC 1. Jobs multitarefa permitem orquestração completa de tarefas, adicionando dependências e agendando uma mistura de notebooks e pipelines DLT
# MAGIC 1. O Databricks SQL permite editar e executar consultas SQL, construir visualizações e definir dashboards
# MAGIC 1. O Data Explorer simplifica o gerenciamento de ACLs de tabelas, disponibilizando dados do Lakehouse para analistas SQL (em breve com expansão via Unity Catalog)
# MAGIC
# MAGIC Nesta seção, focaremos em explorar mais funcionalidades do Databricks SQL para suportar workloads de produção.
# MAGIC
# MAGIC Vamos começar focando em usar o Databricks SQL para configurar consultas que suportam ETL da última milha para análises. Note que, embora usemos a interface do Databricks SQL para essa demonstração, os SQL Endpoints <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">integram-se a várias outras ferramentas para permitir execução externa de consultas</a>, além de oferecerem <a href="https://docs.databricks.com/sql/api/index.html" target="_blank">suporte completo via API para execução programática de consultas arbitrárias</a>.
# MAGIC
# MAGIC A partir dos resultados dessas consultas, geraremos uma série de visualizações, que combinaremos em um dashboard.
# MAGIC
# MAGIC Por fim, mostraremos como agendar atualizações para consultas e dashboards, além de configurar alertas para ajudar a monitorar o estado dos datasets de produção ao longo do tempo.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Usar o Databricks SQL como ferramenta para suportar tarefas de ETL de produção que dão suporte a workloads analíticos
# MAGIC * Configurar consultas SQL e visualizações no Editor do Databricks SQL
# MAGIC * Criar dashboards no Databricks SQL
# MAGIC * Agendar atualizações para consultas e dashboards
# MAGIC * Configurar alertas para consultas SQL

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar Script de Configuração
# MAGIC
# MAGIC As células seguintes executam um notebook que define uma classe que usaremos para gerar consultas SQL.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-12.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar um Banco de Dados de Demonstração
# MAGIC
# MAGIC Execute a célula abaixo e copie os resultados para o Editor do Databricks SQL.
# MAGIC
# MAGIC Essas consultas:
# MAGIC * Criam um novo banco de dados
# MAGIC * Declararão duas tabelas (que usaremos para carregar dados)
# MAGIC * Declararão duas funções (que usaremos para gerar dados)
# MAGIC
# MAGIC Depois de copiar, execute a consulta usando o botão **Executar**.

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **NOTA**: As consultas acima devem ser executadas apenas uma vez após o reset completo da demonstração para reconfigurar o ambiente. Usuários precisarão de permissões **`CREATE`** e **`USAGE`** no catálogo para executá-las.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png">  
# MAGIC **AVISO:** Certifique-se de selecionar seu banco de dados antes de continuar, pois o comando **`USE`** ainda não altera o banco contra o qual suas consultas serão executadas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar uma Consulta para Carregar Dados
# MAGIC
# MAGIC Passos:
# MAGIC 1. Execute a célula abaixo para imprimir uma consulta SQL formatada para carregar dados na tabela **`user_ping`** criada anteriormente.
# MAGIC 2. Salve esta consulta com o nome **Load Ping Data**.
# MAGIC 3. Execute essa consulta para carregar um lote de dados.
# MAGIC

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Executar a consulta deve carregar alguns dados e retornar uma pré-visualização da tabela.
# MAGIC
# MAGIC **NOTA**: Números aleatórios são usados para definir e carregar dados, então cada usuário terá valores ligeiramente diferentes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definir Agendamento de Atualização da Consulta
# MAGIC
# MAGIC Passos:
# MAGIC 1. Localize o campo **Refresh Schedule** no canto inferior direito do editor de consulta SQL; clique no azul **Never**
# MAGIC 2. Use o menu para alterar para atualizar a cada **1 minuto**
# MAGIC 3. Para **Ends**, selecione o botão **On**
# MAGIC 4. Escolha a data de amanhã
# MAGIC 5. Clique **OK**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar uma Consulta para Monitorar Total de Registros
# MAGIC
# MAGIC Passos:
# MAGIC 1. Execute a célula abaixo.
# MAGIC 2. Salve esta consulta com o nome **User Counts**.
# MAGIC 3. Execute a consulta para calcular os resultados atuais.

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar uma Visualização de Gráfico de Barras
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão **Add Visualization**, localizado abaixo do botão Refresh Schedule no canto inferior direito da janela de consulta
# MAGIC 2. Clique no nome (deve estar como **`Visualization 1`**) e renomeie para **Total User Records**
# MAGIC 3. Defina **`user_id`** para a **Coluna X**
# MAGIC 4. Defina **`total_records`** para as **Colunas Y**
# MAGIC 5. Clique em **Salvar**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar um Novo Dashboard
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão com três pontos verticais no rodapé da tela e selecione **Add to Dashboard**.
# MAGIC 2. Clique na opção **Create new dashboard**
# MAGIC 3. Nomeie seu dashboard como <strong>User Ping Summary **`<suas_iniciais_aqui>`**</strong>
# MAGIC 4. Clique em **Salvar** para criar o novo dashboard
# MAGIC 5. Seu novo dashboard deve estar selecionado como destino; clique em **OK** para adicionar a visualização
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar uma Consulta para Calcular a Média Recente de Ping
# MAGIC
# MAGIC Passos:
# MAGIC 1. Execute a célula abaixo para imprimir a consulta SQL formatada.
# MAGIC 2. Salve esta consulta com o nome **Avg Ping**.
# MAGIC 3. Execute a consulta para calcular os resultados atuais.
# MAGIC

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adicionar um Gráfico de Linha ao seu Dashboard
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão **Add Visualization**
# MAGIC 2. Clique no nome (deve estar como **`Visualization 1`**) e renomeie para **Avg User Ping**
# MAGIC 3. Selecione **`Line`** para o **Tipo de Visualização**
# MAGIC 4. Defina **`end_time`** para a **Coluna X**
# MAGIC 5. Defina **`avg_ping`** para as **Colunas Y**
# MAGIC 6. Defina **`user_id`** para **Agrupar por**
# MAGIC 7. Clique em **Salvar**
# MAGIC 8. Clique no botão com três pontos verticais no rodapé da tela e selecione **Add to Dashboard**.
# MAGIC 9. Selecione o dashboard que você criou anteriormente
# MAGIC 10. Clique em **OK** para adicionar a visualização
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar uma Consulta para Relatar Estatísticas Resumidas
# MAGIC
# MAGIC Passos:
# MAGIC 1. Execute a célula abaixo.
# MAGIC 2. Salve esta consulta com o nome **Ping Summary**.
# MAGIC 3. Execute a consulta para calcular os resultados atuais.
# MAGIC

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adicionar a Tabela Resumo ao seu Dashboard
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão com três pontos verticais no rodapé da tela e selecione **Add to Dashboard**.
# MAGIC 2. Selecione o dashboard que você criou anteriormente
# MAGIC 3. Clique em **OK** para adicionar a visualização

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar e Atualizar seu Dashboard
# MAGIC
# MAGIC Passos:
# MAGIC 1. Use a barra lateral esquerda para navegar até **Dashboards**
# MAGIC 2. Encontre o dashboard onde adicionou suas consultas
# MAGIC 3. Clique no botão azul **Refresh** para atualizar seu dashboard
# MAGIC 4. Clique no botão **Schedule** para revisar opções de agendamento do dashboard
# MAGIC   * Observe que agendar a atualização do dashboard executará todas as consultas associadas
# MAGIC   * Não agende o dashboard por enquanto
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Compartilhar seu Dashboard
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão azul **Share**
# MAGIC 2. Selecione **All Users** no campo superior
# MAGIC 3. Escolha **Can Run** no campo da direita
# MAGIC 4. Clique em **Add**
# MAGIC 5. Altere as **Credentials** para **Run as viewer**
# MAGIC
# MAGIC **NOTA**: Atualmente, nenhum outro usuário deve ter permissões para executar seu dashboard, pois não receberam permissões para os bancos e tabelas subjacentes via Table ACLs. Se desejar que outros possam atualizar seu dashboard, será necessário conceder permissões para **Run as owner** ou adicionar permissões para as tabelas referenciadas nas suas consultas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configurar um Alerta
# MAGIC
# MAGIC Passos:
# MAGIC 1. Use a barra lateral esquerda para navegar até **Alerts**
# MAGIC 2. Clique em **Create Alert** no canto superior direito
# MAGIC 3. Clique no campo superior esquerdo para dar um nome ao alerta, por exemplo **`<suas_iniciais> Count Check`**
# MAGIC 4. Selecione sua consulta **User Counts**
# MAGIC 5. Configure as opções de **Trigger when**:
# MAGIC   * **Coluna de valor**: **`total_records`**
# MAGIC   * **Condição**: **`>`**
# MAGIC   * **Limite**: **`15`**
# MAGIC 6. Para **Refresh**, selecione **Never**
# MAGIC 7. Clique em **Create Alert**
# MAGIC 8. Na tela seguinte, clique no azul **Refresh** no canto superior direito para avaliar o alerta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar Opções de Destino do Alerta
# MAGIC
# MAGIC Passos:
# MAGIC 1. No preview do alerta, clique no botão azul **Add** à direita de **Destinations** na lateral direita da tela
# MAGIC 2. Na janela que abrir, localize e clique no texto azul em **Create new destinations in Alert Destinations**
# MAGIC 3. Revise as opções disponíveis para envio de alertas
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

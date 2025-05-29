# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Navegando pelo Databricks SQL e Conectando-se a Endpoints
# MAGIC
# MAGIC * Navegue até o Databricks SQL  
# MAGIC   * Certifique-se de que a opção SQL esteja selecionada na barra lateral do workspace (logo abaixo do logotipo do Databricks)
# MAGIC * Certifique-se de que um endpoint SQL esteja ativo e acessível
# MAGIC   * Vá até a seção de endpoints SQL na barra lateral
# MAGIC   * Se um endpoint SQL existir e estiver com o status **`Running`**, você usará esse endpoint
# MAGIC   * Se um endpoint existir mas estiver **`Stopped`**, clique no botão **`Start`** se essa opção estiver disponível (**NOTA**: Inicie o endpoint menor disponível para você)
# MAGIC   * Se nenhum endpoint existir e você tiver a opção, clique em **`Create SQL Endpoint`**; nomeie o endpoint com algo reconhecível e defina o tamanho do cluster como 2X-Small. Deixe todas as outras opções como padrão.
# MAGIC   * Se você não conseguir criar ou se conectar a um endpoint SQL, será necessário contatar o administrador do workspace e solicitar acesso aos recursos de computação do Databricks SQL para continuar.
# MAGIC * Navegue até a página inicial no Databricks SQL
# MAGIC   * Clique no logotipo do Databricks no topo da barra lateral
# MAGIC * Localize os **Dashboards de exemplo** e clique em **`Visit gallery`**
# MAGIC * Clique em **`Import`** ao lado da opção **Retail Revenue & Supply Chain**
# MAGIC   * Supondo que você tenha um endpoint SQL disponível, isso deve carregar um dashboard e exibir resultados imediatamente
# MAGIC   * Clique em **Refresh** no canto superior direito (os dados subjacentes não mudaram, mas esse é o botão usado para atualizar os dados)
# MAGIC
# MAGIC # Atualizando um Dashboard no DBSQL
# MAGIC
# MAGIC * Use o navegador da barra lateral para encontrar a seção **Dashboards**
# MAGIC   * Localize o dashboard de exemplo que você acabou de carregar; ele deve se chamar **Retail Revenue & Supply Chain** e ter seu nome de usuário no campo **`Created By`**. **NOTA**: a opção **My Dashboards** no lado direito pode ser usada como atalho para filtrar outros dashboards do workspace
# MAGIC   * Clique no nome do dashboard para visualizá-lo
# MAGIC * Visualize a consulta por trás do gráfico **Shifts in Pricing Priorities**
# MAGIC   * Passe o mouse sobre o gráfico; três pontos verticais devem aparecer. Clique neles
# MAGIC   * Selecione **View Query** no menu que aparecer
# MAGIC * Revise o código SQL usado para gerar esse gráfico
# MAGIC   * Note que a notação de 3 níveis (namespace) é usada para identificar a tabela de origem; isso é uma prévia da nova funcionalidade que será suportada pelo Unity Catalog
# MAGIC   * Clique em **`Run`** no canto superior direito da tela para visualizar os resultados da consulta
# MAGIC * Revise a visualização
# MAGIC   * Abaixo da consulta, a aba **Table** deve estar selecionada; clique em **Price by Priority over Time** para alternar para a visualização do gráfico
# MAGIC   * Clique em **Edit Visualization** na parte inferior da tela para revisar as configurações
# MAGIC   * Explore como as alterações de configuração afetam sua visualização
# MAGIC   * Se quiser aplicar suas alterações, clique em **Save**; caso contrário, clique em **Cancel**
# MAGIC * De volta ao editor de consulta, clique no botão **Add Visualization** à direita do nome da visualização
# MAGIC   * Crie um gráfico de barras
# MAGIC   * Defina a **Coluna X** como **`Date`**
# MAGIC   * Defina a **Coluna Y** como **`Total Price`**
# MAGIC   * **Agrupar por** **`Priority`**
# MAGIC   * Defina **Empilhamento (Stacking)** como **`Stack`**
# MAGIC   * Deixe todas as outras configurações como padrão
# MAGIC   * Clique em **Save**
# MAGIC * No editor de consulta, clique no nome padrão da visualização para editá-lo; altere o nome da visualização para **`Stacked Price`**
# MAGIC * Na parte inferior da tela, clique nos três pontos verticais à esquerda do botão **`Edit Visualization`**
# MAGIC   * Selecione **Add to Dashboard** no menu
# MAGIC   * Selecione seu dashboard **`Retail Revenue & Supply Chain`**
# MAGIC * Volte ao seu dashboard para visualizar essa alteração
# MAGIC
# MAGIC # Criar uma Nova Consulta
# MAGIC
# MAGIC * Use a barra lateral para navegar até **Queries**
# MAGIC * Clique no botão **`Create Query`**
# MAGIC * Certifique-se de estar conectado a um endpoint. No **Schema Browser**, clique no metastore atual e selecione **`samples`**
# MAGIC   * Selecione o banco de dados **`tpch`**
# MAGIC   * Clique na tabela **`partsupp`** para ver uma prévia do esquema
# MAGIC   * Ao passar o mouse sobre o nome da tabela **`partsupp`**, clique no botão **>>** para inserir o nome da tabela no texto da sua consulta
# MAGIC * Escreva sua primeira consulta:
# MAGIC   * **`SELECT * FROM`** a tabela **`partsupp`** usando o nome completo importado na etapa anterior; clique em **Run** para visualizar os resultados
# MAGIC   * Modifique essa consulta para **`GROUP BY ps_partkey`** e retorne os campos **`ps_partkey`** e **`sum(ps_availqty)`**; clique em **Run** para visualizar os resultados
# MAGIC   * Atualize sua consulta para usar um alias na segunda coluna com o nome **`total_availqty`** e execute a consulta novamente
# MAGIC * Salve sua consulta
# MAGIC   * Clique no botão **Save** ao lado de **Run**, no canto superior direito da tela
# MAGIC   * Dê um nome para a consulta que você consiga lembrar
# MAGIC * Adicione a consulta ao seu dashboard
# MAGIC   * Clique nos três botões verticais na parte inferior da tela
# MAGIC   * Clique em **Add to Dashboard**
# MAGIC   * Selecione seu dashboard **`Retail Revenue & Supply Chain`**
# MAGIC * Volte ao seu dashboard para visualizar essa alteração
# MAGIC   * Se desejar reorganizar as visualizações, clique nos três botões verticais no canto superior direito da tela; clique em **Edit** no menu que aparecer e você poderá arrastar e redimensionar as visualizações
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

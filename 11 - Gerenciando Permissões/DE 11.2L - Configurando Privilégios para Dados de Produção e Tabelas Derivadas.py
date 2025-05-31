# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Configurando Privilégios para Dados de Produção e Tabelas Derivadas
# MAGIC
# MAGIC As instruções abaixo são fornecidas para pares de usuários explorarem como funcionam as ACLs de tabelas no Databricks. Elas utilizam o Databricks SQL e o Data Explorer para realizar essas tarefas, e assumem que nenhum dos usuários possui privilégios de administrador no workspace. Um administrador precisará ter concedido previamente privilégios **`CREATE`** e **`USAGE`** em um catálogo para que os usuários possam criar bancos de dados no Databricks SQL.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC * Usar o Data Explorer para navegar por entidades relacionais
# MAGIC * Configurar permissões para tabelas e views com o Data Explorer
# MAGIC * Configurar permissões mínimas para permitir descoberta e consulta de tabelas
# MAGIC * Alterar propriedade de bancos de dados, tabelas e views criados no DBSQL

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.2L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Troque os Nomes de Usuário com seu Parceiro
# MAGIC
# MAGIC Se você não está em um workspace onde seu nome de usuário corresponda ao seu endereço de e-mail, certifique-se de que seu parceiro tenha seu nome de usuário.
# MAGIC
# MAGIC Eles precisarão disso para atribuir privilégios e buscar seu banco de dados nas etapas seguintes.
# MAGIC
# MAGIC A célula abaixo imprimirá seu nome de usuário.

# COMMAND ----------

print(f"Your username: {DA.username}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gerar Instruções de Configuração
# MAGIC
# MAGIC A célula a seguir usa Python para extrair o nome de usuário do usuário atual e formatar várias instruções usadas para criar bancos de dados, tabelas e views.
# MAGIC
# MAGIC Ambos os estudantes devem executar a célula abaixo.
# MAGIC
# MAGIC A execução bem-sucedida imprimirá uma série de consultas SQL formatadas, que podem ser copiadas para o editor de consultas DBSQL e executadas.

# COMMAND ----------

DA.generate_query()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Passos:
# MAGIC 1. Execute a célula acima
# MAGIC 2. Copie toda a saída para a área de transferência
# MAGIC 3. Navegue até o workspace do Databricks SQL
# MAGIC 4. Certifique-se de que um endpoint DBSQL esteja em execução
# MAGIC 5. Use a barra lateral esquerda para selecionar o **Editor SQL**
# MAGIC 6. Cole a consulta copiada e clique no botão azul **Executar** no canto superior direito
# MAGIC
# MAGIC **NOTA**: Você precisará estar conectado a um endpoint DBSQL para executar essas consultas com sucesso. Se não conseguir se conectar, deverá contatar o administrador para obter acesso.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Encontre Seu Banco de Dados
# MAGIC
# MAGIC No Data Explorer, localize o banco de dados criado anteriormente (ele deve seguir o padrão **`dbacademy_<nome_do_usuario>_dewd_acls_lab`**).
# MAGIC
# MAGIC Clicar no nome do banco de dados exibirá uma lista das tabelas e views contidas no lado esquerdo.
# MAGIC
# MAGIC À direita, você verá detalhes sobre o banco, incluindo **Proprietário** e **Localização**.
# MAGIC
# MAGIC Clique na aba **Permissões** para revisar quem atualmente possui permissões (dependendo da configuração do workspace, algumas permissões podem ter sido herdadas das configurações do catálogo).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alterar Permissões do Banco de Dados
# MAGIC
# MAGIC Passos:
# MAGIC 1. Certifique-se de que a aba **Permissões** esteja selecionada para o banco de dados
# MAGIC 2. Clique no botão azul **Conceder**
# MAGIC 3. Selecione as opções **USAGE**, **SELECT** e **READ_METADATA**
# MAGIC 4. Insira o nome de usuário do seu parceiro no campo no topo
# MAGIC 5. Clique em **OK**
# MAGIC
# MAGIC Confirme com seu parceiro que vocês podem ver os bancos de dados e tabelas um do outro.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar uma Consulta para Confirmar
# MAGIC
# MAGIC Ao conceder **`USAGE`**, **`SELECT`** e **`READ_METADATA`** no seu banco de dados, seu parceiro agora deve poder consultar livremente as tabelas e views neste banco, mas não poderá criar novas tabelas nem modificar seus dados.
# MAGIC
# MAGIC No Editor SQL, cada usuário deve executar uma série de consultas para confirmar esse comportamento no banco de dados ao qual foi adicionado.
# MAGIC
# MAGIC **Certifique-se de especificar o banco de dados do seu parceiro ao executar as consultas abaixo.**
# MAGIC
# MAGIC **NOTA**: As três primeiras consultas devem ser bem-sucedidas, mas a última deve falhar.

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_confirmation_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar Consulta para Gerar a União dos seus Beans
# MAGIC
# MAGIC Execute a consulta abaixo contra seus próprios bancos de dados.
# MAGIC
# MAGIC **NOTA**: Como valores aleatórios foram inseridos nas colunas **`grams`** e **`delicious`**, você deve ver 2 linhas distintas para cada par **`name`**, **`color`**.

# COMMAND ----------

DA.generate_union_query()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Registrar uma View Derivada no Seu Banco de Dados
# MAGIC
# MAGIC Execute a consulta abaixo para registrar os resultados da consulta anterior no seu banco de dados.

# COMMAND ----------

DA.generate_derivative_view()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Consultar a View do seu Parceiro
# MAGIC
# MAGIC Após seu parceiro completar a etapa anterior, execute a seguinte consulta em cada uma das suas tabelas; vocês devem obter os mesmos resultados:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_partner_view("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adicionar Permissões de Modificação
# MAGIC
# MAGIC Agora tente dropar as tabelas **`beans`** um do outro.
# MAGIC
# MAGIC No momento, isso não deve funcionar.
# MAGIC
# MAGIC Usando o Data Explorer, adicione a permissão **`MODIFY`** para a tabela **`beans`** do seu parceiro.
# MAGIC
# MAGIC Novamente, tente dropar a tabela **`beans`** do seu parceiro.
# MAGIC
# MAGIC Isso deve falhar novamente.
# MAGIC
# MAGIC **Apenas o proprietário da tabela deve poder emitir esse comando**.<br/>
# MAGIC (Observe que a propriedade pode ser transferida de um indivíduo para um grupo, se desejado).
# MAGIC
# MAGIC Em vez disso, execute uma consulta para deletar registros da tabela do seu parceiro:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_delete_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Essa consulta deve excluir com sucesso todos os registros da tabela alvo.
# MAGIC
# MAGIC Tente executar novamente consultas contra quaisquer views ou tabelas que você havia consultado anteriormente neste laboratório.
# MAGIC
# MAGIC **NOTA**: Se os passos foram concluídos com sucesso, nenhuma das suas consultas anteriores deve retornar resultados, pois os dados referenciados pelas views foram deletados. Isso demonstra os riscos associados a fornecer privilégios **`MODIFY`** a usuários em dados usados em aplicações e dashboards de produção.
# MAGIC
# MAGIC Se tiver tempo adicional, veja se consegue usar os métodos Delta **`DESCRIBE HISTORY`** e **`RESTORE`** para reverter os registros na sua tabela.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logo Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Gerenciando Permissões para Bancos de Dados, Tabelas e Visões
# MAGIC
# MAGIC As instruções detalhadas abaixo foram fornecidas para que grupos de usuários explorem como funcionam as ACLs de Tabela no Databricks. Elas utilizam o Databricks SQL e o Data Explorer para realizar essas tarefas, assumindo que ao menos um usuário do grupo tenha status de administrador (ou que um administrador tenha previamente configurado permissões para permitir que os usuários criem bancos de dados, tabelas e visões).
# MAGIC
# MAGIC Como está escrito, estas instruções devem ser concluídas pelo usuário administrador. O notebook seguinte terá um exercício semelhante para os usuários realizarem em duplas.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Descrever as permissões padrão para usuários e administradores no DBSQL
# MAGIC * Identificar o proprietário padrão de bancos de dados, tabelas e visões criados no DBSQL e alterar a propriedade
# MAGIC * Usar o Data Explorer para navegar pelas entidades relacionais
# MAGIC * Configurar permissões para tabelas e visões com o Data Explorer
# MAGIC * Configurar permissões mínimas para permitir descoberta e consulta de tabelas

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gerar Instruções de Configuração
# MAGIC
# MAGIC A célula a seguir usa Python para extrair o nome de usuário atual e formatá-lo em diversas instruções utilizadas para criar bancos de dados, tabelas e visões.
# MAGIC
# MAGIC Apenas o administrador precisa executar a célula abaixo. A execução bem-sucedida imprimirá uma série de consultas SQL formatadas, que podem ser copiadas e coladas no editor de consultas do DBSQL e executadas.

# COMMAND ----------

DA.generate_users_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Execute a célula acima
# MAGIC 1. Copie toda a saída para sua área de transferência
# MAGIC 1. Navegue até o workspace do Databricks SQL
# MAGIC 1. Certifique-se de que um endpoint DBSQL esteja em execução
# MAGIC 1. Use a barra lateral esquerda para selecionar o **SQL Editor**
# MAGIC 1. Cole a consulta copiada e clique no botão azul **Run** no canto superior direito
# MAGIC
# MAGIC **OBSERVAÇÃO**: Você precisa estar conectado a um endpoint DBSQL para executar essas consultas com sucesso. Se não conseguir se conectar, entre em contato com seu administrador para obter acesso.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Usando o Data Explorer
# MAGIC
# MAGIC * Use o navegador da barra lateral esquerda para selecionar a aba **Data**; isso o levará ao **Data Explorer**
# MAGIC
# MAGIC ## O que é o Data Explorer?
# MAGIC
# MAGIC O Data Explorer permite que usuários e administradores:
# MAGIC * Naveguem por bancos de dados, tabelas e visões
# MAGIC * Explore o esquema de dados, metadados e histórico
# MAGIC * Definam e modifiquem permissões de entidades relacionais
# MAGIC
# MAGIC Observe que, no momento em que estas instruções foram escritas, o Unity Catalog ainda não está geralmente disponível. A funcionalidade de nomes em três níveis que ele adiciona pode ser visualizada, em certa medida, alternando entre o **`hive_metastore`** padrão e o catálogo **`sample`** usado para painéis e consultas de exemplo. Espere que a interface e as funcionalidades do Data Explorer evoluam à medida que o Unity Catalog for adicionado aos workspaces.
# MAGIC
# MAGIC ## Configurando Permissões
# MAGIC
# MAGIC Por padrão, administradores terão a capacidade de visualizar todos os objetos registrados no metastore e poderão controlar permissões para outros usuários no workspace. Os usuários, por padrão, **não** terão permissões em nada registrado no metastore, exceto nos objetos que criarem no DBSQL; observe que, antes de poderem criar bancos de dados, tabelas ou visões, os usuários precisam ter privilégios de criação e uso explicitamente concedidos.
# MAGIC
# MAGIC Geralmente, as permissões são configuradas usando Grupos que foram definidos por um administrador, muitas vezes importando estruturas organizacionais via integração SCIM com um provedor de identidade. Esta lição explorará as ACLs usadas para controlar permissões, mas usando indivíduos em vez de grupos.
# MAGIC
# MAGIC ## ACLs de Tabela
# MAGIC
# MAGIC O Databricks permite configurar permissões para os seguintes objetos:
# MAGIC
# MAGIC | Objeto | Escopo |
# MAGIC | --- | --- |
# MAGIC | CATALOG | controla acesso ao catálogo de dados inteiro |
# MAGIC | DATABASE | controla acesso a um banco de dados |
# MAGIC | TABLE | controla acesso a uma tabela gerenciada ou externa |
# MAGIC | VIEW | controla acesso a visões SQL |
# MAGIC | FUNCTION | controla acesso a funções nomeadas |
# MAGIC | ANY FILE | controla acesso ao sistema de arquivos subjacente. Usuários com acesso ao ANY FILE podem contornar restrições impostas ao catálogo, bancos de dados, tabelas e visões lendo diretamente do sistema de arquivos. |
# MAGIC
# MAGIC **OBSERVAÇÃO**: No momento, o objeto **`ANY FILE`** não pode ser configurado via Data Explorer.
# MAGIC
# MAGIC ## Concessão de Privilégios
# MAGIC
# MAGIC Administradores e proprietários de objetos no Databricks podem conceder privilégios de acordo com as seguintes regras:
# MAGIC
# MAGIC | Função | Pode conceder privilégios para |
# MAGIC | --- | --- |
# MAGIC | Administrador do Databricks | Todos os objetos no catálogo e sistema de arquivos subjacente |
# MAGIC | Proprietário do catálogo | Todos os objetos no catálogo |
# MAGIC | Proprietário do banco de dados | Todos os objetos no banco de dados |
# MAGIC | Proprietário da tabela | Somente para a tabela (semelhante para views e funções) |
# MAGIC
# MAGIC **OBSERVAÇÃO**: Atualmente, o Data Explorer só pode ser usado para modificar a propriedade de bancos de dados, tabelas e visões. Permissões de catálogo podem ser configuradas interativamente no SQL Editor.
# MAGIC
# MAGIC ## Privilégios
# MAGIC
# MAGIC Os seguintes privilégios podem ser configurados no Data Explorer:
# MAGIC
# MAGIC | Privilégio | Capacidade |
# MAGIC | --- | --- |
# MAGIC | ALL PRIVILEGES | concede todos os privilégios (equivale a todos os abaixo) |
# MAGIC | SELECT | concede acesso de leitura a um objeto |
# MAGIC | MODIFY | permite adicionar, excluir e modificar dados em um objeto |
# MAGIC | READ_METADATA | permite visualizar um objeto e seus metadados |
# MAGIC | USAGE | não concede nenhuma capacidade diretamente, mas é um requisito adicional para realizar qualquer ação em objetos de banco de dados |
# MAGIC | CREATE | permite criar objetos (por exemplo, uma tabela em um banco de dados) |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Revisar Permissões Padrão
# MAGIC No Data Explorer, encontre o banco de dados que você criou anteriormente (deve seguir o padrão **`dbacademy_<username>_acls_demo`**).
# MAGIC
# MAGIC Clicar no nome do banco de dados deve exibir uma lista de tabelas e visões à esquerda. À direita, serão exibidos detalhes como **Proprietário** e **Localização**.
# MAGIC
# MAGIC Clique na aba **Permissions** para revisar quem tem permissões atualmente (dependendo da configuração do seu workspace, algumas permissões podem ter sido herdadas do catálogo).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Atribuindo Propriedade
# MAGIC
# MAGIC Clique no ícone de lápis azul ao lado do campo **Owner**. Observe que um proprietário pode ser um indivíduo **ou** um grupo. Para a maioria das implementações, ter um ou vários pequenos grupos de usuários avançados confiáveis como proprietários limita o acesso administrativo a conjuntos de dados importantes, enquanto garante que um único usuário não se torne um gargalo de produtividade.
# MAGIC
# MAGIC Aqui, vamos definir o proprietário como **Admins**, que é um grupo padrão contendo todos os administradores do workspace.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alterar Permissões do Banco de Dados
# MAGIC
# MAGIC Comece permitindo que todos os usuários revisem metadados sobre o banco de dados.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Certifique-se de que a aba **Permissions** esteja selecionada para o banco de dados
# MAGIC 1. Clique no botão azul **Grant**
# MAGIC 1. Selecione as opções **USAGE** e **READ_METADATA**
# MAGIC 1. Selecione o grupo **All Users** no menu suspenso no topo
# MAGIC 1. Clique em **OK**
# MAGIC
# MAGIC Os usuários podem precisar atualizar sua visualização para ver as permissões atualizadas. As atualizações devem ser refletidas em tempo quase real tanto no Data Explorer quanto no SQL Editor.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alterar Permissões da View
# MAGIC
# MAGIC Agora que os usuários podem ver informações sobre esse banco de dados, eles ainda não conseguem interagir com a tabela ou view declarada anteriormente.
# MAGIC
# MAGIC Vamos começar dando aos usuários a capacidade de consultar nossa view.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Selecione a view **`ny_users_vw`**
# MAGIC 1. Vá para a aba **Permissions**
# MAGIC    * Os usuários devem ter herdado as permissões concedidas ao nível do banco de dados; será possível ver quais permissões os usuários possuem atualmente sobre o recurso, bem como de onde essa permissão foi herdada
# MAGIC 1. Clique no botão azul **Grant**
# MAGIC 1. Selecione as opções **SELECT** e **READ_METADATA**
# MAGIC    * **READ_METADATA** é tecnicamente redundante, pois os usuários já a herdaram do banco de dados. No entanto, concedê-la no nível da view garante que os usuários ainda tenham essa permissão mesmo que as permissões do banco de dados sejam revogadas
# MAGIC 1. Selecione o grupo **All Users** no menu suspenso
# MAGIC 1. Clique em **OK**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar uma Consulta para Confirmar
# MAGIC
# MAGIC No **SQL Editor**, todos os usuários devem usar o **Schema Browser** à esquerda para navegar até o banco de dados controlado pelo administrador.
# MAGIC
# MAGIC Os usuários devem iniciar uma consulta digitando **`SELECT * FROM`** e, em seguida, clicar no **>>** que aparece ao passar o mouse sobre o nome da view para inseri-la na consulta.
# MAGIC
# MAGIC Essa consulta deve retornar 2 resultados.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Esta view é definida sobre a tabela **`users`**, que ainda não teve permissões configuradas. Observe que os usuários têm acesso apenas à parte dos dados que passam pelos filtros definidos na view; esse padrão demonstra como uma única tabela pode ser usada para fornecer acesso controlado a partes específicas dos dados para diferentes stakeholders.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Alterar Permissões da Tabela
# MAGIC
# MAGIC Realize os mesmos passos acima, agora para a tabela **`users`**.
# MAGIC
# MAGIC Etapas:
# MAGIC 1. Selecione a tabela **`users`**
# MAGIC 1. Vá para a aba **Permissions**
# MAGIC 1. Clique no botão azul **Grant**
# MAGIC 1. Selecione as opções **SELECT** e **READ_METADATA**
# MAGIC 1. Selecione o grupo **All Users**
# MAGIC 1. Clique em **OK**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Fazer os Usuários Tentarem o Comando **`DROP TABLE`**
# MAGIC
# MAGIC No **SQL Editor**, incentive os usuários a explorar os dados nesta tabela.
# MAGIC
# MAGIC Incentive-os a tentar modificar os dados; assumindo que as permissões foram corretamente configuradas, esses comandos devem gerar erro.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Criar um Banco de Dados para Conjuntos de Dados Derivados
# MAGIC
# MAGIC Na maioria dos casos, os usuários precisarão de um local para salvar os conjuntos de dados derivados. Atualmente, os usuários podem não ter permissão para criar novas tabelas em nenhum local (dependendo das ACLs já existentes).
# MAGIC
# MAGIC A célula abaixo imprime o código para gerar um novo banco de dados e conceder permissões a todos os usuários.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Aqui configuramos permissões usando o SQL Editor, em vez do Data Explorer. Você pode revisar o Histórico de Consultas para observar que todas as alterações anteriores feitas no Data Explorer foram, na verdade, executadas como consultas SQL e registradas ali (além disso, a maioria das ações no Data Explorer são registradas junto com a consulta SQL correspondente que preencheu os campos da interface).
# MAGIC

# COMMAND ----------

DA.generate_create_database_with_grants()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Permitir que os Usuários Criem Novas Tabelas ou Views
# MAGIC
# MAGIC Dê um momento para que os usuários testem se conseguem criar tabelas e views neste novo banco de dados.
# MAGIC
# MAGIC **OBSERVAÇÃO**: como os usuários também receberam permissões de **MODIFY** e **SELECT**, todos os usuários poderão consultar e modificar imediatamente os objetos criados por seus colegas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuração do Administrador
# MAGIC
# MAGIC Atualmente, os usuários não possuem nenhuma permissão de ACL de Tabela no catálogo padrão **`hive_metastore`** por padrão. O próximo laboratório assume que os usuários poderão criar bancos de dados.
# MAGIC
# MAGIC Para permitir a criação de bancos de dados e tabelas no catálogo padrão via Databricks SQL, peça a um administrador que execute o seguinte comando no editor de consultas do DBSQL:
# MAGIC
# MAGIC <strong><code>GRANT usage, create ON CATALOG &#x60;hive_metastore&#x60; TO &#x60;users&#x60;</code></strong>
# MAGIC
# MAGIC Para confirmar a execução com sucesso, execute a seguinte consulta:
# MAGIC
# MAGIC <strong><code>SHOW GRANT ON CATALOG &#x60;hive_metastore&#x60;</code></strong>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

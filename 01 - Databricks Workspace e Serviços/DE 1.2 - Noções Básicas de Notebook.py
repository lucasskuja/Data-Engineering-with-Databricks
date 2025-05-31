# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Noções Básicas sobre Notebooks
# MAGIC
# MAGIC Notebooks são o principal meio de desenvolver e executar código de forma interativa no Databricks. Esta lição fornece uma introdução básica ao uso de notebooks no Databricks.
# MAGIC
# MAGIC Se você já usou notebooks do Databricks antes, mas esta é a sua primeira vez executando um notebook em **Databricks Repos**, perceberá que a funcionalidade básica continua a mesma. Na próxima lição, revisaremos algumas funcionalidades adicionais que os Repos do Databricks trazem aos notebooks.
# MAGIC
# MAGIC ## Objetivos de Aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Vincular um notebook a um cluster
# MAGIC * Executar uma célula em um notebook
# MAGIC * Definir a linguagem de um notebook
# MAGIC * Descrever e utilizar comandos mágicos (magic commands)
# MAGIC * Criar e executar uma célula SQL
# MAGIC * Criar e executar uma célula Python
# MAGIC * Criar uma célula Markdown
# MAGIC * Exportar um notebook do Databricks
# MAGIC * Exportar uma coleção de notebooks do Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conectar a um Cluster
# MAGIC
# MAGIC Na lição anterior, você deve ter implantado um cluster ou identificado um cluster que um administrador configurou para seu uso.
# MAGIC
# MAGIC Logo abaixo do nome deste notebook, na parte superior da sua tela, use a lista suspensa para conectar este notebook ao seu cluster.
# MAGIC
# MAGIC **OBSERVAÇÃO**: A implantação de um cluster pode levar vários minutos. Uma seta verde aparecerá à direita do nome do cluster assim que os recursos forem implantados.
# MAGIC Se o seu cluster tiver um círculo cinza sólido à esquerda, você precisará seguir as instruções para <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">iniciar um cluster</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Noções Básicas sobre Notebooks
# MAGIC
# MAGIC Notebooks permitem a execução de código célula por célula. Múltiplas linguagens podem ser combinadas em um mesmo notebook. Usuários podem adicionar gráficos, imagens e texto em markdown para enriquecer seu código.
# MAGIC
# MAGIC Ao longo deste curso, nossos notebooks foram projetados como instrumentos de aprendizado. No entanto, notebooks também podem ser facilmente implantados como código de produção no Databricks, além de oferecer um conjunto robusto de ferramentas para exploração de dados, geração de relatórios e criação de dashboards.
# MAGIC
# MAGIC ### Executando uma Célula
# MAGIC * Execute a célula abaixo usando uma das seguintes opções:
# MAGIC   * **CTRL+ENTER** ou **CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER** ou **SHIFT+RETURN** para executar a célula e ir para a próxima
# MAGIC   * Usando **Run Cell**, **Run All Above** ou **Run All Below**

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **OBSERVAÇÃO**: A execução de código célula por célula significa que as células podem ser executadas várias vezes ou fora de ordem. A menos que seja instruído explicitamente, você deve sempre assumir que os notebooks deste curso devem ser executados uma célula por vez, de cima para baixo.
# MAGIC
# MAGIC Se você encontrar um erro, leia o texto antes e depois da célula para garantir que o erro não foi incluído intencionalmente como parte do aprendizado, antes de tentar resolver.
# MAGIC
# MAGIC A maioria dos erros pode ser resolvida executando células anteriores que foram ignoradas ou reexecutando o notebook inteiro desde o início.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Definindo a Linguagem Padrão do Notebook
# MAGIC
# MAGIC A célula acima executa um comando em Python, pois a linguagem padrão atual deste notebook está definida como Python.
# MAGIC
# MAGIC Os notebooks do Databricks suportam Python, SQL, Scala e R. Uma linguagem pode ser escolhida no momento da criação do notebook, mas isso pode ser alterado a qualquer momento.
# MAGIC
# MAGIC A linguagem padrão aparece diretamente à direita do título do notebook, no topo da página. Ao longo deste curso, usaremos uma combinação de notebooks em SQL e Python.
# MAGIC
# MAGIC Vamos alterar a linguagem padrão deste notebook para SQL.
# MAGIC
# MAGIC Passos:
# MAGIC * Clique em **Python**, ao lado do título do notebook, no topo da tela
# MAGIC * Na interface que aparecer, selecione **SQL** na lista suspensa
# MAGIC
# MAGIC **OBSERVAÇÃO**: Na célula logo antes desta, você deve ver uma nova linha aparecer com <strong><code>&#37;python</code></strong>. Vamos falar sobre isso em breve.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Criar e Executar uma Célula SQL
# MAGIC
# MAGIC * Selecione esta célula e pressione a tecla **B** no teclado para criar uma nova célula abaixo
# MAGIC * Copie o código abaixo para a nova célula e, em seguida, execute-a
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "Estou executando SQL!"`**
# MAGIC
# MAGIC **OBSERVAÇÃO**: Existem diversos métodos para adicionar, mover e excluir células, incluindo opções na interface gráfica e atalhos de teclado. Consulte a <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">documentação</a> para mais detalhes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Comandos Mágicos (Magic Commands)
# MAGIC * Comandos mágicos são específicos dos notebooks do Databricks
# MAGIC * Eles são muito semelhantes aos comandos mágicos encontrados em outros ambientes de notebooks semelhantes
# MAGIC * São comandos integrados que produzem o mesmo resultado independentemente da linguagem do notebook
# MAGIC * Um símbolo de porcentagem (%) no início de uma célula identifica um comando mágico
# MAGIC   * Só é possível ter **um único** comando mágico por célula
# MAGIC   * Um comando mágico deve ser **a primeira linha** da célula
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Comandos Mágicos de Linguagem
# MAGIC
# MAGIC Os comandos mágicos de linguagem permitem a execução de código em linguagens diferentes da linguagem padrão do notebook. Neste curso, veremos os seguintes comandos mágicos de linguagem:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC Adicionar o comando mágico correspondente à linguagem padrão atual do notebook **não é necessário**.
# MAGIC
# MAGIC Quando alteramos a linguagem do notebook de Python para SQL acima, as células existentes escritas em Python passaram a incluir o comando <strong><code>&#37;python</code></strong>.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Em vez de mudar constantemente a linguagem padrão de um notebook, é recomendável manter uma linguagem principal como padrão e utilizar comandos mágicos apenas quando for necessário executar código em outra linguagem.
# MAGIC

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Markdown
# MAGIC
# MAGIC O comando mágico **&percnt;md** nos permite renderizar Markdown em uma célula:
# MAGIC * Dê um clique duplo nesta célula para começar a editá-la
# MAGIC * Em seguida, pressione **`Esc`** para parar de editar
# MAGIC
# MAGIC # Título Um
# MAGIC ## Título Dois
# MAGIC ### Título Três
# MAGIC
# MAGIC Este é um teste do sistema de transmissão de emergência. Isto é apenas um teste.
# MAGIC
# MAGIC Este é um texto com uma palavra em **negrito**.
# MAGIC
# MAGIC Este é um texto com uma palavra em *itálico*.
# MAGIC
# MAGIC Esta é uma lista ordenada
# MAGIC 0. um
# MAGIC 0. dois
# MAGIC 0. três
# MAGIC
# MAGIC Esta é uma lista não ordenada
# MAGIC * maçãs
# MAGIC * pêssegos
# MAGIC * bananas
# MAGIC
# MAGIC Links/HTML incorporado: <a href="https://pt.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipédia</a>
# MAGIC
# MAGIC Imagens:
# MAGIC ![Spark Engines](https://spark.apache.org/images/spark-logo-rev.svg)
# MAGIC
# MAGIC E claro, tabelas:
# MAGIC
# MAGIC | nome   | valor |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC * Você pode executar um notebook a partir de outro notebook usando o comando mágico **%run**
# MAGIC * Os notebooks a serem executados são especificados com caminhos relativos
# MAGIC * O notebook referenciado é executado como se fosse parte do notebook atual, portanto, visualizações temporárias e outras declarações locais estarão disponíveis no notebook que chamou
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Descomentar e executar a célula a seguir gerará o seguinte erro:<br/>
# MAGIC **`Erro na instrução SQL: AnalysisException: Tabela ou visão não encontrada: demo_tmp_vw`**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Mas podemos declará-la, assim como algumas outras variáveis e funções, executando esta célula:
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O notebook **`../Includes/Classroom-Setup-1.2`** que referenciamos inclui lógica para criar e **`USAR`** um banco de dados, além de criar a visão temporária **`demo_temp_vw`**.
# MAGIC
# MAGIC Podemos verificar que essa visão temporária está disponível na sessão atual do notebook com a seguinte consulta.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Usaremos esse padrão de notebooks de "configuração" ao longo do curso para ajudar a preparar o ambiente para as aulas e laboratórios.
# MAGIC
# MAGIC Essas variáveis, funções e outros objetos "fornecidos" devem ser facilmente identificáveis, pois fazem parte do objeto **`DA`**, que é uma instância de **`DBAcademyHelper`**.
# MAGIC
# MAGIC Com isso em mente, a maioria das lições usará variáveis derivadas do seu nome de usuário para organizar arquivos e bancos de dados.
# MAGIC
# MAGIC Esse padrão nos permite evitar conflitos com outros usuários em um ambiente de trabalho compartilhado.
# MAGIC
# MAGIC A célula abaixo usa Python para imprimir algumas dessas variáveis definidas anteriormente no script de configuração deste notebook:
# MAGIC

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.db_name:           {DA.db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Além disso, essas mesmas variáveis são "injetadas" no contexto SQL para que possamos usá-las em instruções SQL.
# MAGIC
# MAGIC Falaremos mais sobre isso mais adiante, mas você pode ver um exemplo rápido na célula seguinte.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Note a diferença sutil, mas importante, na forma das letras das palavras **`da`** e **`DA`** nesses dois exemplos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.db_name}' as database_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Utilitários do Databricks
# MAGIC Os notebooks do Databricks fornecem diversos comandos utilitários para configurar e interagir com o ambiente: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">documentação do dbutils</a>
# MAGIC
# MAGIC Ao longo deste curso, usaremos ocasionalmente o comando **`dbutils.fs.ls()`** para listar diretórios de arquivos a partir de células Python.
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC Ao executar consultas SQL a partir das células, os resultados sempre serão exibidos em formato tabular renderizado.
# MAGIC
# MAGIC Quando temos dados tabulares retornados por uma célula Python, podemos usar o comando **`display`** para obter o mesmo tipo de visualização.
# MAGIC
# MAGIC Aqui, vamos envolver o comando de listagem anterior do nosso sistema de arquivos com **`display`**.
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O comando **`display()`** possui as seguintes funcionalidades e limitações:
# MAGIC * Visualização dos resultados limitada a 1000 registros
# MAGIC * Oferece botão para baixar os dados dos resultados em formato CSV
# MAGIC * Permite renderizar gráficos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Baixando Notebooks
# MAGIC
# MAGIC Existem várias opções para baixar notebooks individuais ou coleções de notebooks.
# MAGIC
# MAGIC Aqui, você passará pelo processo para baixar este notebook, bem como uma coleção de todos os notebooks deste curso.
# MAGIC
# MAGIC ### Baixar um Notebook
# MAGIC
# MAGIC Passos:
# MAGIC * Clique na opção **File** (Arquivo) à direita da seleção do cluster no topo do notebook
# MAGIC * No menu que aparecer, passe o mouse sobre **Export** (Exportar) e selecione **Source File** (Arquivo Fonte)
# MAGIC
# MAGIC O notebook será baixado para o seu computador pessoal. Ele será nomeado com o nome atual do notebook e terá a extensão de arquivo do idioma padrão. Você pode abrir esse notebook com qualquer editor de texto e ver o conteúdo bruto dos notebooks do Databricks.
# MAGIC
# MAGIC Esses arquivos fonte podem ser carregados em qualquer workspace do Databricks.
# MAGIC
# MAGIC ### Baixar uma Coleção de Notebooks
# MAGIC
# MAGIC **NOTA**: As instruções a seguir assumem que você importou esses materiais usando **Repos**.
# MAGIC
# MAGIC Passos:
# MAGIC * Clique no ícone ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** na barra lateral esquerda
# MAGIC   * Isso deve mostrar uma prévia dos diretórios pai deste notebook
# MAGIC * No lado esquerdo da prévia do diretório, aproximadamente no meio da tela, deve haver uma seta para cima. Clique nela para subir na hierarquia de arquivos.
# MAGIC * Você deverá ver um diretório chamado **Data Engineering with Databricks**. Clique na seta para baixo/chevron para abrir um menu
# MAGIC * No menu, passe o mouse sobre **Export** e selecione **DBC Archive**
# MAGIC
# MAGIC O arquivo DBC (Databricks Cloud) que será baixado contém uma coleção compactada dos diretórios e notebooks deste curso. Os usuários não devem tentar editar esses arquivos DBC localmente, mas eles podem ser carregados com segurança em qualquer workspace do Databricks para mover ou compartilhar o conteúdo dos notebooks.
# MAGIC
# MAGIC **NOTA**: Ao baixar uma coleção de arquivos DBC, pré-visualizações de resultados e gráficos também serão exportados. Ao baixar notebooks fonte, somente o código será salvo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Aprendendo Mais
# MAGIC
# MAGIC Incentivamos você a explorar a documentação para aprender mais sobre os diversos recursos da plataforma Databricks e dos notebooks.
# MAGIC
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">Guia do Usuário</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Introdução ao Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">Guia do Usuário / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importando notebooks - Formatos Suportados</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Guia de Administração</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Configuração de Clusters</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Notas de Versão</a>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mais uma observação!
# MAGIC
# MAGIC Ao final de cada lição, você verá o seguinte comando, **`DA.cleanup()`**.
# MAGIC
# MAGIC Este método exclui bancos de dados específicos da lição e diretórios de trabalho na tentativa de manter seu workspace limpo e preservar a imutabilidade de cada lição.
# MAGIC
# MAGIC Execute a célula a seguir para deletar as tabelas e arquivos associados a esta lição.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

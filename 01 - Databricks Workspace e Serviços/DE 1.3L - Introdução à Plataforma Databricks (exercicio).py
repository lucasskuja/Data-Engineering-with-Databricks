# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Começando com a Plataforma Databricks
# MAGIC
# MAGIC Este notebook oferece uma revisão prática de algumas funcionalidades básicas do Workspace de Ciência de Dados e Engenharia do Databricks.
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC - Renomear um notebook e alterar a linguagem padrão
# MAGIC - Anexar um cluster
# MAGIC - Usar o comando mágico **`%run`**
# MAGIC - Executar células em Python e SQL
# MAGIC - Criar uma célula Markdown
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Renomeando um Notebook
# MAGIC
# MAGIC Mudar o nome de um notebook é fácil. Clique no nome no topo desta página e, em seguida, faça as alterações no nome. Para facilitar a navegação de volta a este notebook mais tarde, caso precise, adicione uma pequena string de teste ao final do nome existente.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Anexando um cluster
# MAGIC
# MAGIC Executar células em um notebook requer recursos computacionais, que são fornecidos pelos clusters. Na primeira vez que você executar uma célula em um notebook, será solicitado que você anexe um cluster caso nenhum esteja anexado.
# MAGIC
# MAGIC Anexe um cluster a este notebook agora clicando no menu suspenso próximo ao canto superior esquerdo desta página. Selecione o cluster que você criou anteriormente. Isso irá limpar o estado de execução do notebook e conectar o notebook ao cluster selecionado.
# MAGIC
# MAGIC Note que o menu suspenso oferece a opção de iniciar ou reiniciar o cluster conforme necessário. Você também pode desanexar e reanexar a um cluster em um único movimento. Isso é útil para limpar o estado de execução quando necessário.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Usando %run
# MAGIC
# MAGIC Projetos complexos de qualquer tipo podem se beneficiar da capacidade de dividi-los em componentes mais simples e reutilizáveis.
# MAGIC
# MAGIC No contexto dos notebooks do Databricks, essa funcionalidade é fornecida pelo comando mágico **`%run`**.
# MAGIC
# MAGIC Quando usado dessa forma, variáveis, funções e blocos de código tornam-se parte do contexto de programação atual.
# MAGIC
# MAGIC Considere este exemplo:
# MAGIC
# MAGIC **`Notebook_A`** tem quatro comandos:
# MAGIC   1. **`name = "John"`**
# MAGIC   2. **`print(f"Hello {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC **`Notebook_B`** tem apenas um comando:
# MAGIC   1. **`full_name = f"{name} Doe"`**
# MAGIC
# MAGIC Se executarmos **`Notebook_B`**, ele falhará porque a variável **`name`** não está definida em **`Notebook_B`**.
# MAGIC
# MAGIC Da mesma forma, alguém poderia pensar que **`Notebook_A`** também falharia porque usa a variável **`full_name`**, que também não está definida em **`Notebook_A`**, mas isso não acontece!
# MAGIC
# MAGIC O que realmente ocorre é que os dois notebooks são mesclados, como vemos abaixo, e **então** executados:
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC E assim, o comportamento esperado é obtido:
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A pasta que contém este notebook possui uma subpasta chamada **`ExampleSetupFolder`**, que por sua vez contém um notebook chamado **`example-setup`**.
# MAGIC
# MAGIC Este notebook simples declara a variável **`my_name`**, define seu valor como **`None`** e, em seguida, cria um DataFrame chamado **`example_df`**.
# MAGIC
# MAGIC Abra o notebook example-setup e modifique-o para que o valor de **`my_name`** não seja **`None`**, mas sim o seu nome (ou o nome de qualquer pessoa) entre aspas. Além disso, certifique-se de que as duas células a seguir sejam executadas sem lançar um **`AssertionError`**.
# MAGIC

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar uma célula Python
# MAGIC
# MAGIC Execute a célula a seguir para verificar se o notebook **`example-setup`** foi executado, exibindo o DataFrame **`example_df`**. Esta tabela consiste em 16 linhas com valores crescentes.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Desanexar e Reanexar um Cluster
# MAGIC
# MAGIC Embora anexar clusters seja uma tarefa bastante comum, às vezes é útil desanexar e reanexar em uma única operação. O principal efeito colateral dessa ação é limpar o estado de execução. Isso pode ser útil quando você deseja testar células isoladamente ou simplesmente deseja redefinir o estado de execução.
# MAGIC
# MAGIC Revise o menu suspenso de clusters. No item de menu que representa o cluster atualmente anexado, selecione o link **Detach & Re-attach** (Desanexar e Reanexar).
# MAGIC
# MAGIC Perceba que a saída da célula acima permanece, pois os resultados e o estado de execução são independentes. No entanto, o estado de execução foi limpo. Isso pode ser verificado ao tentar executar novamente a célula acima. A execução falhará, pois a variável **`example_df`** foi apagada, juntamente com o restante do estado.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Alterar Linguagem
# MAGIC
# MAGIC Note que a linguagem padrão deste notebook está definida como Python. Altere isso clicando no botão **Python**, à direita do nome do notebook. Mude a linguagem padrão para SQL.
# MAGIC
# MAGIC Perceba que as células Python são automaticamente precedidas pelo comando mágico <strong><code>&#37;python</code></strong> para manter a validade dessas células. Observe também que essa operação limpa o estado de execução.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Criar uma Célula Markdown
# MAGIC
# MAGIC Adicione uma nova célula abaixo desta. Preencha com algum conteúdo em Markdown que inclua, pelo menos, os seguintes elementos:
# MAGIC * Um cabeçalho
# MAGIC * Marcadores (bullet points)
# MAGIC * Um link (usando a convenção de sua escolha: HTML ou Markdown)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executar uma Célula SQL
# MAGIC
# MAGIC Execute a célula a seguir para consultar uma tabela Delta usando SQL. Isso executa uma consulta simples em uma tabela que é baseada em um conjunto de dados de exemplo fornecido pela Databricks, incluído em todas as instalações do DBFS.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula a seguir para visualizar os arquivos subjacentes que dão suporte a esta tabela.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Revisar Alterações
# MAGIC
# MAGIC Supondo que você tenha importado este material para seu workspace usando um Databricks Repo, abra o diálogo do repositório clicando no botão da branch **`published`** no canto superior esquerdo desta página. Você deverá ver três alterações:
# MAGIC 1. **Removido** com o nome antigo do notebook
# MAGIC 1. **Adicionado** com o novo nome do notebook
# MAGIC 1. **Modificado** pela criação de uma célula Markdown acima
# MAGIC
# MAGIC Use o diálogo para reverter as alterações e restaurar este notebook ao seu estado original.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusão
# MAGIC
# MAGIC Ao concluir este laboratório, você já deve se sentir confortável em manipular notebooks, criar novas células e executar notebooks dentro de outros notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

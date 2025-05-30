# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Apenas o Suficiente de Python para o Lab do Databricks SQL
# MAGIC
# MAGIC ## Objetivos de Aprendizagem
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC * Revisar código básico em Python e descrever os resultados esperados da execução
# MAGIC * Raciocinar sobre estruturas de controle em funções Python
# MAGIC * Adicionar parâmetros a uma consulta SQL encapsulando-a em uma função Python
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.3L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Revisando os Fundamentos de Python
# MAGIC
# MAGIC No notebook anterior, exploramos brevemente o uso de **`spark.sql()`** para executar comandos SQL arbitrários a partir do Python.
# MAGIC
# MAGIC Observe as 3 células a seguir. Antes de executar cada uma, identifique:
# MAGIC 1. A saída esperada da execução da célula
# MAGIC 2. Qual lógica está sendo executada
# MAGIC 3. Quais mudanças ocorrem no estado atual do ambiente
# MAGIC
# MAGIC Depois, execute as células, compare os resultados com suas expectativas e veja as explicações abaixo.
# MAGIC

# COMMAND ----------

course = "dewd"

# COMMAND ----------

spark.sql(f"SELECT '{course}' AS course_name")

# COMMAND ----------

df = spark.sql(f"SELECT '{course}' AS course_name")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. **Cmd 5** atribui uma string a uma variável. Quando a atribuição é bem-sucedida, nenhuma saída é exibida no notebook. Uma nova variável é adicionada ao ambiente de execução atual.
# MAGIC 2. **Cmd 6** executa uma consulta SQL e exibe o schema do DataFrame junto com a palavra **`DataFrame`**. Nesse caso, a consulta SQL apenas seleciona uma string, então não há alterações no ambiente.
# MAGIC 3. **Cmd 7** executa a mesma consulta SQL e exibe a saída do DataFrame. Essa combinação de **`display()`** com **`spark.sql()`** espelha mais de perto a execução de lógica em uma célula **`%sql`**; os resultados sempre serão apresentados em uma tabela formatada (se houver retorno). Algumas queries, porém, apenas manipulam tabelas ou bancos de dados, e nesses casos a palavra **`OK`** será exibida para indicar sucesso. Aqui, não há alterações no ambiente.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configurando um Ambiente de Desenvolvimento
# MAGIC
# MAGIC Durante este curso, usamos lógica semelhante à da célula seguinte para capturar informações sobre o usuário que está executando o notebook e criar um banco de dados de desenvolvimento isolado.
# MAGIC
# MAGIC A biblioteca **`re`** é a <a href="https://docs.python.org/3/library/re.html" target="_blank">biblioteca padrão de regex do Python</a>.
# MAGIC
# MAGIC O Databricks SQL possui um método especial para capturar o nome de usuário via **`current_user()`**; e o código **`.first()[0]`** é um atalho rápido para obter a primeira linha e primeira coluna de uma query com **`spark.sql()`** (nesse caso, usamos com segurança, pois sabemos que haverá apenas 1 linha e 1 coluna).
# MAGIC
# MAGIC Todo o restante da lógica abaixo é apenas formatação de strings.
# MAGIC

# COMMAND ----------

import re

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
db_name = f"dbacademy_{clean_username}_{course}_5_3l"
working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

print(f"username:    {username}")
print(f"db_name:     {db_name}")
print(f"working_dir: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Abaixo, adicionamos uma simples estrutura de controle a essa lógica para criar e utilizar um banco de dados específico do usuário.
# MAGIC
# MAGIC Opcionalmente, reinicializaremos esse banco e removeremos todo seu conteúdo a cada nova execução. (Note que o valor padrão do parâmetro **`reset`** é **`True`**).
# MAGIC

# COMMAND ----------


def create_database(course, reset=True):
    import re

    username = spark.sql("SELECT current_user()").first()[0]
    clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    db_name = f"dbacademy_{clean_username}_{course}_5_3l"
    working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

    print(f"username:    {username}")
    print(f"db_name:     {db_name}")
    print(f"working_dir: {working_dir}")

    if reset:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
        dbutils.fs.rm(working_dir, True)

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{working_dir}/{db_name}.db'"
    )
    spark.sql(f"USE {db_name}")


create_database(course)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Embora essa lógica tenha sido pensada para isolar alunos em workspaces compartilhados com propósitos educacionais, o mesmo design básico pode ser aproveitado para testar novas lógicas em ambientes isolados antes de levá-las à produção.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lidando com Erros de Forma Elegante
# MAGIC
# MAGIC Revise a lógica da função abaixo.
# MAGIC
# MAGIC Note que acabamos de declarar um novo banco de dados que, no momento, não contém tabelas.
# MAGIC

# COMMAND ----------


def query_or_make_demo_table(table_name):
    try:
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Displayed results for the table {table_name}")

    except:
        spark.sql(
            f"CREATE TABLE {table_name} (id INT, name STRING, value DOUBLE, state STRING)"
        )
        spark.sql(
            f"""INSERT INTO {table_name}
                      VALUES (1, "Yve", 1.0, "CA"),
                             (2, "Omar", 2.5, "NY"),
                             (3, "Elia", 3.3, "OH"),
                             (4, "Rebecca", 4.7, "TX"),
                             (5, "Ameena", 5.3, "CA"),
                             (6, "Ling", 6.6, "NY"),
                             (7, "Pedro", 7.1, "KY")"""
        )

        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Created the table {table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Tente identificar o seguinte antes de executar a próxima célula:
# MAGIC 1. A saída esperada da execução
# MAGIC 2. Qual lógica será executada
# MAGIC 3. Quais mudanças ocorrerão no estado do ambiente
# MAGIC

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Agora, responda as mesmas três perguntas antes de executar novamente a mesma consulta abaixo.
# MAGIC

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Na primeira execução, a tabela **`demo_table`** ainda não existia. Assim, a tentativa de retornar os conteúdos da tabela resultou em um erro, o que fez com que o bloco **`except`** fosse executado. Esse bloco:
# MAGIC   1. Criou a tabela
# MAGIC   2. Inseriu valores
# MAGIC   3. Imprimiu ou exibiu o conteúdo da tabela
# MAGIC - Na segunda execução, a tabela **`demo_table`** já existe, então a consulta no bloco **`try`** é executada sem erro. O resultado é que apenas exibimos os dados da tabela, sem modificar o ambiente.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adaptando SQL para Python
# MAGIC Vamos considerar a seguinte consulta SQL sobre a nossa tabela de demonstração criada acima.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, value
# MAGIC FROM demo_table
# MAGIC WHERE state = "CA"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC que também pode ser expressa usando a API PySpark e a função **`display`**, como visto aqui:
# MAGIC

# COMMAND ----------

results = spark.sql("SELECT id, value FROM demo_table WHERE state = 'CA'")
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos usar esse exemplo simples para praticar a criação de uma função Python que adiciona funcionalidades opcionais.
# MAGIC
# MAGIC Nossa função-alvo irá:
# MAGIC * Basear-se em uma consulta que inclui apenas as colunas **`id`** e **`value`** da tabela chamada **`demo_table`**
# MAGIC * Permitir o filtro da consulta por **`state`**, sendo que o comportamento padrão é incluir todos os estados
# MAGIC * Opcionalmente renderizar os resultados da consulta usando a função **`display`**, sendo que o comportamento padrão é não renderizar
# MAGIC * Retornar:
# MAGIC   * O objeto de resultado da consulta (um DataFrame do PySpark) se **`render_results`** for False
# MAGIC   * O valor **`None`** se **`render_results`** for True
# MAGIC
# MAGIC Meta extra:
# MAGIC * Adicionar uma instrução `assert` para verificar se o valor passado para o parâmetro **`state`** contém duas letras maiúsculas
# MAGIC
# MAGIC Abaixo segue um código inicial como ponto de partida:
# MAGIC

# COMMAND ----------

# TODO
# def preview_values(state=None, render_results=None):
#    query = "SELECT * FROM table_name WHERE condition"
#
#    if state is not None:
#
#
#    if render_results
#
#

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As instruções `assert` abaixo podem ser usadas para verificar se sua função está funcionando como esperado.
# MAGIC

# COMMAND ----------

import pyspark.sql.dataframe

assert (
    type(preview_values()) == pyspark.sql.dataframe.DataFrame
), "Function should return the results as a DataFrame"
assert preview_values().columns == [
    "id",
    "value",
], "Query should only return **`id`** and **`value`** columns"

assert (
    preview_values(render_results=True) is None
), "Function should not return None when rendering"
assert (
    preview_values(render_results=False) is not None
), "Function should return DataFrame when not rendering"

assert preview_values(state=None).count() == 7, "Function should allow no state"
assert (
    preview_values(state="NY").count() == 2
), "Function should allow filtering by state"
assert (
    preview_values(state="CA").count() == 2
), "Function should allow filtering by state"
assert (
    preview_values(state="OH").count() == 1
), "Function should allow filtering by state"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

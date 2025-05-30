# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Python Essencial para Databricks SQL
# MAGIC
# MAGIC Embora o Databricks SQL forneça um estilo de SQL compatível com ANSI com vários métodos personalizados adicionais (incluindo toda a sintaxe do Delta Lake SQL), usuários que estão migrando de outros sistemas podem encontrar a ausência de alguns recursos, especialmente relacionados ao controle de fluxo e tratamento de erros.
# MAGIC
# MAGIC Os notebooks do Databricks permitem que usuários escrevam SQL e Python e executem lógica célula por célula. O PySpark tem amplo suporte para execução de consultas SQL e pode facilmente trocar dados com tabelas e views temporárias.
# MAGIC
# MAGIC Dominar apenas alguns conceitos de Python desbloqueará novas práticas de design poderosas para engenheiros e analistas proficientes em SQL. Em vez de tentar ensinar toda a linguagem, esta lição foca nos recursos que podem ser imediatamente aproveitados para escrever programas SQL mais extensíveis no Databricks.
# MAGIC
# MAGIC ## Objetivos de Aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Imprimir e manipular strings Python multilinha
# MAGIC * Definir variáveis e funções
# MAGIC * Usar f-strings para substituição de variáveis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Strings
# MAGIC Caracteres entre aspas simples (**`'`**) ou duplas (**`"`**) são considerados strings.
# MAGIC

# COMMAND ----------

"This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Para visualizar como uma string será exibida, podemos usar **`print()`**.
# MAGIC

# COMMAND ----------

print("This is a string")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ao envolver uma string com aspas triplas (**`"""`**), é possível usar várias linhas.
# MAGIC

# COMMAND ----------

print(
    """
This 
is 
a 
multi-line 
string
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Isso facilita transformar consultas SQL em strings Python.
# MAGIC

# COMMAND ----------

print(
    """
SELECT *
FROM test_table
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Quando executamos SQL a partir de uma célula Python, passamos a string como argumento para **`spark.sql()`**.
# MAGIC

# COMMAND ----------

spark.sql("SELECT 1 AS test")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Para exibir uma consulta da mesma forma que ela apareceria em um notebook SQL normal, usamos **`display()`** nessa função.
# MAGIC

# COMMAND ----------

display(spark.sql("SELECT 1 AS test"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **NOTA**: Executar uma célula contendo apenas uma string em Python apenas imprimirá a string. Usar **`print()`** com uma string apenas a renderiza no notebook.
# MAGIC
# MAGIC Para executar uma string que contém SQL usando Python, ela deve ser passada dentro de uma chamada para **`spark.sql()`**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Variáveis
# MAGIC Variáveis em Python são atribuídas usando o **`=`**.
# MAGIC
# MAGIC Os nomes de variáveis em Python precisam começar com uma letra e podem conter apenas letras, números e sublinhados. (Nomes que começam com sublinhados são válidos, mas geralmente reservados para casos especiais.)
# MAGIC
# MAGIC Muitos programadores Python preferem usar o estilo snake_case, que usa apenas letras minúsculas e sublinhados para todas as variáveis.
# MAGIC
# MAGIC A célula abaixo cria a variável **`my_string`**.
# MAGIC

# COMMAND ----------

my_string = "This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Executar uma célula com essa variável retornará seu valor.
# MAGIC

# COMMAND ----------

my_string

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A saída aqui é a mesma como se digitássemos **`"This is a string"`** na célula e a executássemos.
# MAGIC
# MAGIC Observe que as aspas não fazem parte da string, como mostrado quando usamos o print.
# MAGIC

# COMMAND ----------

print(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Essa variável pode ser usada da mesma forma que uma string normal.
# MAGIC
# MAGIC Concatenação de strings (juntar duas strings) pode ser feita com **`+`**.
# MAGIC

# COMMAND ----------

print("This is a new string and " + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Podemos juntar variáveis string com outras variáveis string.
# MAGIC

# COMMAND ----------

new_string = "This is a new string and "
print(new_string + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Funções
# MAGIC Funções permitem que você especifique variáveis locais como argumentos e aplique lógica personalizada. Definimos uma função usando a palavra-chave **`def`** seguida pelo nome da função e, entre parênteses, quaisquer argumentos que desejamos passar. Por fim, o cabeçalho da função termina com **`:`**.
# MAGIC
# MAGIC Nota: Em Python, a indentação é importante. Você verá na célula abaixo que a lógica da função está indentada a partir da margem esquerda. Qualquer código com essa indentação faz parte da função.
# MAGIC
# MAGIC A função abaixo recebe um argumento (**`arg`**) e o imprime.
# MAGIC

# COMMAND ----------


def print_string(arg):
    print(arg)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Quando passamos uma string como argumento, ela será impressa.
# MAGIC

# COMMAND ----------

print_string("foo")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Também podemos passar uma variável como argumento.
# MAGIC

# COMMAND ----------

print_string(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Muitas vezes queremos retornar os resultados da nossa função para uso posterior. Para isso usamos a palavra-chave **`return`**.
# MAGIC
# MAGIC A função abaixo constrói uma nova string concatenando nosso argumento. Observe que tanto funções quanto argumentos podem ter nomes arbitrários, assim como variáveis (e seguem as mesmas regras).
# MAGIC

# COMMAND ----------


def return_new_string(string_arg):
    return "The string passed to this function was " + string_arg


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Executar essa função retorna a saída.
# MAGIC

# COMMAND ----------

return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Atribuir essa saída a uma variável permite reutilizá-la em outros lugares.
# MAGIC

# COMMAND ----------

function_output = return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Essa variável não contém a função, apenas o resultado da função (uma string).
# MAGIC

# COMMAND ----------

function_output

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## F-strings
# MAGIC Ao adicionar a letra **`f`** antes de uma string em Python, você pode injetar variáveis ou código Python avaliado, colocando-os entre chaves (**`{}`**).
# MAGIC
# MAGIC Avalie a célula abaixo para ver substituição de variáveis em strings.
# MAGIC

# COMMAND ----------

f"I can substitute {my_string} here"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A célula a seguir insere a string retornada por uma função.
# MAGIC

# COMMAND ----------

f"I can substitute functions like {return_new_string('foobar')} here"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Combine isso com aspas triplas e você pode formatar um parágrafo ou lista, como abaixo.
# MAGIC

# COMMAND ----------

multi_line_string = f"""
I can have many lines of text with variable substitution:
  - A variable: {my_string}
  - A function output: {return_new_string('foobar')}
"""

print(multi_line_string)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ou você pode formatar uma consulta SQL.
# MAGIC

# COMMAND ----------

table_name = "users"
filter_clause = "WHERE state = 'CA'"

query = f"""
SELECT *
FROM {table_name}
{filter_clause}
"""

print(query)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

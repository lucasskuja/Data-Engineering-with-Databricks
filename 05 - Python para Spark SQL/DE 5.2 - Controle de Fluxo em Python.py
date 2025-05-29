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
# MAGIC ## Objetivos de Aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Usar **`if`** / **`else`**
# MAGIC * Descrever como erros afetam a execução de notebooks
# MAGIC * Escrever testes simples com **`assert`**
# MAGIC * Usar **`try`** / **`except`** para tratar erros
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## if/else
# MAGIC
# MAGIC Cláusulas **`if`** / **`else`** são comuns em muitas linguagens de programação.
# MAGIC
# MAGIC Note que o SQL possui o construto **`CASE WHEN ... ELSE`**, que é semelhante.
# MAGIC
# MAGIC <strong>Se você está tentando avaliar condições dentro de suas tabelas ou consultas, use **`CASE WHEN`**.</strong>
# MAGIC
# MAGIC O controle de fluxo em Python deve ser reservado para avaliar condições fora da consulta.
# MAGIC
# MAGIC Mais sobre isso adiante. Primeiro, um exemplo com **`"feijão"`**.
# MAGIC

# COMMAND ----------

food = "beans"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Trabalhar com **`if`** e **`else`** envolve avaliar se certas condições são verdadeiras no ambiente de execução.
# MAGIC
# MAGIC Em Python, usamos os seguintes operadores de comparação:
# MAGIC
# MAGIC | Sintaxe | Operação |
# MAGIC | --- | --- |
# MAGIC | **`==`** | igual |
# MAGIC | **`>`** | maior que |
# MAGIC | **`<`** | menor que |
# MAGIC | **`>=`** | maior ou igual |
# MAGIC | **`<=`** | menor ou igual |
# MAGIC | **`!=`** | diferente de |
# MAGIC
# MAGIC Se você ler a frase abaixo em voz alta, estará descrevendo o fluxo de controle do seu programa.
# MAGIC

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Como esperado, porque a variável **`food`** é o literal **`"beans"`**, o **`if`** foi avaliado como **`True`**, e a primeira instrução de impressão foi executada.
# MAGIC
# MAGIC Vamos atribuir um novo valor à variável.
# MAGIC

# COMMAND ----------

food = "beef"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Agora a primeira condição será avaliada como **`False`**.
# MAGIC
# MAGIC O que você acha que acontecerá ao executar a próxima célula?
# MAGIC

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note que cada vez que atribuirmos um novo valor a uma variável, o valor anterior será completamente sobrescrito.
# MAGIC

# COMMAND ----------

food = "potatoes"
print(food)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A palavra-chave **`elif`** (abreviação de **`else`** + **`if`**) permite avaliar múltiplas condições.
# MAGIC
# MAGIC As condições são avaliadas de cima para baixo. Quando uma condição é verdadeira, as demais são ignoradas.
# MAGIC
# MAGIC Padrões do controle de fluxo com **`if`** / **`else`**:
# MAGIC 1. Deve conter uma cláusula **`if`**
# MAGIC 1. Pode conter qualquer número de cláusulas **`elif`**
# MAGIC 1. Pode conter no máximo uma cláusula **`else`**
# MAGIC

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
elif food == "potatoes":
    print(f"My favorite vegetable is {food}")
elif food != "beef":
    print(f"Do you have any good recipes for {food}?")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Encapsulando essa lógica em uma função, podemos reutilizá-la com argumentos arbitrários, em vez de depender de variáveis globais.
# MAGIC

# COMMAND ----------

def foods_i_like(food):
    if food == "beans":
        print(f"I love {food}")
    elif food == "potatoes":
        print(f"My favorite vegetable is {food}")
    elif food != "beef":
        print(f"Do you have any good recipes for {food}?")
    else:
        print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Aqui, passamos a string **`"bread"`** para a função.
# MAGIC

# COMMAND ----------

foods_i_like("bread")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ao avaliarmos a função, atribuiremos localmente **`"bread"`** à variável **`food`**, e a lógica se comportará como esperado.
# MAGIC
# MAGIC Note que isso não sobrescreve o valor anterior de **`food`** definido no notebook.
# MAGIC

# COMMAND ----------

food

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## try/except
# MAGIC
# MAGIC Enquanto **`if`** / **`else`** nos permite definir lógica condicional, **`try`** / **`except`** foca no tratamento robusto de erros.
# MAGIC
# MAGIC Vamos começar com uma função simples.
# MAGIC

# COMMAND ----------

def three_times(number):
    return number * 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos assumir que o uso desejado da função é multiplicar um valor inteiro por 3.
# MAGIC
# MAGIC A célula abaixo demonstra esse comportamento.
# MAGIC

# COMMAND ----------

three_times(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Veja o que acontece ao passar uma string para a função.
# MAGIC

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Neste caso, não ocorre erro, mas também não obtemos o resultado desejado.
# MAGIC
# MAGIC Instruções **`assert`** nos permitem escrever testes simples. Se o teste for verdadeiro, nada acontece. Se for falso, um erro é levantado.
# MAGIC
# MAGIC Execute a célula abaixo para verificar que o número **`2`** é um inteiro.
# MAGIC

# COMMAND ----------

assert type(2) == int

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Descomente a célula seguinte e execute para verificar se a string **`"2"`** é um inteiro.
# MAGIC
# MAGIC Deverá lançar um **`AssertionError`**.
# MAGIC

# COMMAND ----------

# assert type("2") == int

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Como esperado, a string **`"2"`** não é um inteiro.
# MAGIC
# MAGIC Strings em Python possuem propriedades que indicam se podem ser convertidas em valores numéricos, como mostrado abaixo.
# MAGIC

# COMMAND ----------

assert "2".isnumeric()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Números como string são comuns; podem vir de APIs, registros brutos em JSON ou CSV, ou resultados de consultas SQL.
# MAGIC
# MAGIC **`int()`** e **`float()`** são métodos comuns para converter valores para tipos numéricos.
# MAGIC
# MAGIC Um **`int`** sempre será um número inteiro, enquanto um **`float`** terá sempre casas decimais.
# MAGIC

# COMMAND ----------

int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Python consegue converter strings numéricas em tipos numéricos, mas não permite conversões inválidas.
# MAGIC
# MAGIC Descomente a célula abaixo e experimente:
# MAGIC

# COMMAND ----------

# int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Erros interrompem a execução do notebook; todas as células após o erro serão ignoradas quando o notebook for agendado como um job em produção.
# MAGIC
# MAGIC Ao envolver código propenso a erro em um bloco **`try`**, podemos definir lógica alternativa com **`except`**.
# MAGIC
# MAGIC Veja um exemplo abaixo.
# MAGIC

# COMMAND ----------

def try_int(num_string):
    try:
        int(num_string)
        result = f"{num_string} is a number."
    except:
        result = f"{num_string} is not a number!"
        
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Quando uma string numérica é passada, a função retorna o resultado como inteiro.
# MAGIC

# COMMAND ----------

try_int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Quando uma string não numérica é passada, uma mensagem informativa é impressa.
# MAGIC
# MAGIC **NOTA**: Nenhum erro é lançado, mesmo com falha. Suprimir erros pode causar falhas silenciosas.
# MAGIC

# COMMAND ----------

try_int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Abaixo, a função foi atualizada para lidar com erros e retornar uma mensagem informativa.
# MAGIC

# COMMAND ----------

def three_times(number):
    try:
        return int(number) * 3
    except ValueError as e:
        print(f"You passed the string variable '{number}'.\n")
        print(f"Try passing an integer instead.")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Agora a função consegue processar números passados como string.
# MAGIC

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC E imprime uma mensagem informativa quando uma string inválida é passada.
# MAGIC

# COMMAND ----------

three_times("two")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note que, conforme implementado, isso só é útil em execução interativa (a mensagem não é registrada, e o dado não é retornado em formato útil; seria necessário intervenção humana).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Aplicando Controle de Fluxo Python em Consultas SQL
# MAGIC
# MAGIC Embora os exemplos anteriores demonstrem princípios básicos, o objetivo aqui é aplicar esses conceitos na execução de lógica SQL no Databricks.
# MAGIC
# MAGIC Vamos revisitar a conversão de uma célula SQL para execução em Python.
# MAGIC
# MAGIC **NOTA**: O script de configuração a seguir garante um ambiente de execução isolado.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_tmp_vw(id, name, value) AS VALUES
# MAGIC   (1, "Yve", 1.0),
# MAGIC   (2, "Omar", 2.5),
# MAGIC   (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Execute a célula SQL abaixo para visualizar os dados da view temporária.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Executar SQL em uma célula Python exige apenas passar a string da consulta para **`spark.sql()`**.
# MAGIC

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Mas lembre-se que o **`spark.sql()`** retorna um DataFrame, então usamos **`display()`** para exibir os dados.
# MAGIC

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Com um simples **`if`** em uma função, conseguimos executar consultas SQL arbitrárias, exibir resultados opcionalmente, e sempre retornar o DataFrame.
# MAGIC

# COMMAND ----------

def simple_query_function(query, preview=True):
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

result = simple_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Abaixo, executamos uma consulta diferente e desativamos o preview (**`False`**), pois o objetivo é criar uma view temporária.
# MAGIC

# COMMAND ----------

new_query = "CREATE OR REPLACE TEMP VIEW id_name_tmp_vw AS SELECT id, name FROM demo_tmp_vw"

simple_query_function(new_query, preview=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Agora temos uma função simples e extensível, que pode ser parametrizada conforme a necessidade da empresa.
# MAGIC
# MAGIC Por exemplo, imagine proteger a empresa contra SQL malicioso, como abaixo.
# MAGIC

# COMMAND ----------

injection_query = "SELECT * FROM demo_tmp_vw; DROP DATABASE prod_db CASCADE; SELECT * FROM demo_tmp_vw"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Podemos usar o método **`find()`** para verificar múltiplas instruções SQL procurando por ponto e vírgula.
# MAGIC

# COMMAND ----------

injection_query.find(";")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Se o ponto e vírgula não for encontrado, o método retornará **`-1`**
# MAGIC

# COMMAND ----------

injection_query.find("x")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Com isso, podemos definir uma lógica simples que detecta o ponto e vírgula e levanta uma mensagem de erro personalizada.
# MAGIC

# COMMAND ----------

def injection_check(query):
    semicolon_index = query.find(";")
    if semicolon_index >= 0:
        raise ValueError(f"Query contains semi-colon at index {semicolon_index}\nBlocking execution to avoid SQL injection attack")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **NOTA**: O exemplo aqui é simples, apenas para ilustrar o princípio geral.
# MAGIC
# MAGIC Tenha sempre cuidado ao permitir que usuários passem texto arbitrário para consultas SQL.
# MAGIC
# MAGIC Além disso, o **`spark.sql()`** só permite uma consulta por chamada, então qualquer uso de ponto e vírgula causará erro.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Descomente a célula abaixo e experimente:
# MAGIC

# COMMAND ----------

# injection_check(injection_query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Se adicionarmos essa verificação à nossa função anterior, teremos uma função mais robusta que avalia riscos antes de executar a consulta.
# MAGIC

# COMMAND ----------

def secure_query_function(query, preview=True):
    injection_check(query)
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Como esperado, a execução ocorre normalmente com uma consulta segura.
# MAGIC

# COMMAND ----------

secure_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Mas é impedida quando há lógica perigosa.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. Todos os direitos reservados.<br/>
# MAGIC Apache, Apache Spark, Spark e o logotipo do Spark são marcas registradas da <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Política de Privacidade</a> | <a href="https://databricks.com/terms-of-use">Termos de Uso</a> | <a href="https://help.databricks.com/">Suporte</a>
# MAGIC

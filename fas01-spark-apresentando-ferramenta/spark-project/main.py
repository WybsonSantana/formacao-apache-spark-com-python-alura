import findspark
from pyspark.sql import SparkSession

# Inicializa o FindSpark. Isso é necessário para que o PySpark seja reconhecido no ambiente
findspark.init()

# Cria uma sessão Spark. Configura o master como local[*] para usar todos os núcleos disponíveis
spark = SparkSession.builder.master('local[*]').getOrCreate()

# Prepara os dados para o DataFrame. Cada tupla na lista representa uma linha do DataFrame
# 'Nome' e 'Idade' são os valores para as colunas do DataFrame
data = [('Fulano', '35'), ('Beltrano', '29')]
column_names = ['Nome', 'Idade']

# Cria um DataFrame com os dados e nomes de colunas especificados
df = spark.createDataFrame(data, column_names)

# Exibe o conteúdo do DataFrame no console
df.show()

# Exibe o conteúdo do DataFrame usando o formato do Pandas
# print(df.toPandas())

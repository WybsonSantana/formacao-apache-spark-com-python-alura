import gc
import findspark
from pyspark.sql import SparkSession


def inicializar_spark():
    findspark.init()


def criar_spark_session():
    return SparkSession.builder.master('local[*]').getOrCreate()


def criar_data_frame_lendo_csv(spark_session, path):
    data_frame = spark_session.read.csv(path, inferSchema=True, sep=';', quote='"', escape='"', encoding='UTF-8')
    gc.collect()
    return data_frame


def formatar_separador_de_milhar(numero):
    return f'{numero:,}'.replace(',', '.')


def main():
    inicializar_spark()

    spark = criar_spark_session()

    uri_empresas = 'data/input/empresas/*.csv'
    df_empresas = criar_data_frame_lendo_csv(spark, uri_empresas)
    quantidade_de_empresas_carregadas = df_empresas.count()
    print(f'Quantidade de empresas carregadas: {formatar_separador_de_milhar(quantidade_de_empresas_carregadas)}')

    uri_estabelecimentos = 'data/input/estabelecimentos/*.csv'
    df_estabelecimentos = criar_data_frame_lendo_csv(spark, uri_estabelecimentos)
    quantidade_de_estabelecimentos_carregados = df_estabelecimentos.count()
    print(f'Quantidade de estabelecimentos carregados: {formatar_separador_de_milhar(quantidade_de_estabelecimentos_carregados)}')

    uri_socios = 'data/input/socios/*.csv'
    df_socios = criar_data_frame_lendo_csv(spark, uri_socios)
    quantidade_de_socios_carregados = df_socios.count()
    print(f'Quantidade de sócios carregados: {formatar_separador_de_milhar(quantidade_de_socios_carregados)}')

    spark.stop()


main()

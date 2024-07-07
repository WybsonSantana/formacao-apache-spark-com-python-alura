import findspark
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import DoubleType, StringType


def inicializar_spark():
    findspark.init()


def criar_spark_session():
    return (SparkSession.builder.master('local[*]')
            .config('spark.sql.debug.maxToStringFields', '100')
            .getOrCreate())


def criar_data_frame_lendo_csv(spark_session, path):
    return spark_session.read.csv(path, inferSchema=True, sep=';', quote='"', escape='"', encoding='UTF-8')


def renomear_colunas_do_data_frame(data_frame, nomes_colunas):
    for index, nome_coluna in enumerate(nomes_colunas):
        data_frame = data_frame.withColumnRenamed(f'_c{index}', nome_coluna)
    return data_frame


def processar_dados(spark_session, path, nomes_colunas):
    data_frame = criar_data_frame_lendo_csv(spark_session, path)
    quantidade_de_registros_carregados = data_frame.count()
    print(f'Quantidade de registros carregados: {formatar_separador_de_milhar(quantidade_de_registros_carregados)}')
    data_frame = renomear_colunas_do_data_frame(data_frame, nomes_colunas)
    # data_frame.show(3, truncate=False)
    return data_frame


def converter_separador_decimal(data_frame, nome_coluna):
    print(f'Convertendo separador decimal da coluna {nome_coluna}...')
    return data_frame.withColumn(f'{nome_coluna}', functions.regexp_replace(f'{nome_coluna}', ',', '.'))


def converter_tipo_coluna_para_double(data_frame, nome_coluna):
    print(f'Convertendo tipo da coluna {nome_coluna} para double...')
    return data_frame.withColumn(f'{nome_coluna}', data_frame[f'{nome_coluna}'].cast(DoubleType()))


def converter_tipo_coluna_para_date(data_frame, nome_coluna):
    print(f'Convertendo tipo da coluna {nome_coluna} para date...')
    return data_frame.withColumn(f'{nome_coluna}',
                                 functions.to_date(data_frame[f'{nome_coluna}'].cast(StringType()), 'yyyyMMdd'))


def formatar_separador_de_milhar(numero):
    return f'{numero:,}'.replace(',', '.')


def main():
    inicializar_spark()
    spark = criar_spark_session()

    uri_empresas = 'data/input/empresas/*.csv'
    colunas_empresas = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                        'qualificacao_do_responsavel', 'capital_social_da_empresa', 'porte_da_empresa',
                        'ente_federativo_responsavel']

    uri_estabelecimentos = 'data/input/estabelecimentos/*.csv'
    colunas_estabelecimentos = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
                                'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                                'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade',
                                'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro',
                                'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
                                'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial',
                                'data_da_situacao_especial']

    uri_socios = 'data/input/socios/*.csv'
    colunas_socios = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio',
                      'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal',
                      'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']

    # Trabalhando com o Data Frame de empresas
    print('processando dados de empresas...')
    df_empresas = processar_dados(spark, uri_empresas, colunas_empresas)
    df_empresas = converter_separador_decimal(df_empresas, 'capital_social_da_empresa')
    df_empresas = converter_tipo_coluna_para_double(df_empresas, 'capital_social_da_empresa')

    (df_empresas
     .select('natureza_juridica', 'porte_da_empresa', 'capital_social_da_empresa')
     .show(3, truncate=False))

    (df_empresas
     .where('capital_social_da_empresa == 50')
     .show(3, truncate=False))

    (df_empresas
     .select('razao_social_nome_empresarial', 'natureza_juridica', 'porte_da_empresa', 'capital_social_da_empresa')
     .filter(functions.upper(df_empresas['razao_social_nome_empresarial']).like('%RESTAURANTE%'))
     .show(3, truncate=False))

    (df_empresas
     .select('cnpj_basico', 'porte_da_empresa', 'capital_social_da_empresa')
     .groupBy('porte_da_empresa')
     .agg(functions.avg('capital_social_da_empresa').alias('capital_social_medio'),
          functions.count('cnpj_basico').alias('frequencia'))
     .orderBy('porte_da_empresa', ascending=True)
     .show())

    (df_empresas
     .select('capital_social_da_empresa')
     .summary()
     .show())

    # Trabalhando com o Data Frame de estabelecimentos
    print('Processando dados de estabelecimentos...')
    df_estabelecimentos = processar_dados(spark, uri_estabelecimentos, colunas_estabelecimentos)
    df_estabelecimentos = converter_tipo_coluna_para_date(df_estabelecimentos, 'data_situacao_cadastral')
    df_estabelecimentos = converter_tipo_coluna_para_date(df_estabelecimentos, 'data_de_inicio_atividade')
    df_estabelecimentos = converter_tipo_coluna_para_date(df_estabelecimentos, 'data_da_situacao_especial')

    (df_estabelecimentos
     .select('nome_fantasia', 'municipio',
             functions.year('data_de_inicio_atividade').alias('ano_de_inicio_atividade'),
             functions.month('data_de_inicio_atividade').alias('mes_de_inicio_atividade'))
     .show(3, truncate=False))

    # Trabalhando com o Data Frame de sócios
    print('Processando dados de sócios...')
    df_socios = processar_dados(spark, uri_socios, colunas_socios)
    df_socios = converter_tipo_coluna_para_date(df_socios, 'data_de_entrada_sociedade')

    (df_socios
     .select('nome_do_socio_ou_razao_social', 'faixa_etaria',
             functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .show(3, truncate=False))

    df_socios.select([functions.count(functions.when(functions.isnull(column), 1)).alias(column)
                      for column in df_socios.columns]).show()

    (df_socios
     .select('nome_do_socio_ou_razao_social', 'faixa_etaria',
             functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .orderBy(['ano_de_entrada', 'faixa_etaria'], ascending=False)
     .show(3, truncate=False))

    (df_socios
     .select('nome_do_socio_ou_razao_social')
     .filter(df_socios['nome_do_socio_ou_razao_social'].startswith('LORENA'))
     .filter(df_socios['nome_do_socio_ou_razao_social'].endswith('DIAS'))
     .show(3, truncate=False))

    (df_socios
     .select(functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .where('ano_de_entrada >= 2010')
     .groupBy('ano_de_entrada')
     .count()
     .orderBy('ano_de_entrada', ascending=True)
     .show())

    # Trabalhando com junção de Data Frames
    df_estabelecimentos_join_empresas = df_estabelecimentos.join(df_empresas, 'cnpj_basico', how='inner')
    df_estabelecimentos_join_empresas.printSchema()

    df_frequencia = (df_estabelecimentos_join_empresas
                     .select('cnpj_basico', functions.year('data_de_inicio_atividade').alias('data_de_inicio'))
                     .where('data_de_inicio >= 2010')
                     .groupBy('data_de_inicio')
                     .agg(functions.count('cnpj_basico').alias('frequencia'))
                     .orderBy('data_de_inicio', ascending=True))

    df_frequencia.show()

    (df_frequencia.union(df_frequencia.select(
        functions.lit('Total').alias('data_de_inicio'),
        functions.sum(df_frequencia['frequencia']).alias('frequencia')
    )).show())

    spark.stop()


main()

import findspark
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import DoubleType, StringType


def inicializar_spark():
    findspark.init()


def criar_spark_session():
    return (SparkSession.builder.master('local[*]')
            .config('spark.sql.debug.maxToStringFields', '100')
            .config('spark.executor.memory', '2g')
            .config('spark.executor.memoryOverhead', '2g')
            .config('spark.driver.memory', '2g')
            .config('spark.eventLog.gcMetrics.youngGenerationGarbageCollectors', 'G1 Young Generation')
            .config('spark.eventLog.gcMetrics.oldGenerationGarbageCollectors', 'G1 Old Generation')
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

    # Configuração para lidar com datas e timestamps antigos ao escrever arquivos Parquet
    spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'LEGACY')

    # Definindo o caminho dos arquivos e nomes das colunas para o Data Frame de empresas
    uri_empresas = 'data/input/empresas/*.csv'
    colunas_empresas = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica',
                        'qualificacao_do_responsavel', 'capital_social_da_empresa', 'porte_da_empresa',
                        'ente_federativo_responsavel']

    # Definindo o caminho dos arquivos e nomes das colunas para o Data Frame de estabelecimentos
    uri_estabelecimentos = 'data/input/estabelecimentos/*.csv'
    colunas_estabelecimentos = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
                                'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                                'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade',
                                'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro',
                                'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1',
                                'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico', 'situacao_especial',
                                'data_da_situacao_especial']

    # Definindo o caminho dos arquivos e nomes das colunas para o Data Frame de sócios
    uri_socios = 'data/input/socios/*.csv'
    colunas_socios = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio',
                      'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal',
                      'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']

    # Trabalhando com o Data Frame de empresas
    print('processando dados de empresas...')
    df_empresas = processar_dados(spark, uri_empresas, colunas_empresas)
    df_empresas = converter_separador_decimal(df_empresas, 'capital_social_da_empresa')
    df_empresas = converter_tipo_coluna_para_double(df_empresas, 'capital_social_da_empresa')

    # Salvando o Data Frame de empresas em arquivo csv particionado
    print('Salvando o Data Frame de empresas em arquivo csv particionado...')
    df_empresas.write.csv(
        path='./data/output/empresas/csv-particionado',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de empresas em arquivo csv único
    print('Salvando o Data Frame de empresas em arquivo csv único...')
    df_empresas.coalesce(1).write.csv(
        path='./data/output/empresas/csv-unico',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de empresas em arquivo parquet particionado
    print('Salvando o Data Frame de empresas em arquivo parquet particionado...')
    df_empresas.write.parquet(
        path='./data/output/empresas/parquet-particionado',
        mode='overwrite'
    )

    # Salvando o Data Frame de empresas em arquivo parquet particionado por categoria
    print('Salvando o Data Frame de empresas em arquivo parquet particionado por categoria...')
    df_empresas.write.parquet(
        path='./data/output/empresas/parquet-particionado-categoria',
        mode='overwrite',
        partitionBy='porte_da_empresa'
    )

    # Salvando o Data Frame de empresas em arquivo parquet único
    print('Salvando o Data Frame de empresas em arquivo parquet único...')
    df_empresas.coalesce(1).write.parquet(
        path='./data/output/empresas/parquet-unico',
        mode='overwrite'
    )

    # Seleciona e exibe as primeiras 3 linhas das colunas selecionadas
    (df_empresas
     .select('natureza_juridica', 'porte_da_empresa', 'capital_social_da_empresa')
     .show(3, truncate=False))

    # Filtra empresas com capital social igual a 50 e exibe as primeiras 3 linhas
    (df_empresas
     .where('capital_social_da_empresa == 50')
     .show(3, truncate=False))

    # Seleciona e exibe as primeiras 3 linhas das empresas que têm 'RESTAURANTE' no nome
    (df_empresas
     .select('razao_social_nome_empresarial', 'natureza_juridica', 'porte_da_empresa', 'capital_social_da_empresa')
     .filter(functions.upper(df_empresas['razao_social_nome_empresarial']).like('%RESTAURANTE%'))
     .show(3, truncate=False))

    # Agrupa as empresas por porte, calcula a média do capital social e a frequência de CNPJs e exibe os resultados ordenados por porte
    (df_empresas
     .select('cnpj_basico', 'porte_da_empresa', 'capital_social_da_empresa')
     .groupBy('porte_da_empresa')
     .agg(functions.avg('capital_social_da_empresa').alias('capital_social_medio'),
          functions.count('cnpj_basico').alias('frequencia'))
     .orderBy('porte_da_empresa', ascending=True)
     .show())

    # Gera um resumo estatístico da coluna 'capital_social_da_empresa' e exibe os resultados
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

    # Salvando o Data Frame de estabelecimentos em arquivo csv particionado
    print('Salvando o Data Frame de estabelecimentos em arquivo csv particionado...')
    df_estabelecimentos.write.csv(
        path='./data/output/estabelecimentos/csv-particionado',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de estabelecimentos em arquivo csv único
    print('Salvando o Data Frame de estabelecimentos em arquivo csv único...')
    df_estabelecimentos.coalesce(1).write.csv(
        path='./data/output/estabelecimentos/csv-unico',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de estabelecimentos em arquivo parquet particionado
    print('Salvando o Data Frame de estabelecimentos em arquivo parquet particionado...')
    df_estabelecimentos.write.parquet(
        path='./data/output/estabelecimentos/parquet-particionado',
        mode='overwrite'
    )

    # Salvando o Data Frame de estabelecimentos em arquivo parquet único
    print('Salvando o Data Frame de estabelecimentos em arquivo parquet único...')
    df_estabelecimentos.coalesce(1).write.parquet(
        path='./data/output/estabelecimentos/parquet-unico',
        mode='overwrite'
    )

    # Seleciona e exibe as primeiras 3 linhas das colunas selecionadas
    (df_estabelecimentos
     .select('nome_fantasia', 'municipio',
             functions.year('data_de_inicio_atividade').alias('ano_de_inicio_atividade'),
             functions.month('data_de_inicio_atividade').alias('mes_de_inicio_atividade'))
     .show(3, truncate=False))

    # Trabalhando com o Data Frame de sócios
    print('Processando dados de sócios...')
    df_socios = processar_dados(spark, uri_socios, colunas_socios)
    df_socios = converter_tipo_coluna_para_date(df_socios, 'data_de_entrada_sociedade')

    # Salvando o Data Frame de sócios em arquivo csv particionado
    print('Salvando o Data Frame de sócios em arquivo csv particionado...')
    df_socios.write.csv(
        path='./data/output/socios/csv-particionado',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de sócios em arquivo csv único
    print('Salvando o Data Frame de sócios em arquivo csv único...')
    df_socios.coalesce(1).write.csv(
        path='./data/output/socios/csv-unico',
        mode='overwrite',
        sep=';',
        header=True
    )

    # Salvando o Data Frame de sócios em arquivo parquet particionado
    print('Salvando o Data Frame de sócios em arquivo parquet particionado...')
    df_socios.write.parquet(
        path='./data/output/socios/parquet-particionado',
        mode='overwrite'
    )

    # Salvando o Data Frame de sócios em arquivo parquet único
    print('Salvando o Data Frame de sócios em arquivo parquet único...')
    df_socios.coalesce(1).write.parquet(
        path='./data/output/socios/parquet-unico',
        mode='overwrite'
    )

    # Seleciona e exibe as primeiras 3 linhas das colunas selecionadas
    (df_socios
     .select('nome_do_socio_ou_razao_social', 'faixa_etaria',
             functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .show(3, truncate=False))

    # Conta o número de valores nulos em cada coluna do DataFrame e exibe os resultados
    df_socios.select([functions.count(functions.when(functions.isnull(column), 1)).alias(column)
                      for column in df_socios.columns]).show()

    # Seleciona, ordena por ano e faixa etária em ordem decrescente, e exibe as primeiras 3 linhas
    (df_socios
     .select('nome_do_socio_ou_razao_social', 'faixa_etaria',
             functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .orderBy(['ano_de_entrada', 'faixa_etaria'], ascending=False)
     .show(3, truncate=False))

    # Seleciona e exibe as primeiras 3 linhas dos sócios cujo nome começa com 'LORENA' e termina com 'DIAS'
    (df_socios
     .select('nome_do_socio_ou_razao_social')
     .filter(df_socios['nome_do_socio_ou_razao_social'].startswith('LORENA'))
     .filter(df_socios['nome_do_socio_ou_razao_social'].endswith('DIAS'))
     .show(3, truncate=False))

    # Seleciona o ano de entrada dos sócios, filtra pelos que entraram a partir de 2010, conta a frequência por ano, ordena e exibe os resultados
    (df_socios
     .select(functions.year('data_de_entrada_sociedade').alias('ano_de_entrada'))
     .where('ano_de_entrada >= 2010')
     .groupBy('ano_de_entrada')
     .count()
     .orderBy('ano_de_entrada', ascending=True)
     .show())

    # Trabalhando com junção de Data Frames
    # Realiza um inner join dos DataFrames df_estabelecimentos e df_empresas usando a coluna 'cnpj_basico' como chave
    df_estabelecimentos_join_empresas = df_estabelecimentos.join(df_empresas, 'cnpj_basico', how='inner')
    # Exibe o schema do Data Frame criado
    df_estabelecimentos_join_empresas.printSchema()

    # Seleciona o CNPJ básico e o ano de início de atividade dos estabelecimentos, filtra pelos que iniciaram a partir de 2010,
    # agrupa por ano de início, calcula a frequência de CNPJs por ano e ordena os resultados por ano de início em ordem ascendente
    df_frequencia = (df_estabelecimentos_join_empresas
                     .select('cnpj_basico', functions.year('data_de_inicio_atividade').alias('data_de_inicio'))
                     .where('data_de_inicio >= 2010')
                     .groupBy('data_de_inicio')
                     .agg(functions.count('cnpj_basico').alias('frequencia'))
                     .orderBy('data_de_inicio', ascending=True))

    # Exibe o conteúdo filtrado no Data Frame acima
    df_frequencia.show()

    # Adiciona ao DataFrame df_frequencia uma linha com o valor 'Total' em 'data_de_inicio' e a soma das frequências
    (df_frequencia.union(df_frequencia.select(
        functions.lit('Total').alias('data_de_inicio'),
        functions.sum(df_frequencia['frequencia']).alias('frequencia')
    )).show())

    # Trabalhando com consultas SQL padrão
    # Cria ou substitui uma visualização temporária 'df_empresas_view' para permitir consultas SQL diretamente no DataFrame df_empresas
    df_empresas.createOrReplaceTempView('df_empresas_view')

    # Executa uma consulta SQL para selecionar todas as colunas da visualização temporária 'df_empresas_view' e exibe as primeiras 3 linhas
    spark.sql('SELECT * FROM df_empresas_view').show(3, truncate=False)

    # Consulta SQL em 'df_empresas_view' filtrando 'capital_social_da_empresa' = 50 e exibe as 3 primeiras linhas.
    spark.sql("""
        SELECT * FROM df_empresas_view
        WHERE capital_social_da_empresa = 50
    """).show(3, truncate=False)

    # Calcula e exibe a média do capital social por porte de empresa das primeiras 3 linhas em 'df_empresas_view'.
    spark.sql("""
        SELECT porte_da_empresa, MEAN(capital_social_da_empresa) as Media
        FROM df_empresas_view
        GROUP BY porte_da_empresa
    """).show(3, truncate=False)

    # Cria ou substitui uma visualização temporária 'df_estabelecimentos_view' para permitir consultas SQL diretamente no DataFrame df_estabelecimentos
    df_estabelecimentos_join_empresas.createOrReplaceTempView('df_estabelecimentos_join_empresas_view')

    # Consulta SQL em 'df_estabelecimentos_join_empresas_view' para contar CNPJs por ano de início desde 2010 ordenados por ano.
    df_frequencia = spark.sql("""
        SELECT YEAR(data_de_inicio_atividade) AS data_de_inicio, COUNT(cnpj_basico) AS count
        FROM df_estabelecimentos_join_empresas_view
        WHERE YEAR(data_de_inicio_atividade) >= 2010
        GROUP BY data_de_inicio
        ORDER BY data_de_inicio
    """)

    # Exibe o resultado da consulta acima
    df_frequencia.show()

    # Cria ou substitui uma visualização temporária 'df_frequencia_view' para permitir consultas SQL diretamente no DataFrame df_frequencia
    df_frequencia.createOrReplaceTempView('df_frequencia_view')

    # Consulta SQL que une duas consultas na visualização 'df_frequencia_view', adicionando uma linha com o total de 'count'.
    spark.sql("""
        SELECT * FROM df_frequencia_view
        UNION ALL
        SELECT 'Total' AS data_de_inicio, SUM(count) AS count
        FROM df_frequencia_view 
    """).show()

    # Finaliza a sessão do Spark
    spark.stop()


main()

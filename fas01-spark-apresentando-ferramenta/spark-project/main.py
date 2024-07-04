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


def renomear_colunas_do_data_frame(data_frame, nomes_colunas):
    for index, nome_coluna in enumerate(nomes_colunas):
        data_frame = data_frame.withColumnRenamed(f'_c{index}', nome_coluna)
    return data_frame


def processar_dados(spark_session, path, nomes_colunas):
    data_frame = criar_data_frame_lendo_csv(spark_session, path)
    quantidade_de_registros_carregados = data_frame.count()
    print(f'Quantidade de registros carregados: {formatar_separador_de_milhar(quantidade_de_registros_carregados)}')
    data_frame = renomear_colunas_do_data_frame(data_frame, nomes_colunas)
    data_frame.show(3, truncate=False)
    return data_frame


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

    print('processando dados de empresas...')
    df_empresas = processar_dados(spark, uri_empresas, colunas_empresas)

    print('Processando dados de estabelecimentos...')
    df_estabelecimentos = processar_dados(spark, uri_estabelecimentos, colunas_estabelecimentos)

    print('Processando dados de sócios...')
    df_socios = processar_dados(spark, uri_socios, colunas_socios)

    spark.stop()


main()

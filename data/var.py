# CONSTANTES PARA DEFINICAO CENTRALIZADA DA NOMENCLATURA A SER UTILIZADA
# Nomes das tabelas/arquivos
EMPRESAS = 'empresas'
SOCIOS = 'socios'
CNAES_SECUNDARIOS = 'cnaes_secundarios'

# Nome das colunas de empresas
EMP_CNPJ = 'cnpj'
EMP_MATRIZ_FILIAL = 'matriz_filial'
EMP_RAZAO_SOCIAL = 'razao_social'
EMP_NOME_FANTASIA = 'nome_fantasia'
EMP_SITUACAO = 'situacao'
EMP_DATA_SITUACAO = 'data_situacao'
EMP_MOTIVO_SITUACAO = 'motivo_situacao'
EMP_NM_CIDADE_EXTERIOR = 'nm_cidade_exterior'
EMP_COD_PAIS = 'cod_pais'
EMP_NOME_PAIS = 'nome_pais'
EMP_COD_NAT_JURIDICA = 'cod_nat_juridica'
EMP_DATA_INICIO_ATIV = 'data_inicio_ativ'
EMP_CNAE_FISCAL = 'cnae_fiscal'
EMP_TIPO_LOGRADOURO = 'tipo_logradouro'
EMP_LOGRADOURO = 'logradouro'
EMP_NUMERO = 'numero'
EMP_COMPLEMENTO = 'complemento'
EMP_BAIRRO = 'bairro'
EMP_CEP = 'cep'
EMP_UF = 'uf'
EMP_COD_MUNICIPIO = 'cod_municipio'
EMP_MUNICIPIO = 'municipio'
EMP_DDD_1 = 'ddd_1'
EMP_TELEFONE_1 = 'telefone_1'
EMP_DDD_2 = 'ddd_2'
EMP_TELEFONE_2 = 'telefone_2'
EMP_DDD_FAX = 'ddd_fax'
EMP_NUM_FAX = 'num_fax'
EMP_EMAIL = 'email'
EMP_QUALIF_RESP = 'qualif_resp'
EMP_CAPITAL_SOCIAL = 'capital_social'
EMP_PORTE = 'porte'
EMP_OPC_SIMPLES = 'opc_simples'
EMP_DATA_OPC_SIMPLES = 'data_opc_simples'
EMP_DATA_EXC_SIMPLES = 'data_exc_simples'
EMP_OPC_MEI = 'opc_mei'
EMP_SIT_ESPECIAL = 'sit_especial'
EMP_DATA_SIT_ESPECIAL = 'data_sit_especial'

# Nome das colunas de socios
SOC_CNPJ = 'cnpj'
SOC_TIPO_SOCIO = 'tipo_socio'
SOC_NOME_SOCIO = 'nome_socio'
SOC_CNPJ_CPF_SOCIO = 'cnpj_cpf_socio'
SOC_COD_QUALIFICACAO = 'cod_qualificacao'
SOC_PERC_CAPITAL = 'perc_capital'
SOC_DATA_ENTRADA = 'data_entrada'
SOC_COD_PAIS_EXT = 'cod_pais_ext'
SOC_NOME_PAIS_EXT = 'nome_pais_ext'
SOC_CPF_REPRES = 'cpf_repres'
SOC_NOME_REPRES = 'nome_repres'
SOC_COD_QUALIF_REPRES = 'cod_qualif_repres'

# Nome das colunas de cnaes secundarios
CNA_CNPJ = 'cnpj'
CNA_CNAE = 'cnae'
CNA_ORDEM = 'cnae_ordem'
# FIM DAS CONSTANTES PARA DEFINICAO DE NOMENCLATURA

REGISTROS_TIPOS = {
    '1':EMPRESAS,
    '2':SOCIOS,
    '6':CNAES_SECUNDARIOS
}

EMPRESAS_COLUNAS = [
    (EMP_CNPJ,(3, 17)),
    (EMP_MATRIZ_FILIAL,(17,18)),
    (EMP_RAZAO_SOCIAL,(18,168)),
    (EMP_NOME_FANTASIA,(168,223)),
    (EMP_SITUACAO,(223,225)),
    (EMP_DATA_SITUACAO,(225,233)),
    (EMP_MOTIVO_SITUACAO,(233,235)),
    (EMP_NM_CIDADE_EXTERIOR,(235,290)),
    (EMP_COD_PAIS,(290,293)),
    (EMP_NOME_PAIS,(293,363)),
    (EMP_COD_NAT_JURIDICA,(363,367)),
    (EMP_DATA_INICIO_ATIV,(367,375)),
    (EMP_CNAE_FISCAL,(375,382)),
    (EMP_TIPO_LOGRADOURO,(382,402)),
    (EMP_LOGRADOURO,(402,462)),
    (EMP_NUMERO,(462,468)),
    (EMP_COMPLEMENTO,(468,624)),
    (EMP_BAIRRO,(624,674)),
    (EMP_CEP,(674,682)),
    (EMP_UF,(682,684)),
    (EMP_COD_MUNICIPIO,(684,688)),
    (EMP_MUNICIPIO,(688,738)),
    (EMP_DDD_1,(738,742)),
    (EMP_TELEFONE_1,(742,750)),
    (EMP_DDD_2,(750,754)),
    (EMP_TELEFONE_2,(754,762)),
    (EMP_DDD_FAX,(762,766)),
    (EMP_NUM_FAX,(766,774)),
    (EMP_EMAIL,(774,889)),
    (EMP_QUALIF_RESP,(889,891)),
    (EMP_CAPITAL_SOCIAL,(891,905)),
    (EMP_PORTE,(905,907)),
    (EMP_OPC_SIMPLES,(907,908)),
    (EMP_DATA_OPC_SIMPLES,(908,916)),
    (EMP_DATA_EXC_SIMPLES,(916,924)),
    (EMP_OPC_MEI,(924,925)),
    (EMP_SIT_ESPECIAL,(925,948)),
    (EMP_DATA_SIT_ESPECIAL,(948,956))
]

EMPRESAS_DTYPE = {EMP_CAPITAL_SOCIAL:float}

SOCIOS_COLUNAS = [
    (SOC_CNPJ,(3, 17)),
    (SOC_TIPO_SOCIO,(17,18)),
    (SOC_NOME_SOCIO,(18,168)),
    (SOC_CNPJ_CPF_SOCIO,(168,182)),
    (SOC_COD_QUALIFICACAO,(182,184)),
    (SOC_PERC_CAPITAL,(184,189)),
    (SOC_DATA_ENTRADA,(189,197)),
    (SOC_COD_PAIS_EXT,(197,200)),
    (SOC_NOME_PAIS_EXT,(200,270)),
    (SOC_CPF_REPRES,(270,281)),
    (SOC_NOME_REPRES,(281,341)),
    (SOC_COD_QUALIF_REPRES,(341,343))
]

SOCIOS_DTYPE = {SOC_PERC_CAPITAL:float}

CNAES_COLNOMES = [CNA_CNPJ] + [num for num in range(99)]
CNAES_COLSPECS = [(3,17)] + [(num*7+17,num*7+24) for num in range(99)]

HEADER_COLUNAS = [
    ('Nome do arquivo',(17,28)),
    ('Data de gravacao',(28,36)),
    ('Numero da remessa',(36,44))
]

TRAILLER_COLUNAS = [
    ('Total de registros de empresas',(17,26)),
    ('Total de registros de socios',(26,35)),
    ('Total de registros de CNAEs secundarios',(35,44)),
    ('Total de registros incluindo header e trailler',(44,55))
]

# (<nome_do_indice>,<tabela>,<coluna>)
INDICES = [
    ('empresas_cnpj', EMPRESAS, EMP_CNPJ),
    ('empresas_raiz', EMPRESAS, 'substr({},0,9)'.format(EMP_CNPJ)),
    ('socios_cnpj', SOCIOS, SOC_CNPJ),
    ('socios_cpf_cnpj', SOCIOS, SOC_CNPJ_CPF_SOCIO),
    ('socios_nome', SOCIOS, SOC_NOME_SOCIO),
    ('cnaes_cnpj', CNAES_SECUNDARIOS, CNA_CNPJ)
]

PREFIXO_INDICE = 'ix_'

CHUNKSIZE=200000


header_colnomes = list(list(zip(*HEADER_COLUNAS))[0])
empresas_colnomes = list(list(zip(*EMPRESAS_COLUNAS))[0])
socios_colnomes = list(list(zip(*SOCIOS_COLUNAS))[0])
trailler_colnomes = list(list(zip(*TRAILLER_COLUNAS))[0])

header_colspecs = list(list(zip(*HEADER_COLUNAS))[1])
empresas_colspecs = list(list(zip(*EMPRESAS_COLUNAS))[1])
socios_colspecs = list(list(zip(*SOCIOS_COLUNAS))[1])
trailler_colspecs = list(list(zip(*TRAILLER_COLUNAS))[1])


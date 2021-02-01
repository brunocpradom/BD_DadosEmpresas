import os
import glob
import sys
import csv
import datetime
import pandas as pd
from progress.spinner import LineSpinner
from config import *
from data import cod_qualificacao


class DataClean:
    def __init__(self):
        self.dir = csvDirPath
        self.dataPath = dataPath    #-- output path
        #--- variável definida no script antigo
        file_path = str(file_dir) + 'socios.csv'
        self.zipPath = zipPath   #--input_path
    
    def cnpj_full(input_list, tipo_output, output_path):
        """Essa função recebe uma lista com os arquivos zips, baixados do site
        da receita federal, o tipo de output (CSV) e o diretório onde deverão
        ser salvos os 3 arquivos resultantes do processo('empresas.csv', 
        'socios.csv' e 'cnaesecundario.csv')
        """
        #--------------------------------------------------------------------#
        #-- Posso melhorar essa função, mudando por exemplo o input_list.    #
        #-- Agora o processo vai acontecer de forma diferente. Os arquivos   #
        #-- serão baixados e processados um a um, para economizar espaço     #
        #-- quando o script for implementado em um servidor                  #
        #--------------------------------------------------------------------#
        
        total_empresas = 0
        controle_empresas = 0
        total_socios = 0
        controle_socios = 0
        total_cnaes = 0
        controle_cnaes = 0
    
        if not os.path.exists(output_path):
            os.makedirs(output_path)
    
        
        header_colnomes = list(list(zip(*HEADER_COLUNAS))[0])
        empresas_colnomes = list(list(zip(*EMPRESAS_COLUNAS))[0])
        socios_colnomes = list(list(zip(*SOCIOS_COLUNAS))[0])
        trailler_colnomes = list(list(zip(*TRAILLER_COLUNAS))[0])
    
        header_colspecs = list(list(zip(*HEADER_COLUNAS))[1])
        empresas_colspecs = list(list(zip(*EMPRESAS_COLUNAS))[1])
        socios_colspecs = list(list(zip(*SOCIOS_COLUNAS))[1])
        trailler_colspecs = list(list(zip(*TRAILLER_COLUNAS))[1])
    
        # Itera sobre sequencia de arquivos (p/ suportar arquivo dividido pela RF)
        for i_arq, arquivo in enumerate(input_list):
            print('Processando arquivo: {}'.format(arquivo))
            
            dados = read_cfwf(arquivo, 
                            type_width=1, 
                            colspecs= {'0':header_colspecs,
                                        '1':empresas_colspecs,
                                        '2':socios_colspecs,
                                        '6':CNAES_COLSPECS,
                                        '9':trailler_colspecs},
                            names={'0':header_colnomes,
                                    '1':empresas_colnomes, 
                                    '2':socios_colnomes,
                                    '6':CNAES_COLNOMES,
                                    '9':trailler_colnomes},
                            dtype={'1': EMPRESAS_DTYPE,
                                    '2': SOCIOS_DTYPE},
                            chunksize=CHUNKSIZE,
                            encoding='ISO-8859-15')
            
            # Itera sobre blocos (chunks) do arquivo
            for i_bloco, bloco in enumerate(dados):
                print('Bloco {}: até linha {}. [Emps:{}|Socios:{}|CNAEs:{}]'.format(i_bloco+1,
                                                                    (i_bloco+1)*CHUNKSIZE,
                                                                    total_empresas, 
                                                                    total_socios, 
                                                                    total_cnaes), 
                    end='\r')
            
                for tipo_registro, df in bloco.items():
            
                    if tipo_registro == '1': # empresas
                        total_empresas += len(df)
            
                        # Troca datas zeradas por vazio
                        df[EMP_DATA_OPC_SIMPLES] = (df[EMP_DATA_OPC_SIMPLES]
                                .where(df[EMP_DATA_OPC_SIMPLES] != '00000000',''))
                        df[EMP_DATA_EXC_SIMPLES] = (df[EMP_DATA_EXC_SIMPLES]
                                .where(df[EMP_DATA_EXC_SIMPLES] != '00000000',''))
                        df[EMP_DATA_SIT_ESPECIAL] = (df[EMP_DATA_SIT_ESPECIAL]
                                .where(df[EMP_DATA_SIT_ESPECIAL] != '00000000',''))
            
                    elif tipo_registro == '2': # socios
                        total_socios += len(df)
    
                        # Troca cpf invalido por vazio
                        df[SOC_CPF_REPRES] = (df[SOC_CPF_REPRES]
                                .where(df[SOC_CPF_REPRES] != '***000000**',''))
                        df[SOC_NOME_REPRES] = (df[SOC_NOME_REPRES]
                                .where(df[SOC_NOME_REPRES] != 'CPF INVALIDO',''))  
    
                        # Se socio for tipo 1 (cnpj), deixa campo intacto, do contrario, 
                        # fica apenas com os ultimos 11 digitos
                        df[SOC_CNPJ_CPF_SOCIO] = (df[SOC_CNPJ_CPF_SOCIO]
                                .where(df[SOC_TIPO_SOCIO] == '1',
                                        df[SOC_CNPJ_CPF_SOCIO].str[-11:]))
    
                    elif tipo_registro == '6': # cnaes_secundarios       
                        total_cnaes += len(df)
    
                        # Verticaliza tabela de associacao de cnaes secundarios,
                        # mantendo apenas os validos (diferentes de 0000000)
                        df = pd.melt(df, 
                                    id_vars=[CNA_CNPJ], 
                                    value_vars=range(99),
                                    var_name=CNA_ORDEM, 
                                    value_name=CNA_CNAE)
    
                        df = df[df[CNA_CNAE] != '0000000']
    
                    elif tipo_registro == '0': # header
                        print('\nINFORMACOES DO HEADER:')
    
                        header = df.iloc[0,:]
    
                        for k, v in header.items():
                            print('{}: {}'.format(k, v))
    
                        # Para evitar que tente armazenar dados de header
                        continue
    
                    elif tipo_registro == '9': # trailler
                        print('\nINFORMACOES DE CONTROLE:')
    
                        trailler = df.iloc[0,:]
    
                        controle_empresas = int(trailler['Total de registros de empresas'])
                        controle_socios = int(trailler['Total de registros de socios'])
                        controle_cnaes = int(trailler['Total de registros de CNAEs secundarios'])
    
                        print('Total de registros de empresas: {}'.format(controle_empresas))
                        print('Total de registros de socios: {}'.format(controle_socios))
                        print('Total de registros de CNAEs secundarios: {}'.format(controle_cnaes))
                        print('Total de registros incluindo header e trailler: {}'.format(
                                int(trailler['Total de registros incluindo header e trailler'])))
    
                        # Para evitar que tente armazenar dados de trailler
                        continue
    
                    if tipo_output == 'csv':
                        if (i_arq + i_bloco) > 0:
                            replace_append = 'a'
                            header=False
                        else:
                            replace_append = 'w'
                            header=True
    
                        nome_arquivo_csv = REGISTROS_TIPOS[tipo_registro] + '.csv'
                        df.to_csv(os.path.join(output_path,nome_arquivo_csv), 
                                header=header,
                                mode=replace_append,
                                index=False,
                                quoting=csv.QUOTE_NONNUMERIC)
    
    def treating_empresas(file):
    '''     Essa função recebe um arquivo csv e trata os dados.Os arquivos são 
        lidos e salvos em pedaços(chunk) de 500000 linhas(chunksize). Esse 
        tamanho foi estimado para que cada arquivo tenha no máximo 70MB de 
        tamanho e não consuma muita memória no momento do tratamento. Algumas 
        cidades, como São Paulo e Belo Horizonte tem um tamanho considerável, 
        chegando até a 2GB.
    ''' 
        #--- Definindo n que será utilizado para nomear os arquivos divididos.
        #--- A cada iteração, no mesmo arquivo n terá o valor aumentado em 1
        n = 1
        try:
            for chunk in pd.read_csv(file, header = None,index_col = None, 
                                    sep = ',', encoding = 'utf-8' , dtype =str ,
                                    usecols = range(38),chunksize =500000):
                df = chunk
                df.drop_duplicates(inplace=True)
                df.fillna('-',inplace = True)
                df = df.astype(str)
                #--- Substituindo valores nos campos
                df[1].replace({'1':'matriz','2':'filial'}, inplace= True)
                df[4].replace({'01':'nula','02':'ativa','03':'suspensa',
                                '04':'inapta','08':'baixada'}, inplace=True)
                
                df[31].replace({'00':"Não informado", '01':"Micro empresa", 
                                '02': "Pequeno porte", '05': "Demais"},
                                inplace=True )
                df[32].replace({'0':"Não optante",'5':"Optante", '7':"Optante", 
                                '6':"Excluído", '8':"Excluído"},inplace=True)
                df = df.drop(columns= [7])
                df = df.drop(columns= [8])
                df = df.drop(columns= [9])
                #---Nomeação de colunas
                df.columns = ["cnpj","id","nome_empresarial", "nome_Fantasia",
                            "situacao_cadastral","data_da_situacao_cadastral",
                            "motivo_cadastral","cod_natureza_juridica",
                            "data_inicio_ativ","cnae_fiscal","tipo_de_logradouro",
                            "logradouro", "numero", "complemento","bairro",
                            "cep","uf","cod_municipio","municipio","ddd", 
                            "telefone","ddd2","telefone2","ddd3", "fax",
                            "e_mail", "qualif_do_responsavel","capital_social",
                            "porte","opcao_pelo_simples","data_opcao_pelo_simples",
                            "data_exclusao_do_simples","opcao_pelo_MEI",
                            "situacao_especial","data_situacao_especial"
                            ]
                nome = str(file).replace('.csv','')
                #--- Salvando os pedaços de arquivo, acrescido de n . 'GUAXUPE1.csv'
                df.to_csv(nome + str(n) + '.csv' , index = False, 
                        index_label = 'CNPJ')
                n += 1
                
            os.remove(str(file))
            
            return True
        except :
            return False
    
    
def socios_clean(file):
    '''Abre arquivo contendo informações sobre sócios e trata os dados. Lê e 
    salva os arquivos por pedaços. O tamanho de 500000 linhas foi estimado 
    para que os csv tenham no máximo 60MB quando salvos, para otimizar o uso 
    da memória.'''
    
    n = 1
    print('Tratando tabelas de sócios')
    
    for chunk in pd.read_csv(file_path , header = 0 ,index_col = None, 
                            sep = ',', encoding = 'utf-8' , dtype =str , 
                            chunksize =500000):
        df = chunk
        spinner.next()        
        df.drop_duplicates(inplace=True)
        spinner.next()
        df.fillna('-',inplace = True)
        spinner.next()
        df = df.astype(str)
        spinner.next()
        df['tipo_socio'].replace({'1':'Pessoa jurídica','2':'Pessoa física',
                                '3':'Extrangeiro'}, inplace= True)
        spinner.next()
        df['cod_qualificacao'].replace(qual_socio, inplace=True)
        spinner.next()
        nome = str(file_dir) + '/socios/' + 'socios' + str(n) + '.csv'
        df.to_csv(nome , index = False, index_label = 'cnpj')
        n += 1
        spinner.next()
                    
    
            
import os

from progress.spinner import LineSpinner
import pandas as pd

from helpers.cfwf import read_cfwf
from data.cod_qualificacao import qual_socio, tipo_socio 
from data import var , path
from config import DB, db_connexion

class Empresas:
    csvDirectory = path.csvDirPath
    
    # def __init__(self,file):
    #     self.file = file
        
    
    def values_replace(self,df):
        df[var.EMP_DATA_OPC_SIMPLES] = (df[var.EMP_DATA_OPC_SIMPLES]
                .where(df[var.EMP_DATA_OPC_SIMPLES] != '00000000',''))
        df[var.EMP_DATA_EXC_SIMPLES] = (df[var.EMP_DATA_EXC_SIMPLES]
                .where(df[var.EMP_DATA_EXC_SIMPLES] != '00000000',''))
        df[var.EMP_DATA_SIT_ESPECIAL] = (df[var.EMP_DATA_SIT_ESPECIAL]
                .where(df[var.EMP_DATA_SIT_ESPECIAL] != '00000000',''))
        df.drop_duplicates(inplace=True)
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df['matriz_filial'].replace({'1':'matriz','2':'filial'}, inplace= True)
        df['situacao'].replace({'01':'nula','02':'ativa','03':'suspensa',
                        '04':'inapta','08':'baixada'}, inplace=True)
                
        df['porte'].replace({'00':"Não informado", '01':"Micro empresa", 
                        '02': "Pequeno porte", '05': "Demais"},
                        inplace=True )
        df['opc_simples'].replace({'0':"Não optante",'5':"Optante", '7':"Optante", 
                        '6':"Excluído", '8':"Excluído"},inplace=True)
        
        return df
    
    def name_columns(self,df):
        df.columns = ["cnpj","id","nome_empresarial", "nome_Fantasia",
                    "situacao_cadastral","data_da_situacao_cadastral",
                    "motivo_cadastral",'nm_cidade_exterior', 'cod_pais', 
                    'nome_pais',"cod_natureza_juridica",
                    "data_inicio_ativ","cnae_fiscal","tipo_de_logradouro",
                    "logradouro", "numero", "complemento","bairro",
                    "cep","uf","cod_municipio","municipio","ddd", 
                    "telefone","ddd2","telefone2","ddd3", "fax",
                    "e_mail", "qualif_do_responsavel","capital_social",
                    "porte","opcao_pelo_simples","data_opcao_pelo_simples",
                    "data_exclusao_do_simples","opcao_pelo_MEI",
                    "situacao_especial","data_situacao_especial"
                    ]
        return df
    
    # def saveCsv(self,df,n):
    #     nome = str(self.file).replace('.csv','')
    #     #--- Salvando os pedaços de arquivo, acrescido de n . 'GUAXUPE1.csv'
    #     df.to_csv(nome + str(n) + '.csv' , index = False, 
    #             index_label = 'CNPJ')
        
    #     return True
    
    # def startDataClean(self):
    #     n = 1
    #     try:
    #         for chunk in pd.read_csv(self.file, header = None,index_col = None, 
    #                                 sep = ',', encoding = 'utf-8' , dtype =str ,
    #                                 usecols = range(38),chunksize =500000):
    #             df = chunk
    #             df = self.valuesReplace(df)
    #             df = self.dropElements(df)
    #             df = self.nameColumns(df)
    #             self.saveCsv(df,n)
    #             n +=1
    #         os.remove(str(self.file))
    #         return True
    
    #     except:
    #         return False


class Socios:
    file_path = str(path.socios_dir) + '/socios.csv'
    
    # def __init__(self, file):
    #     self.file = file
    
    def values_replace(self,df):
        
        df.drop_duplicates(inplace=True)
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df['tipo_socio'].replace({'1':'Pessoa jurídica','2':'Pessoa física',
                                    '3':'Extrangeiro'}, inplace= True)
        df['cod_qualificacao'].replace(qual_socio, inplace=True)
        return df
        
    # def saveCsv(self,df,n):
        
    #     nome = str(self.file_path).replace('.csv','')  + str(n) + '.csv'
    #     df.to_csv(nome , index = False, index_label = 'cnpj')
    #     return True
    
    # def startDataClean(self):
    #     n = 1
    #     try:
    #         for chunk in pd.read_csv(self.file, header = None,index_col = None, 
    #                                 sep = ',', encoding = 'utf-8' , dtype =str ,
    #                                 usecols = range(38),chunksize =500000):
    #             df = chunk
    #             df = self.valuesReplace(df)
    #             df = self.saveCsv(df,n)
    #             n +=1
    #         os.remove(str(self.file))
    #         return True
        
    #     except:
    #         return False

class Data_Processor:
    input_list = path.input_list
    data_path = path.data_path
        
    def process_data_in_chunks(self):
        print('Starting')
        total_empresas = 0
        for i_arq, arquivo in enumerate(self.input_list):
            print('Processando arquivo: {}'.format(arquivo))

            dados = read_cfwf(arquivo, 
                            type_width=1, 
                            colspecs= {'0':var.header_colspecs,
                                        '1':var.empresas_colspecs,
                                        '2':var.socios_colspecs,
                                        '6':var.CNAES_COLSPECS,
                                        '9':var.trailler_colspecs},
                            names={'0':var.header_colnomes,
                                    '1':var.empresas_colnomes, 
                                    '2':var.socios_colnomes,
                                    '6':var.CNAES_COLNOMES,
                                    '9':var.trailler_colnomes},
                            dtype={'1': var.EMPRESAS_DTYPE,
                                    '2': var.SOCIOS_DTYPE},
                            chunksize=var.CHUNKSIZE,
                            encoding='ISO-8859-15')

            spinner = LineSpinner('Working')
            for i_bloco, bloco in enumerate(dados):
                print(
                    'Bloco {}: até linha {}. [Emps:{}]'.format(i_bloco+1,(i_bloco+1)*var.CHUNKSIZE,  total_empresas),end='\r'
                    )

                for tipo_registro, df in bloco.items():
                    if tipo_registro == '1': # empresas
                        total_empresas += len(df)
                        data_frame = Empresas()
                        df = data_frame.values_replace(df)
                        df = data_frame.name_columns(df)
                        table = 'empresas'
                        spinner.next()

                    elif tipo_registro == '2': # socios
                        data_frame = Socios()
                        df = data_frame.values_replace(df)
                        
                        table = 'socios'
                        spinner.next()

                    elif tipo_registro == '6': # cnaes_secundarios
                        
                        # Verticaliza tabela de associacao de cnaes secundarios,
                        # mantendo apenas os validos (diferentes de 0000000)
                        df = pd.melt(df,
                                    id_vars=[var.CNA_CNPJ],
                                    value_vars=range(99),
                                    var_name=var.CNA_ORDEM,
                                    value_name=var.CNA_CNAE)

                        df = df[df[var.CNA_CNAE] != '0000000']
                        table = 'cnae_secundarios'
                        spinner.next()

                    elif tipo_registro == '0': # header
                        print('\nINFORMACOES DO HEADER:')

                        header = df.iloc[0,:]

                        for k, v in header.items():
                            print('{}: {}'.format(k, v))

                        # Para evitar que tente armazenar dados de header
                        spinner.next()
                        continue

                    elif tipo_registro == '9': # trailler
                        print('\nINFORMACOES DE CONTROLE:')

                        trailler = df.iloc[0,:]
                        controle_empresas = int(trailler['Total de registros de empresas'])
                        print('Total de registros de empresas: {}'.format(controle_empresas))
                        # Para evitar que tente armazenar dados de trailler
                        spinner.next()
                        continue
                        
                    
                    to_db = DB()
                    spinner.next()
                    # to_db.insertMany(df,table, db_connexion)
                    to_db.insertMany(df,table)
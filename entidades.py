import config
import os
from data.variaveis import *
from data.cfwf import read_cfwf
import pandas as pd
from data import cod_qualificacao 

class Empresas:
    csvDirectory = config.csvDirPath
    
    def __init__(self,file):
        self.file = file
        
    
    def valuesReplace(self,df):
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df[1].replace({'1':'matriz','2':'filial'}, inplace= True)
        df[4].replace({'01':'nula','02':'ativa','03':'suspensa',
                        '04':'inapta','08':'baixada'}, inplace=True)
                
        df[31].replace({'00':"Não informado", '01':"Micro empresa", 
                        '02': "Pequeno porte", '05': "Demais"},
                        inplace=True )
        df[32].replace({'0':"Não optante",'5':"Optante", '7':"Optante", 
                        '6':"Excluído", '8':"Excluído"},inplace=True)
        
        return df
    
    def dropElements(self,df):
        df.drop_duplicates(inplace=True)
        df = df.drop(columns= [7])
        df = df.drop(columns= [8])
        df = df.drop(columns= [9])
        
        return df
    
    def nameColumns(self,df):
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
        return df
    
    def saveCsv(self,df,n):
        nome = str(self.file).replace('.csv','')
        #--- Salvando os pedaços de arquivo, acrescido de n . 'GUAXUPE1.csv'
        df.to_csv(nome + str(n) + '.csv' , index = False, 
                index_label = 'CNPJ')
        
        return True
    
    def startDataClean(self):
        n = 1
        try:
            for chunk in pd.read_csv(self.file, header = None,index_col = None, 
                                    sep = ',', encoding = 'utf-8' , dtype =str ,
                                    usecols = range(38),chunksize =500000):
                df = chunk
                df = self.valuesReplace(df)
                df = self.dropElements(df)
                df = self.nameColumns(df)
                self.saveCsv(df,n)
                n +=1
            os.remove(str(self.file))
            return True
    
        except:
            return False

class Socios:
    file_path = str(config.socios_dir) + '/socios.csv'
    
    def __init__(self, file):
        self.file = file
    
    def valuesReplace(self,df):
        
        df.drop_duplicates(inplace=True)
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df['tipo_socio'].replace({'1':'Pessoa jurídica','2':'Pessoa física',
                                    '3':'Extrangeiro'}, inplace= True)
        df['cod_qualificacao'].replace(qual_socio, inplace=True)
        
    def saveCsv(self,df,n):
        
        nome = str(self.file_path).replace('.csv','')  + str(n) + '.csv'
        df.to_csv(nome , index = False, index_label = 'cnpj')
        return True
    
    def startDataClean(self):
        n = 1
        try:
            for chunk in pd.read_csv(self.file, header = None,index_col = None, 
                                    sep = ',', encoding = 'utf-8' , dtype =str ,
                                    usecols = range(38),chunksize =500000):
                df = chunk
                df = self.valuesReplace(df)
                df = self.saveCsv(df,n)
                n +=1
            os.remove(str(self.file))
            return True
        
        except:
            return False

class Box:
    input_list = config.input_list
    dataPath = config.dataPath
    
    def read_file(self,arquivo):
        
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
        
        return dados
    
    def changeValuesEmpresas(self,df):
        
        df[EMP_DATA_OPC_SIMPLES] = (df[EMP_DATA_OPC_SIMPLES]
            .where(df[EMP_DATA_OPC_SIMPLES] != '00000000',''))
        df[EMP_DATA_EXC_SIMPLES] = (df[EMP_DATA_EXC_SIMPLES]
            .where(df[EMP_DATA_EXC_SIMPLES] != '00000000',''))
        df[EMP_DATA_SIT_ESPECIAL] = (df[EMP_DATA_SIT_ESPECIAL]
            .where(df[EMP_DATA_SIT_ESPECIAL] != '00000000',''))
        
        return df
import os
import glob
import sys
import csv
import datetime

import pandas as pd
from progress.spinner import LineSpinner

from data import path
from data import cod_qualificacao
from config import DB
from data import var
from helpers.cfwf import read_cfwf

class DataClean:
    dir = path.csvDirPath                             # ./dados/UFs
    dataPath = path.data_path                          # ./dados
    file_path = str(path.socios_dir) + '/socios.csv'
    input_list = path.input_list

    def prepare_df_to_db(self):
        """Essa função recebe uma lista com os arquivos zips, baixados do site
        da receita federal, o tipo de output (CSV) e o diretório onde deverão
        ser salvos os 3 arquivos resultantes do processo('empresas.csv',
        'socios.csv' e 'cnaesecundario.csv')
        """
        total_empresas = 0

        if not os.path.exists(self.dataPath):
            os.makedirs(self.dataPath)

        # Itera sobre sequencia de arquivos (p/ suportar arquivo dividido pela RF)

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

            # Itera sobre blocos (chunks) do arquivo
            for i_bloco, bloco in enumerate(dados):
                print(
                    'Bloco {}: até linha {}. [Emps:{}|Socios:{}|CNAEs:{}]'.format(i_bloco+1,(i_bloco+1)*var.CHUNKSIZE,total_empresas),end='\r'
                    )

                for tipo_registro, df in bloco.items():

                    if tipo_registro == '1': # empresas
                        total_empresas += len(df)
                        df = self.treating_empresas(df)
                        table = 'empresas'

                    elif tipo_registro == '2': # socios

                        df = self.socios_clean(df)
                        table = 'socios'

                    elif tipo_registro == '6': # cnaes_secundarios
                        total_cnaes += len(df)

                        # Verticaliza tabela de associacao de cnaes secundarios,
                        # mantendo apenas os validos (diferentes de 0000000)
                        df = pd.melt(df,
                                    id_vars=[var.CNA_CNPJ],
                                    value_vars=range(99),
                                    var_name=var.CNA_ORDEM,
                                    value_name=var.CNA_CNAE)

                        df = df[df[var.CNA_CNAE] != '0000000']
                        table = 'cnae_secundarios'

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
                        print('Total de registros de empresas: {}'.format(controle_empresas))
                        # Para evitar que tente armazenar dados de trailler
                        continue


                    DB.insertMany(df,table)

    def treating_empresas(self,df):

        try:
            df[var.EMP_DATA_OPC_SIMPLES] = (df[var.EMP_DATA_OPC_SIMPLES]
                    .where(df[var.EMP_DATA_OPC_SIMPLES] != '00000000',''))
            df[var.EMP_DATA_EXC_SIMPLES] = (df[var.EMP_DATA_EXC_SIMPLES]
                    .where(df[var.EMP_DATA_EXC_SIMPLES] != '00000000',''))
            df[var.EMP_DATA_SIT_ESPECIAL] = (df[var.EMP_DATA_SIT_ESPECIAL]
                    .where(df[var.EMP_DATA_SIT_ESPECIAL] != '00000000',''))
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
            return df
        except Exception as e:
            print('Erro ao processar dados de empresas')
            print(e)
            return False

    def socios_clean(self, df):
        # Troca cpf invalido por vazio
        df[var.SOC_CPF_REPRES] = (df[var.SOC_CPF_REPRES]
                .where(df[var.SOC_CPF_REPRES] != '***000000**',''))
        df[var.SOC_NOME_REPRES] = (df[var.SOC_NOME_REPRES]
                .where(df[var.SOC_NOME_REPRES] != 'CPF INVALIDO',''))

        # Se socio for tipo 1 (cnpj), deixa campo intacto, do contrario,
        # fica apenas com os ultimos 11 digitos
        df[var.SOC_CNPJ_CPF_SOCIO] = (df[var.SOC_CNPJ_CPF_SOCIO]
                .where(df[var.SOC_TIPO_SOCIO] == '1',
                        df[var.SOC_CNPJ_CPF_SOCIO].str[-11:]))
        df.drop_duplicates(inplace=True)
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df['tipo_socio'].replace({'1':'Pessoa jurídica','2':'Pessoa física',
                                '3':'Extrangeiro'}, inplace= True)
        df['cod_qualificacao'].replace(qual_socio, inplace=True)

        return df


if __name__ == '__main__':
    print('Starting')
    DataClean.prepare_df_to_db()
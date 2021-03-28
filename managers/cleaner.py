import csv
import os

import pandas as pd
from progress.spinner import LineSpinner

from constants import path, var


class Empresas:
    csvDirectory = path.csv_path
        
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

class Socios:
    file_path = str(path.socios_dir) + '/socios.csv'
        
    def values_replace(self,df):

        df.drop_duplicates(inplace=True)
        df.fillna('-',inplace = True)
        df = df.astype(str)
        df['tipo_socio'].replace({'1':'Pessoa jurídica','2':'Pessoa física',
                                    '3':'Extrangeiro'}, inplace= True)
        df['cod_qualificacao'].replace(qual_socio, inplace=True)
        return df
    
    
class Data_Processor:
    input_list = path.input_list
    data_path = path.data_path
    
    def process_data_in_chunks(self):
        print('Starting')
        total_empresas = 0
        for i_arq, arquivo in enumerate(self.input_list):
            print('Processando arquivo: {}'.format(arquivo))

            dados = self.read_cfwf(arquivo, 
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
                        table = 'cnaes_secundarios'
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
                        
                    
                    if (i_arq + i_bloco) > 0:
                        replace_append = 'a'
                        header=False
                    else:
                        replace_append = 'w'
                        header=True

                    nome_arquivo_csv = var.REGISTROS_TIPOS[tipo_registro] + '.csv'
                    df.to_csv(os.path.join(path.data_path,nome_arquivo_csv), 
                            header=header,
                            mode=replace_append,
                            index=False,
                            quoting=csv.QUOTE_NONNUMERIC)
                    
    def read_cfwf(self,filepath_or_buffer, type_width, colspecs, names=None,
            dtype=None, chunksize=None, nrows=None, compression='infer',
            encoding=None):
        '''Read complex fixed-width formatted lines, which are fixed-width formatted
        files with different line types, each one possibly having different
        colspecs, names and dtypes. Returns a dict of line type -> pandas.DataFrame.

        Also supports optionally breaking of the file into chunks.

        Arguments:
        filepath_or_buffer -- str, pathlib.Path, py._path.local.LocalPath or any
            object with a read() method (such as a file handle or StringIO).
        type_width -- int
            Number of characters indicating the line type in the beginning of each 
            line.
        colspecs -- dict of line type -> list of pairs (int, int).
            A dict of list of pairs (tuples) giving the extents of the fixed-width
            fields of each line as half-open intervals (i.e., [from, to[ ), for each
            line type. The line types included in the colspecs indicates which line 
            types are supposed to be read. Lines with other types will be ignored.
        names -- dict of line type -> list, default None
            dict of list of column names to use, one list for each line type.
        dtype -- dict of line type -> dict of column -> type, default None
            Data type for columns, for each line type. If not specified for a
            specific column, data will be kept as str.
        chuncksize -- int, default None
            If specified, break the file into chunks and returns a generator.
        nrows -- int, default None
            Limit the number of lines to be read.
        '''

        # Calculate line width as the maximum 
        # position number from the colspecs.
        line_width = max([max(colspec)[1] for colspec in colspecs.values()])

        # Read raw file as a two column dataframe, one column for the line type
        # and the other column for the line content (to be split later).
        raw_data = pd.read_fwf(filepath_or_buffer,
                               colspecs=[(0,type_width),(type_width,line_width)],
                               names=['line_type','_content'],
                               dtype=str,
                               header=None,
                               delimiter='\t', # To avoid autostrip content
                               chunksize=chunksize,
                               nrows=nrows,
                               compression=compression,
                               encoding=encoding)

        if chunksize is None:
            return _cfwf_chunck(raw_data, 
                                type_width, 
                                colspecs, 
                                names, 
                                dtype)
        else:
            return _cfwf_chunck_reader(raw_data, 
                                       type_width, 
                                       colspecs, 
                                       names, 
                                       dtype)


    def _cfwf_chunck(self,df, type_width, colspecs, names=None, dtype=None):

        df.set_index('line_type', inplace=True)

        data_dict = {}

        # For each line type specified in colspecs.
        for ltype, specs in colspecs.items():
            try:
                # Get all rows corresponding to the line type.
                data = df.loc[[ltype],:].copy()

                # Create columns spliting content according to colspecs.
                for i, column in enumerate(specs):
                    data[i] = (data['_content']
                                .str.slice( column[0]-type_width, 
                                            column[1]-type_width)
                                .str.strip())

                # Original content column not necessary anymore.
                data_dict[ltype] = data.drop('_content', axis=1)

                # Change column names according to parameter "names".
                if names is not None:
                    data_dict[ltype].columns = names[ltype]

                # If dtypes specified and only if specified 
                # for this specific line type.
                if (dtype is not None) & (ltype in dtype):
                    # Change column dtypes according to parameter "dtype"
                    for col_name, col_type in dtype[ltype].items():
                        data_dict[ltype][col_name] = (data_dict[ltype][col_name]
                                                        .astype(col_type))

            except KeyError:
                pass

        return data_dict    

    def _cfwf_chunck_reader(self,reader, type_width, colspecs, names=None, dtype=None):

        for chunk in reader:
            yield _cfwf_chunck(chunk, type_width, colspecs, names, dtype)

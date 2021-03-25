import os

import pandas as pd


from collectors.receita_federal import SiteRF
from constants import estados, path
from data.cleaner import Data_Processor
from database import MongoDB, connexion_mongo


def check_for_update():
    update_date_BD = MongoDB.checkUpdateDate()
    update_date_RF = SiteRF.check_RF_update_date()
    print(update_date_RF)
    print(update_date_BD)
    return update_date_RF > update_date_BD


def create_directories():
    if not os.path.exists(path.data_path):
        os.mkdir(path.data_path)
    if not os.path.exists(path.zip_path):
        os.mkdir(path.zip_path)
    return True


def init_update():
    urls = SiteRF().search_download_urls()
    for url in urls:
        init_process.download_file_wget(url)
        print('Starting cleaning data ')
        Data_Processor.process_data_in_chunks()
        os.remove(path.zip_path + '/file.zip')
        separar_csv_por_cidade()
        # Inserir socios
        # inserir cnaes_secundarios
        # Deletar CSVs
    separar_csv_por_cidade()


# Aqui devo iterar por todos os arquivos da pasta CSVs e jogar pro banco de dados
for uf in estados.estados:
    dir = str(path.csv_path) + '/' + uf
    os.chdir(dir)
    for f in glob.glob('*.*'):
        try:
            # bar.next()
            InterfaceDB().insert_many(file, 'empresas')

        except:
            pass


# #CRIAR ÍNDICES

def cnaesecundario_to_mongoDB():
    """Essa função insere o arquivo cnae_secundarios.csv no banco de dados mongoDB."""
    db = connexion_mongo()
    if 'cnaesecundario' in db.list_collection_names():
        db.cnaesecundario.rename('cnaesecundario2')
    print('Insert CNAEs secundários ')
    cnae_dir = str(diretorio_cnaessecundarios) + 'cnaes_secundarios.csv'
    InterfaceDB().insert_many(cnae_dir, 'cnae_secundario')

    db.cnaesecundario2.drop()


def socios_to_mongoDB():
    """Essa função insere os dados do arquivo socios.csv no banco de dados MongoDB
    """
    db = connexion_mongo()
    if 'socios' in db.list_collection_names():
        if 'socios2' in db.list_collection_names():
            result = db.socios.find({}, {'_id': 0})
            lista = []
            for i in result:
                lista.append(i)
            print('Transferindo dados para outra coleção')
            db.socios3.insert_many(lista)
            db.socios.drop()
            db.socios.rename('socios3')
        else:
            db.socios.rename('socios3')

    print('Insert sócios ')
    os.chdir(str(file_dir) + 'socios/')
    for file in glob.glob('*.*'):
        InterfaceDB().insert_many(file, 'socios')
    db.socios2.drop()

    criando_parametros()


def read_cfwf(filepath_or_buffer, type_width, colspecs, names=None,
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


def _cfwf_chunck(df, type_width, colspecs, names=None, dtype=None):

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
                            .str.slice(column[0]-type_width, 
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

def _cfwf_chunck_reader(reader, type_width, colspecs, names=None, dtype=None):

    for chunk in reader:
        yield _cfwf_chunck(chunk, type_width, colspecs, names, dtype)

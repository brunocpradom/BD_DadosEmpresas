import os
import glob
import csv
import codecs

from progress.bar import FillingSquaresBar

from collectors.receita_federal import SiteRF
from constants import estados, path
from managers.cleaner import Data_Processor
from database import DB
from managers.processor import criando_parametros


def check_for_update():
    update_date_BD = DB().checkUpdateDate()
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
        SiteRF().download_file_wget(url)
        print('Starting cleaning data ')
        Data_Processor.process_data_in_chunks()
        os.remove(path.zip_path + '/file.zip')
        separar_csv_por_cidade()
    insert_data_to_db()
    socios_to_mongoDB()
    cnaesecundario_to_mongoDB()
    DB().create_index()

def insert_data_to_db():
    
    for uf in estados.estados:
        dir = str(path.csv_path) + '/' + uf
        os.chdir(dir)
        city_list_lenght = 0
        for file in glob.glob('*.*'):
            city_list_lenght +=1

        bar = FillingSquaresBar(uf, max = city_list_lenght)
        for file in glob.glob('*.*'):
            try:
                bar.next()
                DB().insert_many(file, 'empresas')

            except:
                pass

def cnaesecundario_to_mongoDB():
    """Essa função insere o arquivo cnae_secundarios.csv no banco de dados mongoDB."""
    db = DB().connexion()
    if 'cnaesecundario' in db.list_collection_names():
        db.cnaesecundario.rename('cnaesecundario2')
    print('Insert CNAEs secundários ')
    cnae_dir = str(path.data_path) + '/cnaes_secundarios.csv'
    DB().insert_many(cnae_dir, 'cnae_secundario')

    db.cnaesecundario2.drop()


def socios_to_mongoDB():
    """Essa função insere os dados do arquivo socios.csv no banco de dados MongoDB
    """
    db = DB().connexion()
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
    os.chdir(str(path.data_path) + 'socios/')
    for file in glob.glob('*.*'):
        DB().insert_many(file, 'socios')
    db.socios2.drop()

    criando_parametros()


def separar_csv_por_cidade():
    location = path.csv_path
        
    file = 'empresas.csv'
    
    with codecs.open(location + file,'r+','utf-8') as empresas:
    
        empresas = csv.reader(cleaned.replace('\0', '') for cleaned in empresas)
    
        next(empresas,None)
    
        count_sucesso = 0
        count_erro = 0
    
        if not os.path.exists(location + 'UFs/'):
            os.makedirs(location + 'UFs/')
    
        for empresa in empresas:
    
            uf = empresa[get_header_index('uf')]
            municipio = empresa[get_header_index('municipio')]
    
            if not os.path.exists(location + 'UFs/' + uf):
                os.makedirs(location + 'UFs/' + uf)
    
            try:
                uf_file = open(location + 'UFs/' + uf + "/" + municipio + ".csv", "a+")
            except:
                uf_file = open(location + 'UFs/' + uf + "/" + municipio + ".csv", "w+")
    
            line = '"' + '","'.join(empresa) + '"\n'
    
            uf_file.writelines(line)
    
            try:
                uf_file.writelines('"' + '","'.join(empresa) + '"\n')
                count_sucesso += 1
            except:
                count_erro += 1
                print('Erros de processamento: {}'.format(count_erro),end='\r')
    
            print('Empresas processadas: {}'.format(count_sucesso),end='\r')
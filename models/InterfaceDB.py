from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from models.models import Empresas, Config, Base

class SQL():   

    # def search(self,search_params_dict):
    #     empresa =self.session.query(Empresas).filter_by(CNPJ = cnpj)
        # return list(empresa)

    def insertMany(self,df , table,db_connexion):
        engine = create_engine(db_connexion)
        Session = sessionmaker(bind = engine)
        session = Session()
        if not database_exists(db_connexion):
            create_database(engine.url)
            Base.metadata.create_all(engine, checkfirst =True)

        df.to_sql(table, con = engine , if_exists = 'append',index = False )
        pass

    # def checkUpdateDate(self):
    #     result =self.session.query(Config).filter(
    #             Config.ultima_atualizacao.like('\d\d/\d\d/\d\d\d\d')
    #             )
    #     #Processar as informações de result e retornar apenas a data

    #     return dateDB

class MongoDB():
    # def __init__(self):
    #     """Estabele a conexão com o banco de dados.
    #     """
    #     uri = "mongodb://bcpm:ddd@172.17.0.2:port"
    #     self.db = MongoClient('172.17.0.2')

    # def search(self, search_params_dict):
    #     """Essa função recebe um dicionário com os parâmetros a serem buscados.
    #     E retorna uma lista com os resultados da pesquisa
    #     Ex: search_params = {'cnpj':'000000000000', 'cnae_fiscal':'0000000'}
    #     """
    #     result = self.db.empresas.find(search_params_dict,{'_id':0})

    #     return list(result)

    def insertMany(self, df , table):
        
        try:
            client = MongoClient('172.17.0.2')
            db = client.dados_empresas
            documents = df.to_dict(orient = 'records')
            if table == 'empresas':
                db.empresas.insert_many(documents)
            if table == 'socios':
                db.socios.insert_many(documents)
            if table == 'cnaes_secundarios':
                db.cnaesecundario.insert_many(documents)
            return True

        except Exception as e:
            print(e)
            print('Error inserting')
            return False


    # def checkUpdateDate(self):
    #     result = self.db.atualizacao.find(
    #             {'ultima_atualizacao':{'$regex': '\d\d/\d\d/\d\d\d\d'}},
    #             {'_id':0}
    #             )
    #     dateDB = parse(list(result)[0]['ultima_atualizacao'])
    #     return dateDB






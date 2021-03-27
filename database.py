import csv

import pymongo
from dateutil.parser import parse
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

#MongoDB
host = '172.17.0.2'
port = ''
username = ''
password = ''
#SQL

class SQL():
    db_connexion = "mysql://bcpm:ddd@172.17.0.2/dados_empresas"

    # def search(self,search_params_dict):
    #     empresa =self.session.query(Empresas).filter_by(CNPJ = cnpj)
    # return list(empresa)
    def connexion(self):
        pass
    def insert_many(self, df, table):
        engine = create_engine(self.db_connexion)
        Session = sessionmaker(bind=engine)
        session = Session()

        if not database_exists(self.db_connexion):
            create_database(engine.url)
            Base.metadata.create_all(engine, checkfirst=True)

        df.to_sql(table, con=engine, if_exists='append', index=False)
        pass

    def create_index(self):
        pass

    # def checkUpdateDate(self):
    #     result =self.session.query(Config).filter(
    #             Config.ultima_atualizacao.like('\d\d/\d\d/\d\d\d\d')
    #             )
    #     #Processar as informações de result e retornar apenas a data

    #     return dateDB


class MongoDB():
    # db = connexion_mongo()
    def __init__(self):
        self.db = self.connexion()

    def connexion(self):
        client = MongoClient(host)
        db = client.dados_empresas
        return db

    # def search(self, search_params_dict):
    #     """Essa função recebe um dicionário com os parâmetros a serem buscados.
    #     E retorna uma lista com os resultados da pesquisa
    #     Ex: search_params = {'cnpj':'000000000000', 'cnae_fiscal':'0000000'}
    #     """
    #     result = self.db.empresas.find(search_params_dict,{'_id':0})

    #     return list(result)

    def insert_many(self, file, table):
        try:
            list_of_dict = []
            file = csv.DictReader(open(str(file), encoding='utf-8'))
            for row in file:
                # conn.execute(ins,row)
                list_of_dict.append(row)
            documents = list_of_dict

            if table == 'empresas':
                empresas = self.db.empresas
                result = empresas.insert_many(documents)
                result.inserted_ids
            if table == 'socios':
                socios = self.db.empresas
                result = socios.insert_many(documents)
                result.inserted_ids
            if table == 'cnaes_secundarios':
                cnaes_secundarios = self.db.empresas
                result = cnaes_secundarios.insert_many(documents)
                result.inserted_ids

        except Exception as e:
            print(e)
            print('Error inserting')
            return False

    def create_index(self):
        # Empresas
        self.db.empresas.create_index([('cnpj', pymongo.ASCENDING)])
        self.db.empresas.create_index([('CNAE_fiscal', pymongo.ASCENDING)])
        self.db.empresas.create_index([('Município', pymongo.ASCENDING),
                                       ('Situação_cadastral', pymongo.ASCENDING),
                                       ('CNAE_fiscal', pymongo.ASCENDING), ])
        self.db.empresas.create_index([('UF', pymongo.ASCENDING),
                                       ('CNAE_fiscal', pymongo.ASCENDING),
                                       ('Situação_cadastral', pymongo.ASCENDING)])

        # Cnae_secundario
        self.db.cnae_legenda.create_index([('CNAE_fiscal', pymongo.ASCENDING)])

        # Socios
        self.db.socios.create_index([('nome_socio', pymongo.ASCENDING)])
        self.db.socios.create_index([('cnpj', pymongo.ASCENDING)])

    def checkUpdateDate(self):
        update = self.db.atualizacao
        result = update.find(
            {'ultima_atualizacao': {'$regex': '\d\d/\d\d/\d\d\d\d'}},
            {'_id': 0}
        )
        print(list(result))
        if list(result):
            dateDB = parse(list(result)[0]['ultima_atualizacao'])
        else:
            dateDB = parse('01/01/2020')
        return dateDB


class DB(MongoDB):
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """

    # def search(self, search_params_dict):
    #     super().search(search_params_dict)

    def insert_many(self, df, table):
        super().insert_many(df, table)

    def create_index(self):
        super().create_index()

    def checkUpdateDate(self):
        super().checkUpdateDate()

    def connexion(self):
        db = super().connexion()
        return db
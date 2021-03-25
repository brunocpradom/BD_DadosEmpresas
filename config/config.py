from pymongo import MongoClient

#Database config
def connexion_mongo():
    client = MongoClient('172.17.0.2')
    db = client.dados_empresas
    return db

def connexion_sql():
    pass
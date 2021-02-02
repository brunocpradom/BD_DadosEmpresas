from pymongo import MongoClient
#importar Sqlalchemy

class MongoDB():
    def __init__(self):
        """Estabele a conexão com o banco de dados. 
        """        
        self.db = MongoClient().dados_empresas
        
    def search(self, search_params_dict):
        """Essa função recebe um dicionário com os parâmetros a serem buscados.
        E retorna uma lista com os resultados da pesquisa
        Ex: search_params = {'cnpj':'000000000000', 'cnae_fiscal':'0000000'}
        
        """
        result = self.db.empresas.find(search_params,{'_id':0})
        
        return list(result)
        
    def insertMany(self, documents_dict):
        """Essa função recebe um dicionário com os valores a serem inseridos
        e os insere no banco de dados
        """
        try:
            result = self.db.empresas.insert_many(documents_dict)
            return True
        except:
            print('Error inserting')
            return False
            
    
    def checkUpdateDate(self):
        result = self.db.atualizacao.find(
                {'ultima_atualizacao':{'$regex': '\d\d/\d\d/\d\d\d\d'}},
                {'_id':0}
                )
        dateDB = parse(list(result)[0]['ultima_atualizacao'])
        return dateDB
        

class SQL():
    
    def __init__(self):
        self.engine = create_engine(("postgresql://postgres:ddd@172.17.0.2/empresas1")) 
        self.Session = sessionmaker(bind=engine)
        self.session = Session()
        
        
    def search(self,search_params_dict):
        
        empresa =self.session.query(Empresas).filter_by(CNPJ = cnpj)
        
        return list(empresa)
        
    def insertMany(self):
        pass
    
    def checkUpdateDate(self):
        result =self.session.query(Config).filter(
                Config.ultima_atualizacao.like('\d\d/\d\d/\d\d\d\d')
                )
        #Processar as informações de result e retornar apenas a data
        
        return dateDB
        
        
class Interface_BD():
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira 
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """
    def search(self):
        super().search()
        
    def insertMany(self):
        super().insert()
        
    def checkUpdateDate(self):
        super().checkUpdateDate()
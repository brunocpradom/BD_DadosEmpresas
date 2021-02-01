from pymongo import MongoClient
#importar Sqlalchemy

class MongoDB():
    def __init__(self):
        """Essa função estabele a conexão com o banco de dados. Poderia colocar 
        ela em outro arquivo. E conforme o banco de dados que estiver usando, eu 
        a modificaria.
        """        
        self.db = MongoClient().dados_empresas
        

class SQL():
    def __init__(self):
        self.engine = create_engine(("postgresql://postgres:ddd@172.17.0.2/empresas1")) 
        self.Session = sessionmaker(bind=engine)
        self.session = Session()
        
        
class BancoDeDados(MongoDB):
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira 
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """
    def __init__(self):
        super().__init__()
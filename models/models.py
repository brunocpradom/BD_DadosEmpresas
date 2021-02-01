class BancoDeDadosMongoDB():
    def __init__():
        """Essa função estabele a conexão com o banco de dados. Poderia colocar 
        ela em outro arquivo. E conforme o banco de dados que estiver usando, eu 
        a modificaria.
        """        
        self.db = MongoClient().dados_empresas
        

class BancoDeDadosSQL():
    def __init__():
        self.engine = create_engine(("postgresql://postgres:ddd@172.17.0.2/empresas1")) 
        self.Session = sessionmaker(bind=engine)
        self.session = Session()
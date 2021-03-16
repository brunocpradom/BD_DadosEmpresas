from models.InterfaceDB import  SQL, MongoDB

db_connexion = "mysql://bcpm:ddd@172.17.0.2/dados_empresas"

uri = "mongodb://user:password@example.com/?authSource=the_database&authMechanism=SCRAM-SHA-256"

class DB(MongoDB):
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """
    # def search(self, search_params_dict):
    #     super().search(search_params_dict)

    # def insertMany(self,df , table, db_connexion):
    #     super().insertMany(df, table, db_connexion)
    
    def insertMany(self,df , table):
        super().insertMany(df, table)

    # def checkUpdateDate(self):
    #     super().checkUpdateDate()
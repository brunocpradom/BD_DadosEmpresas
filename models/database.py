from InterfaceDB import MongoDB, SQL

class DB(MongoDB):
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """
    # def search(self, search_params_dict):
    #     super().search(search_params_dict)
    
    def insert_many(self,df , table):
        super().insert_many(df, table)

    def create_index(self):
        super().create_index()

    # def checkUpdateDate(self):
    #     super().checkUpdateDate()
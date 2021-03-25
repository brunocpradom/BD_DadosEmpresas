from models.InterfaceDB import  SQL, MongoDB



class DB(MongoDB):
    """Essa classe vai ser herdada por toda classe que faça conexão com o banco
    de dados.Caso queira trabalhar com o mongoDB, ela deve herdá-lo, caso queira
    trabalhar com qualquer banco suportado pelo SQLAlchemy, BancoDeDados deve
    herdar SQL
    """
    # def search(self, search_params_dict):
    #     super().search(search_params_dict)

    def insertMany(self,df , table):
        super().insert_many(df, table)

    # def checkUpdateDate(self):
    #     super().checkUpdateDate()
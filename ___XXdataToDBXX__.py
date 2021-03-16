from sqlalchemy import create_engine

class DataToBD:
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')

    def __init__(self):
        """Função construtora que defini as variáveis necessárias para realizar
        uma conexão com o banco de dados postgres através do SQLAlchemy ORM"""

        self.engine = create_engine(
            "postgresql://postgres:ddd@172.17.0.2/empresas1"
            )

    def df_to_BD(self,df, tabela):

        df.to_sql(
            tabela, con = self.engine , if_exists = 'append',index = False
            )

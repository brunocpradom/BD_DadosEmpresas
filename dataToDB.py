from sqlalchemy import create_engine

class DataToBD:
    
    def __init__(self):
        """Função construtora que defini as variáveis necessárias para realizar
        uma conexão com o banco de dados postgres através do SQLAlchemy ORM"""
        
        self.engine = create_engine(
            "postgresql://postgres:ddd@172.17.0.2/empresas1"
            ) 
            
    def csv_to_BD(file, tabela):
    """Essa função recebe um arquivo(file), .CSV , e insere os dados em um 
    banco de dados.
    """
    
        df =pd.read_csv(file,encoding = 'utf-8')
        df.to_sql(tabela, con = self.engine , if_exists = 'append',
                    index = False)
        
class BancoDeDadosMongoDB():
    def __init__():
        """Essa função estabele a conexão com o banco de dados. Poderia colocar 
        ela em outro arquivo. E conforme o banco de dados que estiver usando, eu 
        a modificaria.
        """
        self.empresas = MongoClient().dados_empresas.empresas
        
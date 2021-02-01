from dateutil.parser import parse  
from models.models import BancoDeDadosMongoDB , BancoDeDadosSQL
#importar conexão com banco de dados

class Update(BancoDeDadosMongoDB):
    def __init__():
        self.db = super().__init__()
        self.last_update = '01/01/1111'
    
    def check_update_date():
        #Checa se o banco de dados escolhido 
        if issuclass(Update,BancoDeDadosMongoDB):
            #Faz a busca na coleção atualização
            result = self.db.atualizacaofind(
                {'ultima_atualizacao':{'$regex': '\d\d/\d\d/\d\d\d\d'}},
                {'_id':0}
                )
            return result
        
        if issubclass(Update,BancoDeDadosSQL):
            #Preciso ajeitar essa busca para buscar pela data de atualização.
            #Primeiro preciso ver como ficará organizado a tabela no BD relacional
            
            empresa =self.session.query(Config).filter(Config.ultima_atualizacao.like('\d\d/\d\d/\d\d\d\d')):
            
            return result
        
        
        dic = {}
        for i in result: 
            dic = i
            print('dic is {}'.format(dic))
            
        if len(dic) == 0:
            date1 = parse('01/01/1111')
        else:        
            date1 = parse(dic['ultima_atualizacao'])
    
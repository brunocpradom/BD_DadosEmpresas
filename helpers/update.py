from dateutil.parser import parse  
from models.models import MongoDB , SQL, BancoDeDados
from models.empresas import Config
import re
from url.request import urlopen
import os
#importar conexão com banco de dados

#!------------------------------------------------------------------#
#!-Esse arquivo conterá todos os códigos que tratem da atualização -#
#!-do banco de dados                                                #
#!------------------------------------------------------------------#


class Update(BancoDeDados):
    def __init__(self):
        self.db = super().__init__()
        self.last_update = '01/01/1111'
    
    def check_DB_update_date(self):
        """Essa função retorna result, que é ou um objeto mongodb ou sqlalchemy
        Talvez a melhor maneira seja tratar esses resultados e devolver eles
        de uma forma padrão
        """
        
        if issuclass(Update,MongoDB):
            #Faz a busca na coleção atualização
            result = self.db.atualizacaofind(
                {'ultima_atualizacao':{'$regex': '\d\d/\d\d/\d\d\d\d'}},
                {'_id':0}
                )
            dateDB = parse(list(result)[0]['ultima_atualizacao'])
            return dateDB
        
        if issubclass(Update,SQL):
            #Preciso ajeitar essa busca para buscar pela data de atualização.
            #Primeiro preciso ver como ficará organizado a tabela no BD relacional
            
            result =self.session.query(Config).filter(
                Config.ultima_atualizacao.like('\d\d/\d\d/\d\d\d\d')
                )
            #Processar as informações de result e retornar apenas a data
            
            return dateDB
        
    def check_RF_update_date(self):
        """Essa função procura pela data de atualização no site da receita federal
        """
        html = urlopen(
            """https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj"""
            )
        urls = re.findall(
            'Data de geração do arquivo: (\d\d/\d\d/\d\d\d\d)',html.read()
            )
        dateRF = parse(url[0])
        return dateRF
    
    def isUpdateAvailable(self):
        dateDB = self.check_DB_update_date
        dateRF = self.check_RF_update_date 
        
        if dateDB < dateRF:
            
            return True
        else :
            return False
        


class SiteRF():
    def __init__(self):
        self.url ="https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj"
        self.zip = '' #Colocar aqui o caminho para a pasta zip. Está em algum arquivo
    
    def search_download_urls(self):
        
        r = requests.get('https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj')
        urls = re.findall(r'http://200.152.38.155/CNPJ/DADOS_ABERTOS_CNPJ_\d*.zip',r.text)
    
        return urls
    
    def download_file(self,url):
        os.chdir(self.zip)
        name = url.split('/')[-1]
        download = requests.get(url,stream = True)
        size = download.headers['Content-length']
        pbar = tqdm(total = int(size),unit = 'B', unit_scale= True, unit_divisor = 1000, desc=url.split('/')[-1])
    
        with open(name, 'wb') as file:
            for chunk in download.iter_content(chunk_size = 1000):
                if chunk:
                    file.write(chunk)
                    pbar.update(1000)
        pbar.close()
        return size
    
    def downloadFilesInParalell(self):
        print('Iniciando download de arquivos.')
        #Com o parallel_backend consigo ver todas as barras de progresso separadamente. Se eu colocar somente o Parallel
        #eu consigo ver a barra de progresso, mas uma de cada vez. Elas ficam se alternando na tela.
        urls = self.search_download_urls()
        with parallel_backend('threading',n_jobs = 4):
            Parallel( verbose = 70)(delayed(self.download_file)(url)for url in urls)
        
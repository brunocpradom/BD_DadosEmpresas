import os
import re

import requests
import wget
from dateutil.parser import parse

from constants.path import zip_path

#importar conexão com banco de dados


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
    
    def download_file_wget(self,url):
        dir = zip_path + '/file.zip'
        wget.download(url , out = dir)
    
    def downloadFilesInParalell(self):
        print('Iniciando download de arquivos.')
        #Com o parallel_backend consigo ver todas as barras de progresso separadamente. Se eu colocar somente o Parallel
        #eu consigo ver a barra de progresso, mas uma de cada vez. Elas ficam se alternando na tela.
        urls = self.search_download_urls()
        with parallel_backend('threading',n_jobs = 4):
            Parallel( verbose = 70)(delayed(self.download_file)(url)for url in urls)
    
        return True
    @classmethod
    def check_RF_update_date(self):
        """Essa função procura pela data de atualização no site da receita federal
        """
        r = requests.get('https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj')
        urls = re.findall(
            'Data de geração do arquivo: (\d\d/\d\d/\d\d\d\d)',r.text
            )
        dateRF = parse(urls[0])
        return dateRF
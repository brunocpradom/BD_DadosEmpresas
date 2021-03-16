import re
import requests
from pymongo import MongoClient
from data.municipios import municipios

def criando_parametros():
    """Essa função acessa o banco de dados MongoDB, que contém as informações referente às empresas,
    calcula quantas empresas tem por setor, e coloca esses valores no mesmo banco de dados, em outra
    coleção. 
    """
    #Buscando a data referente a atualização de dados
    r = requests.get('https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj')
    urls = re.findall('Data de geração do arquivo: (\d\d/\d\d/\d\d\d\d)',r.text)
    
    client = MongoClient('172.17.0.2')
    db = client.dados_empresas 
    if 'empresas_ativas' in db.list_collection_names():
        #Aqui estou copiando os dados da última atualização para a coleção onde ficarão os dados antigos,
        #que serão usados para criar grafico de empresas ativas por trimestre
        
        result = db.empresas_ativas.find({},{'_id':0})
        lista = []
        for i in result :
            lista.append(i)
        
        print('Transferindo dados para outra coleção')
        db.empresas_ativas2.insert_many(lista)
        db.empresas_ativas.drop()
        
        
    #db.coll.find({"mykey":{'$exists': 1}}) #Isso é pra pegar tudo que tem essa key
    
    documents = db.empresas
    
    lista_por_estado =[]
    for f in municipios:
        list_cidades = []
        
        cnae_setor ={'A': "(^01\w*|^02\w*|^03\w*)" ,
                    'B': "(^05\w*|^06\w*|^07\w*|^08\w*|^09\w*)" , 
                    'C': "(^10\w*|^11\w*|^12\w*|^13\w*|^14\w*|^15\w*|^16\w*|^17\w*|^18\w*|^19\w*|^20\w*|^21\w*|^22\w*|^23\w*|^24\w*|^25\w*|^26\w*|^27\w*|^28\w*|^29\w*|^30\w*|^31\w*|^32\w*|^33\w*)" , 
                    'D': "(^35\w*)" , 
                    'E': "(^36\w*|^37\w*|^38\w*|^39\w*)",
                    'F': "(^41\w*|^42\w*|^43\w*)" , 
                    'G': "(^45\w*|^46\w*|^47\w*)'",
                    'H': "(^49\w*|^50\w*|^51\w*|^52\w*|^53\w*)" , 
                    'I': "(^55\w*|^56\w*)" , 
                    'J': "(^58\w*|^59\w*|^60\w*|^61\w*|^62\w*|^63\w*)" , 
                    'K': "(^64\w*|^65\w*|^66\w*)" , 
                    'L': "(^68\w*)" , 
                    'M': "(^69\w*|^70\w*|^71\w*|^72\w*|^73\w*|^74\w*|^75\w*)" , 
                    'N': "(^77\w*|^78\w*|^79\w*|^80\w*|^81\w*|^82\w*)" , 
                    'O': "(^84\w*)" , 
                    'P': "(^85\w*)" , 
                    'Q': "(^86\w*|^87\w*|^88\w*)",
                    'R': "(^90\w*|^91\w*|^92\w*|^93\w*)" , 
                    'S': "(^94\w*|^95\w*|^96\w*)" , 
                    'T': "(^97\w*)" , 
                    'U': "(^99\w*)" } 
        size = []
        valores = {}
        nome =str(f).replace('.csv','')
        print(nome)

        valores['Município'] = nome
        valores['data'] = urls[0]
        
        for i in cnae_setor:
                    
            dict1 = {}
            dict2 = {}
            dict3 = {}
            lista=[]
            myquery = {}
            myquery['$regex'] = cnae_setor[i]
            dict1['CNAE_fiscal'] =  myquery 
            dict2['Município'] = nome
            dict3['Situação_cadastral'] = 'ativa'
            lista.append(dict1)
            lista.append(dict2)
            lista.append(dict3)
            global texto
            texto = {}
            texto['$and']= lista
        
            result = documents.find(texto)
            contagem = 0 
            for res in result:
                contagem += 1
            
            valores[str(i)]= contagem

        lista_por_estado.append(valores)
                    
        
        valores = {}
                
    grafico = db.empresas_ativas
    grafico.insert_many(lista_por_estado)
    lista_por_estado =[]

if __name__ == '__main__':
    criando_parametros()




        
    
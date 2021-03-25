import os

from helpers.separar_csv_por_cidade import separar_csv_por_cidade
from helpers.empresas_ativas import criando_parametros
from helpers.siteRF import SiteRF
from entidades import Data_Processor
from data import path, estados
from models.InterfaceDB import MongoDB
#main.py
"""Esse arquivo vai conter a camada superior da aplicação, onde serão ligadas
as peças da máquina
"""

#!---------------------------------------------------------------------------#
#!---------------------------------------------------------------------------#
#!--------------PROCESSO DE ATUALIZAÇÃO--------------------------------------#
#!---------------------------------------------------------------------------#
#!--------Caso exista:                                 ----------------------#
#!----------1 - Verificar a existência de um banco de dados------------------#
#!----------2 - Caso não exista , pular para o processo de criação de BD ----#
#!----------3 - Caso exista , continuar o processo de atualização -----------#
#!----------4 - Pegar a data da última atualização --------------------------#
#!----------5 - Conectar-se à receita federal e pegar a data de atualização--#
#!----------6 - Comparar as duas datas---------------------------------------#
#!----------7 - Se o banco de dados não tiver atualizado, continuar processo #
#!----------8 - Caso estiver, encerrar aqui----------------------------------#
#!---------------------------------------------------------------------------#
#!---------------------------------------------------------------------------#

#!---------------------------------------------------------------------------#
#!---------------------------------------------------------------------------#
#!------------PROCESSO DE CRIAÇÃO DE BANCO DE DADOS--------------------------#
#!---------------------------------------------------------------------------#
#!---------------------------------------------------------------------------#
#!----1 - Verificar a existência dos diretórios onde o trabalho irá ocorrer--#
#!----2 - Caso não existam, criá-los (Referências em DataClean)--------------#
#!----3 - Acessar o site da receita federal e pegar links para download -----#
#!----4 - Baixar todos os arquivos com multi-processing(4 em 4)--------------#
#!----                                                                       #
#!----***********************************************************************#
#!----*******************ATENÇÃO*********************************************#
#!----***********************************************************************#
#!----**    Nesse ponto preciso pensar com cuidado com estruturarei essa   **#
#!----** parte, pois, caso queira implementar esse processo de criação do  **#
#!----** banco de dados em um servidor, seria interessante baixar um ar-   **#
#!----** por vez e tratá-los individualmente                               **#
#!----***********************************************************************#
#!---------------------------------------------------------------------------#
#!----5 - Processar arquivos baixados-resultado:3 CSV                        #
#!----6 - Separar empresas.csv por cidade------------------------------------#
#!----7 - Tratar as informações de cada cidade-------------------------------#
#!----8 - Tratar socios.csv--------------------------------------------------#
#!----9 - Estabelecer conexão com banco de dados-----------------------------#
#!----10- Caso seja Mysql/postgres/sqlite, criar tabelas---------------------#
#!----11- Caso seja MongoDB, criar coleções(empresas,cnaes_secundarios)------#
#!----                                                                       #
#!----***********************************************************************#
#!----***********************ATENÇÃO*****************************************#
#!----***********************************************************************#
#!----**    Nesse momento é importante eu considerar a possibilidade de já **#
#!----** existir uma banco de dados desatualizado.Como farei a substituição,*#
#!----** faria apenas um update? Ou faria a inserção de todos os dados pra **#
#!----** depois apagar os antigos, garantindo que o aplicativo de consulta **#
#!----** não quebraria caso houvesse algum problema na atualização?        **#
#!----***********************************************************************#
#!----***********************************************************************#
#!----                                                                       #
#!----12- Inserir dados referentes a empresas--------------------------------#
#!----13- Inserir dados referentes a sócios ---------------------------------#
#!----14- Inserir dados referentes a cnaes secundários-----------------------#
#!----15- Verificar se os dados foram inseridos ou atualizados---------------#
#!----16- Calcular a quantidade de empresas ativas por município ------------#
#!----17- Inserir dados referentes a empresas ativas no banco de dados-------#
#!----18- Remover todos arquivos baixados e gerados(zips,csv)----------------#
#!---------------------------------------------------------------------------#

#--Essa função vai retornar True se a data de atualização do bd for menor que da
#--Receita federal

#!-----------------------------------------------------------------------------
#!---Antes de conferir se tem uma atualização disponível, é necessário conferir
#!---se existe um banco de dados. Caso não exista, ele deve pular direto para
#!---a criação do banco de dados
#!-----------------------------------------------------------------------------

def check_for_update():
    update_date_BD = MongoDB.checkUpdateDate()
    update_date_RF = SiteRF.check_RF_update_date()
    print(update_date_RF)
    print(update_date_BD)
    return update_date_RF > update_date_BD
    
def create_directories():

    if not os.path.exists(path.data_path):
        os.mkdir(path.data_path)
    if not os.path.exists(path.zip_path):
        os.mkdir(path.zip_path)
    return True

def init_update():
    urls = SiteRF().search_download_urls()
    for url in urls:
        init_process.download_file_wget(url)
        print('Starting cleaning data ')
        Data_Processor.process_data_in_chunks()
        os.remove(path.zip_path + '/file.zip')
        separar_csv_por_cidade()
        #Inserir socios
        #inserir cnaes_secundarios
        #Deletar CSVs
    separar_csv_por_cidade()

    #Aqui devo iterar por todos os arquivos da pasta CSVs e jogar pro banco de dados
    for uf in estados.estados:              
        dir = str(path.csv_path) + '/' + uf
        os.chdir(dir)        
        for f in glob.glob('*.*'):
            try:
                #bar.next()
                InterfaceDB().insert_many(file,'empresas')
                
            except:
                pass
    # #CRIAR ÍNDICES

    def cnaesecundario_to_mongoDB():
        """Essa função insere o arquivo cnae_secundarios.csv no banco de dados mongoDB."""
        db = config.connexion_mongo()
        if 'cnaesecundario' in db.list_collection_names():
            db.cnaesecundario.rename('cnaesecundario2')
        print('Insert CNAEs secundários ')
        cnae_dir = str(diretorio_cnaessecundarios) + 'cnaes_secundarios.csv' 
        InterfaceDB().insert_many(cnae_dir,'cnae_secundario')
    
        db.cnaesecundario2.drop()

    def socios_to_mongoDB():
    """Essa função insere os dados do arquivo socios.csv no banco de dados MongoDB
    """   
        db = config.connexion_mongo()     
        if 'socios' in db.list_collection_names():
            if 'socios2' in db.list_collection_names():
                result = db.socios.find({},{'_id':0})
                lista = []
                for i in result :
                    lista.append(i)
                print('Transferindo dados para outra coleção')
                db.socios3.insert_many(lista)
                db.socios.drop()
                db.socios.rename('socios3')
            else:
                db.socios.rename('socios3') 
        
        print('Insert sócios ')
        os.chdir(str(file_dir) + 'socios/')
        for file in glob.glob('*.*'):
            InterfaceDB().insert_many(file,'socios')
        db.socios2.drop()

    criando_parametros()

if __name__ == '__main__':
    # if check_for_update():
    #     create_directories()
    #     init_update()
    init_update()
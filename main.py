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
#!----


def mudando_diretorio_cnpj_clean(dir):
    """ Essa função percorre os diretórios contendo arquivos CSV
    e chama treating_csv()."""
    
    estados = ['SP']  
    'Iniciando tratamento de tabelas.'    
    for uf in estados:
        print('-----------------------------------------------------')
        print('------------------------Estado: {}-------------------'.format(uf))
        print('-----------------------------------------------------')
        diretorio_base = dir + '/' + uf
        os.chdir(diretorio_base)
        treating_csv()
        os.chdir('../../../')
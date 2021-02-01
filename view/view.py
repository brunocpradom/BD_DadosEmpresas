import pymongo
from models.models import BancoDeDadosMongoDB


class BuscadorPorCnpj(BancoDeDadosMongoDB):
    
    def __init__(self,cnpj):
        """A função construtora contém a conexão com o banco de dados e o cnpj
        a ser pesquisado
        """
        super().__init__()
        self.cnpj = cnpj
    
    def buscar_empresa_por(cnpj):
        """Essa função estabelece uma conexão com o banco de dados, recebe
        uma string contendo o CNPJ a ser pesquisado e retorna os resultados.
        """
        texto ={}
        texto['CNPJ'] = str(cnpj)
        
        result = self.empresas.find(texto,{'_id':0})
        
        results=[]
        for i in result:
            results.append(i)
        
        
        return results

class Cnae():
    def __init__(self, cnae, municipio, situacao_cadastral, codigo_do_setor ='',estado = ''):
        self.cnae = cnae
        self.municipio = municipio
        self.situacao_cadastral = situacao_cadastral
        self.codigo_do_setor = codigo_do_setor
        self.estado = estado
    
    def busca_cnae():
        empresas = conexão_BD()
        
        cnae = request.form['cnae']
        municipio = request.form['municipio']
        sit_cad = request.form['sit_cad']
        
        texto_busca = {}
        texto_busca['CNAE_fiscal'] = cnae
        texto_busca['Município'] = municipio
        texto_busca['Situação_cadastral'] = sit_cad
                
        result = empresas.find(texto_busca,{'_id':0})
        
        results=[]
        for i in result:
            results.append(i)
        
        return results

class CnaeHelp():
    def __init__(self,cnae,atividade_economica =''):
        self.cnae = cnae
        self.atividade_economica = atividade_economica
    
    def busca_atividade_econ(self):
        
        texto = {}
        
        cnae_busca = request.form['atividade_econ']
        cnae_regex ={}
        cnae_regex['$regex'] = cnae_busca
        texto['CNAE_sig'] = cnae_regex
        
        cnae_sig_col = db.cnae_legenda
        
        result = cnae_sig_col.find(texto,{'_id':0})
        print(texto)
        datas = []
        data_table = []
        final_result =[]
        
            
        for i in result :
            datas.append(i)
            
        cont = len(datas)
        
        for n in range(cont):
            for i in datas[n]:
                data_table.append(datas[n][i])
    
            tup = tuple(data_table)
            print(tup)
            final_result.append(tup)
            data_table = []
            
        cnae_table = final_result
        print(cnae_table)
        
        self.data_table_CNAE = MDDataTable(            
            size_hint =(0.8, 0.6),
            use_pagination = True,
            column_data =[
                        ("CNAE",dp(40)),
                        ("Significado", dp(100))],
            row_data = cnae_table)
        return self.data_table_CNAE.open()
    

class Municipio():
    def __init__(self, municipio): 
        self.municipio = municipio
        self.situacao_cadastral = situacao_cadastral

class Socios():
    def __init__(self, cnpj, socio):
        self.cnpj = cnpj
        self.socio = socio
        



def situacao_cadastral(self,texto,sit):
    """Essa função seria uma função pra ajudar a definir a frase
    de busca (texto_busca) em relação à situação cadastral. Todas
    as buscas que tem a opção de escolher a função cadastral tem
    três opções. Ativa, todas e outras(inativas,inaptas,baixadas,
    ...). Ela recebe uma das 3 opções e retorna um dicionário,
    ao qual deverão ser adicionados os outros campos para pesquisa.
    """
    
    if sit.lower() == '':
        return texto
    if sit.lower() == 'todas':
        return texto
    if sit.lower() == 'ativa':
        texto['Situação_cadastral'] = sit.lower()
        return texto
    if sit.lower() == 'outros':
        outros = {}
        lista_parametros = []
        outros['Situação_cadastral']= 'baixada'
        lista_parametros.append(outros)
        outros = {}
        outros['Situação_cadastral']= 'suspensa'
        lista_parametros.append(outros)
        outros = {}
        outros['Situação_cadastral']= 'inapta'
        lista_parametros.append(outros)
        outros = {}
        outros['Situação_cadastral'] = 'nula'

        texto['$or'] = lista_parametros
        return texto
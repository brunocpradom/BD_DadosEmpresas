"""Esse arquivo apenas referencia as tabelas do banco de dados. As relações
que serão feitas com o banco será especificada em /views/empresas.py
"""
from sqlalchemy import Column, Integer, String

from database import Base


class Empresas(Base):
    """Definição de classe/tabela das empresas e seus dados"""
    __tablename__ = 'empresas'

    cnpj = Column(String(50),primary_key=True)
    id = Column(String(50))
    nome_empresarial =Column(String(300))
    nome_Fantasia = Column( String(200))
    situacao_cadastral =Column(String(40))
    data_da_situacao_cadastral = Column(String(50))
    motivo_cadastral = Column(String(100))
    cod_natureza_juridica = Column(String(100))
    data_inicio_ativ = Column(String(50))
    cnae_fiscal = Column(String(50), index = True )
    tipo_de_logradouro = Column(String(200))
    logradouro = Column(String(300))
    numero = Column(String(100))
    complemento = Column(String(200))
    bairro = Column(String(100))
    cep = Column(String(100))
    uf = Column(String(90),index = True)
    cod_municipio = Column(String(90))
    municipio = Column(String(200),index = True)
    ddd = Column(String(50))
    telefone = Column(String(50))
    ddd2 =Column(String(50))
    telefone2 = Column(String(50))
    ddd3 = Column(String(50))
    fax = Column(String(50))
    e_mail = Column(String(200))
    qualif_do_responsavel = Column(String(50))
    capital_social = Column(String(50))
    porte = Column(String(50))
    opcao_pelo_simples = Column(String(40))
    data_opcao_pelo_simples = Column(String(50))
    data_exclusao_do_simples = Column(String(50))
    opcao_pelo_MEI = Column(String(50))
    situacao_especial = Column(String(100))
    data_situacao_especial = Column(String(50))

    def __repr__(self):
        return "{'cnpj':{},'id':{}}".format(self.cnpj,self.id)


class Socios(Base):
    """ Classe que defini a tabela que conterá informações sobre
    os sócios de empresas de todo Brasil.
    """
    __tablename__ = 'socios'

    id = Column(Integer, primary_key=True)
    cnpj = Column(String(50))
    tipo_socio = Column(String(100))
    nome_socio = Column(String(200))
    cnpj_cpf_socio = Column(String(50))
    cod_qualificacao = Column(String(100))
    perc_capital = Column(String(100))
    data_entrada = Column(String(100))
    cod_pais_ext = Column(String(100))
    nome_pais_ext = Column(String(100))
    cpf_repres = Column(String(100))
    nome_repres = Column(String(100))
    cod_qualif_repres = Column(String(50))

class CnaeSecundario(Base):
    __tablename__ = 'cnaes_secundarios'

    id= Column(Integer, primary_key=True)
    cnpj  = Column(String(50))
    cnae_ordem = Column(String(50))
    cnae = Column(String(200))


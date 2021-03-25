from sqlalchemy import Column, Integer, String

from database import Base


class Config(Base):
    """Classe que defini a tabela que conterá informações como data da
    última atualização da tabela, data_de_criação do banco de dados,
    quantas vezes ele já foi atualizado."""

    __tablename__ = 'config'

    id = Column(Integer, primary_key=True)
    data_atualizacao = Column(String(40))
    data_de_criacao_banco_de_dados = Column(String(40))
    num_atualizacoes = Column(String(30))

    def __init__(self,data_atualizacao, data_de_criacao_banco_de_dados, num_atualizacoes):
        self.data_atualizacao = data_atualizacao
        self. data_de_criacao_banco_de_dados = data_de_criacao_banco_de_dados
        self.num_atualizacoes = num_atualizacoes

    def __repr__(self):
        return "<Config(data_atualizacao = '%s')>" % self.data_atualizacao
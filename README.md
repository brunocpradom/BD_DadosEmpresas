#------------------------------------#
#------------READ ME-----------------#
#------------------------------------#
# Essa versão baixa os arquivos da --#
# receita federal,trata-os com pandas#
# e joga as informações no banco de -#
# dados.                             #
# -Ponto negativo. O banco de dados  #
# acaba ficando desorganizado. As em-#
# presas da mesma cidade não ficam   #
# próximas, tornando as operações que#
# levam em conta as empresas do mesmo#
# mais lentas.                       #
# - Ponto positivo: Não são gerados  #
# arquivos CSVs, ocupando menos espa-#
# ço no servidor. Mesmo que os arqui-#
# sejam apagados depois, são 12GB que#
# ocuparão por umas 4h. Para servi - #
# dores com tamanho limitados esse é #
# um tamanho considerável            #
#------------------------------------#

sudo apt-get install libmysqlclient-dev
pip install mysqlclient
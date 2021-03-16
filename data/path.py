import os
import glob

rootPath = os.getcwd()
data_path = rootPath + '/dados'
zip_path = rootPath + '/zips'
csvDirPath = data_path + '/UFs'
socios_dir = data_path + '/socios'
#---Lista contendo todos arquivos .ZIP
input_list = glob.glob(os.path.join(zip_path,'*.zip'))

import os
import glob

rootPath = os.getcwd()
dataPath = rootPath + '/dados'
zipPath = rootPath + '/zips'
csvDirPath = dataPath + '/UFs'
socios_dir = dataPath + '/socios'
#---Lista contendo todos arquivos .ZIP 
input_list = glob.glob(os.path.join(zipPath,'*.zip'))
import os

rootPath = os.getcwd()
dataPath = rootPath + '/dados'
zipPath = rootPath + '/zips'
csvDirPath = dataPath + '/UFs'
#---Lista contendo todos arquivos .ZIP 
input_list = glob.glob(os.path.join(zipPath,'*.zip'))
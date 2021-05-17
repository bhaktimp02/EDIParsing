from scripts.parsingv1 import dataParsing
import pandas as pd
from scripts.dbConnection import dbInsert
import os,shutil


def main(inputpath):
    print(inputpath)
    #finaldf = pd.DataFrame()
    for file in os.listdir(r"{}".format(inputpath)):
        print(file)
        resultdf = dataParsing(file, fpath='{}'.format(inputpath))
        dbInsert(resultdf)





dir_path = os.path.dirname(os.path.realpath(__file__))
def main(inputpath):
    print(inputpath)
    finaldf = pd.DataFrame()
    for file in os.listdir(r"{}".format(inputpath)):
        print(file)
        try:
            resultdf = dataParsing(file, fpath='{}'.format(inputpath))
            #resultdf.to_excel(dir_path+'\\lib\\output\\{}.xlsx'.format(file.replace('.','_')))
            shutil.copy(inputpath+"\{}".format(file), dir_path + '\lib\processed\{}'.format(file))
            finaldf = finaldf.append(resultdf)
            dbInsert(resultdf, file)
        except:
            shutil.copy(inputpath+"\{}".format(file), dir_path + '\lib\\failed\{}'.format(file))
        #resultdf.to_excel(dir_path + '\\lib\\output\\InboundTestFiles\\{}.xlsx'.format(file.replace('.', '_')))
        #finaldf=finaldf.append(resultdf)
        #finaldf=resultdf
    finaldf.to_excel(dir_path + '\lib\output\allfiles15.xlsx')
    dbInsert(resultdf, file)




if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if not os.path.exists(dir_path + '\lib\failed'):
        os.makedirs(dir_path + '\lib\failed')
    if not os.path.exists(dir_path + '\lib\processed'):
        os.makedirs(dir_path + '\lib\processed')
    inputpath = dir_path + '\lib\input'
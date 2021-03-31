from scripts.parsingv1 import dataParsing
import pandas as pd

import os


def main(inputpath):
    print(inputpath)
    #finaldf = pd.DataFrame()
    for file in os.listdir(r"{}".format(inputpath)):
        print(file)
        resultdf = dataParsing(file, fpath='{}'.format(inputpath))


if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    inputpath=dir_path+'\lib\input'
    print(inputpath)
    main(inputpath)
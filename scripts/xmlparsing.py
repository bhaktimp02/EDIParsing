import sys
import pandas as pd
import pdb as bp
from datetime import datetime
import datetime
import xml.etree.ElementTree as ET
from configs.constantsValue import colsumLists
import copy
from lxml import etree


class var():
    df= pd.DataFrame(columns=list(colsumLists()))
    repValue=0



def fileParsingStratergy(fpath,filename):
    data = []
    #df = pd.DataFrame()
    print(var.df)
    print(var.df.columns)
    print('{}/{}'.format(fpath, filename))
    print(data)

    try:
        f = open('{}/{}'.format(fpath, filename), 'r', encoding="utf8")

        texts = f.readlines()
    except:
        with open('{}/{}'.format(fpath, filename), 'r+') as reader:
            my_data = reader.read()
            # print(my_data.encode())
            print(my_data.encode()[3:4])
            my_data = my_data.encode().replace(my_data.encode()[3:4], b'*')
            my_data = my_data.decode()
            reader.seek(0)
            reader.write(my_data)
            reader.truncate()
        f = open('{}/{}'.format(fpath, filename), 'r')

        texts = f.readlines()

    return texts
def outboundParsing(filename,fpath):
    """
    inputpath: file path for directories.
    :param
    filename: Iterating file available in folder

    :return:
    """
    from bs4 import BeautifulSoup
    allcapture=[]
    if filename.endswith('.xml'):
        # parser = etree.XMLParser(recover=True)
        # etree.fromstring(filename, parser=parser)
        print('Its txt extension file {}'.format(filename))
        with open('{}/{}'.format(fpath,filename)) as fp:
            soup = BeautifulSoup(fp, 'xml')

        idoc=soup.find('IDOC',{"BEGIN":"1"})
        children=idoc.findChildren("QUALF",recursive=False)
        for child in idoc:
            #print(child)
            e1edk2=soup.find_all('E1EDK02',{"SEGMENT":"1"})
            output = e1edk2['value']
            print(output)
            bp()
            for ch in e1edk2:
                print(ch)
                ponum=soup.find_all()
                print(ch.get_text())
                bp()
            SEGMENT = "1"
        qualf=soup.find_all('QUALF')

        # for val in qualf:
        #     print(val.get_text())
        #     bp()

        #print(soup)
        bp()
        parser = ET.XMLParser(encoding="utf-8")
        tree = ET.fromstring(filename, parser=parser)
        print(tree)
        #root = ET.parse('{}/{}'.format(fpath,filename)).getroot()
        for type_tag in root.findall('-AFS_-ORDERS05'):
            value = type_tag.get('BEGIN="1"')
            print(value)
        bp()
    elif filename.startswith(('POOH','POOJ','POOK','POOM')):
        startNum=33
        endNum=43
        ValStartwith='H1'
        data1 = {'TRANSACTION_TYPE': 'PO', 'TRANSACTION_ID': 850,
                 "RECV_FILENAME": filename}
        TRANSACTION_TYPE='PO'
        TRANSACTION_ID=850

        texts=fileParsingStratergy(fpath,filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['PURCHASE_ORDER_NUMBER']=line[startNum:endNum]

                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('JCPPRO') or 'JCPPRO' in filename:
        data1 = {'TRANSACTION_TYPE': 'PR', 'TRANSACTION_ID': 855,
                 "RECV_FILENAME": filename}
        texts=fileParsingStratergy(fpath,filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['PURCHASE_ORDER_NUMBER']=line[21:45]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('POOR') or 'POOR' in filename:
        data1 = {'TRANSACTION_TYPE': 'PO', 'TRANSACTION_ID': 850,
                 "RECV_FILENAME": filename}
        texts=fileParsingStratergy(fpath,filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['PURCHASE_ORDER_NUMBER']=line[24:49]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('POOM') or 'POOM' in filename:
        data1 = {'TRANSACTION_TYPE': 'PO', 'TRANSACTION_ID': 850,
                 "RECV_FILENAME": filename}
        texts=fileParsingStratergy(fpath,filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['PURCHASE_ORDER_NUMBER']=line[33:43]
                allcapture.append(copy.deepcopy(data1))
    elif filename.startswith(('SCOA')):
        startNum = 78
        endNum = 88
        ValStartwith = 'H1'
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 832,
                 "RECV_FILENAME": filename}
        texts=fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['PURCHASE_ORDER_NUMBER']=line[startNum:endNum]

                allcapture.append(copy.deepcopy(data1))
    elif 'MIO' in filename:
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 213,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:

            if line.startswith('D1'):
                data1['REF_NAME1'] = 'D1_REF_NUM'
                data1['REF_VAL1'] = line[5:30]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('INOE')) or 'INOE' in filename:
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 810,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('00'):
                data1['REF_NAME1'] = 'NotaFiscal_Nbr'
                data1['REF_VAL1'] = line[76:85]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('INO')) or 'INO' in filename:
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 810,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:

            if line.startswith('H1'):
                data1['INVOICE_NUMBER'] = line[5:30]

                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('INOC')) or 'INOC' in filename:
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 810,
                 "RECV_FILENAME": filename}
        #texts = fileParsingStratergy(fpath, filename)
        allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('RFO')) or 'RFO' in filename:
        data1 = {'TRANSACTION_TYPE': 'RF', 'TRANSACTION_ID': 753,
                 "RECV_FILENAME": filename}
        #texts = fileParsingStratergy(fpath, filename)
        allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('SHOG')) or 'SHOG' in filename:
        data1 = {'TRANSACTION_TYPE': 'SH', 'TRANSACTION_ID': 856,
                 "RECV_FILENAME": filename}
        # texts = fileParsingStratergy(fpath, filename)
        allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('IBO') or 'IBO' in filename:
        data1 = {'TRANSACTION_TYPE': 'IB', 'TRANSACTION_ID': 846,
                 "RECV_FILENAME": filename}
        # texts = fileParsingStratergy(fpath, filename)
        allcapture.append(copy.deepcopy(data1))


    elif filename.startswith(('AGO','AGOT')) or 'AGO' in filename or 'AGOT' in filename:
        data1 = {'TRANSACTION_TYPE': 'AG', 'TRANSACTION_ID': 824,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            if line.startswith('H1'):
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('SCO','SCOL','SCOE')) or 'SCO' in filename or 'SCOL' in filename or 'SCOE' in filename:
        data1 = {'TRANSACTION_TYPE': 'IN', 'TRANSACTION_ID': 832,
                 "RECV_FILENAME": filename}
        #texts = fileParsingStratergy(fpath, filename)
        allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('UPO')) or 'UPO' in filename:
        data1 = {'TRANSACTION_TYPE': '', 'TRANSACTION_ID': 215,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
           if line.startswith('H1'):
                data1['REF_NAME1'] = 'h1_pickup_nbr'
                data1['REF_VAL1'] = line[24:34]

                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('SHO')) or 'SHO' in filename:
        data1 = {'TRANSACTION_TYPE': 'SH', 'TRANSACTION_ID': 856,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
           if line.startswith('H1'):
                data1['REF_NAME1'] = 's1_printed_BOL_nbr'
                data1['REF_VAL1'] = line[335:355]

                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('RAO')) or 'RAO' in filename:
        data1 = {'TRANSACTION_TYPE': 'RA', 'TRANSACTION_ID': 820,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('H1'):
                data1['REF_NAME1'] = 'H1_EGATE_REF_NUMBER'
                data1['REF_VAL1'] = line[40:75]
                print('--------------------------------------')
                print(line[40:75])
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith(('SHOJ','SHOK')) or 'SHOJ' in filename:
        data1 = {'TRANSACTION_TYPE': 'SH', 'TRANSACTION_ID': 856,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('10'):
                data1['REF_NAME1'] = 'HEAD_BOL'
                data1['REF_VAL1'] = line[58:78]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('SHON') or 'SHON' in filename:
        data1 = {'TRANSACTION_TYPE': 'SH', 'TRANSACTION_ID': 856,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('10'):
                data1['REF_NAME1'] = 'NIKE_SHIP_ID'
                data1['REF_VAL1'] = line[39:49]
                allcapture.append(copy.deepcopy(data1))

    elif filename.startswith('AGOJ') or 'AGOJ' in filename:
        data1 = {'TRANSACTION_TYPE': 'AG', 'TRANSACTION_ID': 824,
                 "RECV_FILENAME": filename}
        texts = fileParsingStratergy(fpath, filename)
        for line in texts:
            print(line)
            if line.startswith('10'):
                data1['REF_NAME1'] = 'h1_bill_reference'
                data1['REF_VAL1'] = line[19:49]
                allcapture.append(copy.deepcopy(data1))
        #data=fileParsingStratergy(fpath, filename, ValStartwith, startNum, endNum,data1,allcapture)



    df = pd.DataFrame(allcapture, columns=list(colsumLists()))
    df['CREATE_DATE_TIME'] = pd.datetime.now()
    df['ASN_DATE'] = pd.to_datetime(df['ASN_DATE'])
    df['INVOICE_DATE'] = pd.to_datetime(df['INVOICE_DATE'])
    df['LAST_UPDATE_DATE_TIME'] = pd.to_datetime(df['LAST_UPDATE_DATE_TIME'])
    df['PO_DATE'] = pd.to_datetime(df['PO_DATE'])
    # df['PO_AMOUNT']=df['PO_AMOUNT'].astype(int)
    # df['INVOICE_AMOUNT']=df['INVOICE_AMOUNT'].astype(int)
    df["PARTNER_NAME"] = df["PARTNER_NAME"].astype(str)
    df["DIRECTION"] = df["DIRECTION"].astype(str)
    df["INTERCHANGE_SENDER_ID"] = df["INTERCHANGE_SENDER_ID"].astype(str)
    df["INTERCHANGE_RECEIVER_ID"] = df["INTERCHANGE_RECEIVER_ID"].astype(str)
    df["INTERCHANGE_CONTROL_NUMBER"] = df["INTERCHANGE_CONTROL_NUMBER"].astype(str)
    df["GROUP_SENDER_ID"] = df["GROUP_SENDER_ID"].astype(str)
    df["GROUP_RECIEVER_ID"] = df["GROUP_RECIEVER_ID"].astype(str)
    df["GROUP_CONTROL_NUMBER"] = df["GROUP_CONTROL_NUMBER"].astype(str)
    df["TRANSACTION_CONTROL_NUMBER"] = df["TRANSACTION_CONTROL_NUMBER"].astype(str)
    df["TRANSACTION_TYPE"] = df["TRANSACTION_TYPE"].astype(str)
    df["TRANSACTION_ID"] = df["TRANSACTION_ID"].astype(str)
    df["TRANSACTION_STATUS"] = df["TRANSACTION_STATUS"].astype(str)
    df["RECV_FILENAME"] = df["RECV_FILENAME"].astype(str)
    df["RECV_FILE_ARCHIVE_PATH"] = df["RECV_FILE_ARCHIVE_PATH"].astype(str)
    df["SEND_FILENAME"] = df["SEND_FILENAME"].astype(str)
    df["SEND_FILE_ARCHIVE_PATH"] = df["SEND_FILE_ARCHIVE_PATH"].astype(str)
    df["APPLICATIONDOC_NUM"] = df["APPLICATIONDOC_NUM"].astype(str)
    print(df)
    return df

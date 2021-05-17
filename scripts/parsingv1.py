import pandas as pd
import copy
from configs.constantsValue import colsumLists
from scripts.parsingRecursing import __isa


resetDict={}
allcapture = []
colList = colsumLists()
def dataParsing(filename, fpath):
    data = []
    print('{}/{}'.format(fpath, filename))
    print(data)

    try:
        f = open('{}/{}'.format(fpath, filename), 'r', encoding="utf8")

        texts = f.readlines()
    except:
        with open('{}/{}'.format(fpath, filename),'r+') as reader:
            my_data = reader.read()
            #print(my_data.encode())
            print(my_data.encode()[3:4])
            my_data=my_data.encode().replace(my_data.encode()[3:4],b'*')
            my_data=my_data.decode()
            reader.seek(0)
            reader.write(my_data)
            reader.truncate()
        f = open('{}/{}'.format(fpath, filename), 'r')

        texts = f.readlines()

    colList = colsumLists()
    listOflines = []


    for line in texts:
        if line.startswith("ISA"):
            elementDelimiter = line[3]
            segmentTerminator = line[105]
            print('elementdelimeter----{}'.format(elementDelimiter))


            dataSplit = line.split(elementDelimiter)


            data = {'Sender qualifiers': dataSplit[5].strip(),
                    'sender id': dataSplit[6].strip(),
                    'receiver qualifiers': dataSplit[7].strip(),
                    'receiver id': dataSplit[8].strip(),
                    'ASN_DATE': dataSplit[9].strip(),
                    'interchange time': dataSplit[10].strip(),
                    'ISA control number': dataSplit[10].strip()}
            data = {'Sender qualifiers': dataSplit[5].strip(),
                    'INTERCHANGE_SENDER_ID': dataSplit[6].strip(),
                    'receiver qualifiers': dataSplit[7].strip(),
                    'INTERCHANGE_RECEIVER_ID': dataSplit[8].strip(),
                    'Interchange date': dataSplit[9].strip(),
                    'interchange time': dataSplit[10].strip(),
                    "INTERCHANGE_CONTROL_NUMBER": dataSplit[13].strip(),
                    "RECV_FILENAME": filename,
                    "DIRECTION": 'INBOUND'}
            subementDelimeter=None
        if line.startswith("UNA") or line.startswith("UNB"):
            #print(line)
            if line.startswith("UNB"):
                elementDelimiter = line[3]
                segmentTerminator = line[72]
                subementDelimeter = line[8]
                #print(line[3],line[8],line[72])
            else:
                print('--------entering in UNA----')
                #dataR = parsingUNA(line, data)
                elementDelimiter = line[3]
                segmentTerminator = line[8]
                subementDelimeter = line[4]
            print(elementDelimiter,subementDelimeter,segmentTerminator)
            dataSplit = (line).split(elementDelimiter)
            # dataSplit = line.split(elementDelimiter)

            # print(dataSplit)
            print(segmentTerminator, subementDelimeter, elementDelimiter)

        allLines = line.split(segmentTerminator)

        listOflines.extend(allLines[:-1])


    return iterateLines(f,listOflines,segmentTerminator,subementDelimeter,elementDelimiter,filename,data)

def iterateLines(f,listOflines,segmentTerminator,subementDelimeter,elementDelimiter,filename,data):
    allcapture=[]

    if elementDelimiter is None:
        elementDelimiter=''
    else:
        elementDelimiter=elementDelimiter

    for value in range(0, len(listOflines)):
        #print(listOflines[value])
        linestrip=listOflines[value].strip(' ')
        if linestrip.startswith('ISA'):
            isaP=__isa(listOflines[value],filename)
            elementDelimiter=isaP[1]
            data=isaP[0]

        # ----------------- Group Parsing ---------------------------------#
        if listOflines[value].startswith('GS'):
            print('--------------inside GS-------------------------')
            #print(elementDelimiter)

            gsData = listOflines[value].split(elementDelimiter)
            print(gsData[2])
            #bp()
            #print(gsData)

            data["GROUP_SENDER_ID"] = gsData[2]
            data["GROUP_RECIEVER_ID"] = gsData[3]
            data["GROUP_CONTROL_NUMBER"] = gsData[6]
            data["TRANSACTION_TYPE"] = gsData[2]
            #allcapture.append(copy.deepcopy(data))

        # -------------------- ST * 810 Parsing----------------------------#
        if listOflines[value].startswith('ST{}810{}'.format(elementDelimiter,elementDelimiter)):
            data["TRANSACTION_TYPE"]='810'
            data["TRANSACTION_ID"]=810
            #allcapture = allcapture
            #print(listOflines[value])
            print('#---------------line 810------------------#')
            ind=0
            while True:
                if listOflines[value+ind].startswith('BIG'):
                    dataformat = listOflines[value+ind].split(elementDelimiter)
                    #print(dataformat)
                    data['PURCHASE_ORDER_NUMBER'] = dataformat[4] if len(dataformat)==4 else None
                    data['PO_DATE'] = dataformat[3] if len(dataformat)==4 else None
                    data["INVOICE_DATE"] = dataformat[1]
                    data["INVOICE_NUMBER"] = dataformat[2]
                if listOflines[value+ind].startswith('TDS'):
                    print(listOflines[value+ind])
                    dataformat = listOflines[value+ind].split(elementDelimiter)
                    data["INVOICE_AMOUNT"] = int(dataformat[1])/100
                    #allcapture.append(copy.deepcopy(data))
                    break
                else:
                    ind=ind+1
            # print(data2)
            allcapture.append(copy.deepcopy(data))
        # -------------------- Ends ST * 810 Parsing----------------------------#

        # -------------------- ST * 870 Parsing----------------------------#
        if listOflines[value].startswith('ST{}870{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            print(value+1)
            print(listOflines[value+1])
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1
            data['REF_NAME1'] = ''
            data['REF_VALUE1'] = ''

            ind = 1
            print(value)
            while True:
                # print('inside while true 870')
                if listOflines[value+ind].startswith('BSR'):
                    data['REF_NAME1'] = "Document Reference"
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[3]
                    #allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind = ind + 1

        # -------------------- Ends ST * 870 Parsing----------------------------#

        if listOflines[value].startswith('ST{}820{}'.format(elementDelimiter,elementDelimiter)):
            data["TRANSACTION_TYPE"]='820'
            data["TRANSACTION_ID"]=820
            #allcapture = allcapture
            #print(listOflines[value])
            print('#---------------line 820------------------#')
            ind=0
            while True:
                if listOflines[value+ind].startswith('BPR'):
                    dataformat = listOflines[value+ind].split(elementDelimiter)
                    #print(dataformat)
                    data['REF_NAME1'] = "Remittance Amount"
                    data['REF_VALUE1'] = dataformat[2]
                if listOflines[value+ind].startswith('TRN'):
                    print(listOflines[value+ind])
                    data['REF_NAME2'] = "Remintance Trace ID"
                    data['REF_VALUE2'] = dataformat[2]
                    #allcapture.append(copy.deepcopy(data))
                    break
                else:
                    ind=ind+1
            # print(data2)
            allcapture.append(copy.deepcopy(data))
        # -------------------- ST * 812 Parsing----------------------------#
        if listOflines[value].startswith('ST{}812{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            ind=1
            data['REF_NAME1'] = ''
            data['REF_VALUE1'] = ''
            while True:
                print('inside while true 947')
                if listOflines[value+ind].startswith('BCD{}'.format(elementDelimiter)):
                    data['REF_NAME1'] = "Document Reference"
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[1]
                    #allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind=ind+1
            # if listOflines[value].startswith('BCD'):
            #     data['REF_NAME1']="Document Reference"
            #     data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[1]
            #bp()
            print('#---------------line 812------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            #print(dataformat)

            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # -------------------- Ends ST * 812 Parsing----------------------------#

        # -------------------- ST * 846 Parsing----------------------------#
        if listOflines[value].startswith('ST{}846{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            data['REF_NAME1'] = ''
            data['REF_VALUE1'] = ''
            ind=1
            while True:
                print('inside while true 947')
                if listOflines[value+ind].startswith('BIA{}'.format(elementDelimiter)):
                    data['REF_NAME1'] = "Document Reference"
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[3]
                    #allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind=ind+1

            # if listOflines[value].startswith('BIA'):
            #     data['REF_NAME1']="Document Reference"
            #     data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[3]
            #bp()
            print('#---------------line 846------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            #print(dataformat)

            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_TYPE"]= data["TRANSACTION_ID"]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # -------------------- Ends ST * 846 Parsing----------------------------#


        # ------------------------ ST * 861 --------------------------#
        if listOflines[value].startswith('ST{}861{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            data["TRANSACTION_ID"] = '861'
            data["TRANSACTION_TYPE"] = 861
            ind=1
            data['REF_NAME1'] = ''
            data['REF_VALUE1'] = ''
            while True:
                print('inside while true 947')
                if listOflines[value+ind].startswith('BRA{}'.format(elementDelimiter)):
                    data['REF_NAME1'] = "Document Reference"
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[1]
                    allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind=ind+1
            # if listOflines[value].startswith('BRA'):
            #     data['REF_NAME1']="Document Reference"
            #     data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[1]
            #bp()
            print('#---------------line 861------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            #print(dataformat)

            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # ------------------------ End ST * 861 --------------------------#

        # --------------------  ST * 852 Parsing----------------------------#
        if listOflines[value].startswith('ST{}852{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            print('#---------------line 852------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            #print(dataformat)

            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # -------------------- Ends ST * 852 Parsing----------------------------#

        # ------------------------  ST * 850 --------------------------#
        if listOflines[value].startswith('ST{}850{}'.format(elementDelimiter,elementDelimiter)):
            #print(listOflines[value])
            #bp()
            print('#---------------line 850------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            #print(dataformat)

            data['PURCHASE_ORDER_NUMBER'] = dataformat[3]
            data['PO_DATE'] = dataformat[5]
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            #data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_TYPE"]=data["TRANSACTION_ID"]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # -------------------- Ends ST * 850 Parsing----------------------------#

        # ------------------------  ST * 860 --------------------------#
        if listOflines[value].startswith('ST{}860{}'.format(elementDelimiter,elementDelimiter)):
            #print(listOflines[value])
            print('#---------------line 860------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            print(dataformat)

            # print(dataformat)
            data['PURCHASE_ORDER_NUMBER'] = dataformat[3]
            data['PO_DATE'] = dataformat[5]
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            # data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_TYPE"]= data["TRANSACTION_ID"]
            data['TRANSACTION_STATUS'] = 1
            allcapture.append(copy.deepcopy(data))
        # ------------------------  ST * 812 --------------------------#


        if listOflines[value].startswith('ST{}214{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            # bp()
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1
            data['REF_NAME1'] = ''
            data['REF_VALUE1'] = ''

            ind = 1
            while True:
                print('inside while true 214')
                if listOflines[value + ind].startswith('B10{}'.format(elementDelimiter)):
                    data['REF_NAME1'] = 'DOCUMENT REFERENCE'
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[2]
                    allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind = ind + 1


        # ------------------------  ST * 947 --------------------------#
        if listOflines[value].startswith('ST{}947{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            #bp()
            data["TRANSACTION_CONTROL_NUMBER"]=listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"] = '947'
            data["TRANSACTION_ID"] = 947
            data['TRANSACTION_STATUS'] = 1


            ind=1
            while True:
                print('inside while true 947')
                if listOflines[value+ind].startswith('W15{}'.format(elementDelimiter)):
                    data['REF_NAME1']='DOCUMENT REFERENCE'
                    data['REF_VALUE1'] = listOflines[value + 1].split(elementDelimiter)[2]
                    allcapture.append(copy.deepcopy(data))
                    break

                else:
                    ind=ind+1

    # ------------------------ End ST * 947 --------------------------#

        # ------------------------  ST * 856 --------------------------#

        if listOflines[value].startswith('ST{}856{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            #bp()
            data["TRANSACTION_CONTROL_NUMBER"]=listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"] = '856'
            data["TRANSACTION_ID"] = 856
            data['TRANSACTION_STATUS'] = 1
            ind=1
            while True:
                data["ASN_NUMBER"] = None
                data["ASN_DATE"] = None
                print('inside while true 856')
                if listOflines[value+ind].startswith('BSN'):
                    print(listOflines[value+ind].split(elementDelimiter))
                    data["ASN_NUMBER"]=listOflines[value+ind].split(elementDelimiter)[2]
                    data["ASN_DATE"]=listOflines[value+ind].split(elementDelimiter)[3]
                    #allcapture.append(copy.deepcopy(data))
                if listOflines[value+ind].startswith('HL') and listOflines[value].endswith('O'):

                    while True:
                        print('inside while 856 -2')
                        if listOflines[value+ind].startswith('PRF'):
                            print(value+ind,listOflines[value+ind], filename)
                            data["PURCHASE_ORDER_NUMBER"] = listOflines[value+ind].split(elementDelimiter)[1] if len(listOflines[value+ind])>=20 else None
                            data["PO_DATE"] = listOflines[value+ind].split(elementDelimiter)[4] if len(listOflines[value+ind])>=20 else None
                            #print(data)
                            allcapture.append(copy.deepcopy(data))
                            break
                        else:
                            ind=ind+1
                else:
                    allcapture.append(copy.deepcopy(data))
                    break
        # ------------------------ End ST * 856 --------------------------#


        # ------------------------  ST * 997 --------------------------#
        if listOflines[value].startswith('ST{}997{}'.format(elementDelimiter,elementDelimiter)):
            print(listOflines[value])
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"]='997'
            data["TRANSACTION_ID"]=997
            data['TRANSACTION_STATUS']=1
            data["ASN_NUMBER"] = ''
            data["ASN_DATE"] = ''

            ind = 0

            for val in range (value,len(listOflines)):
                print(listOflines[val])
                if listOflines[val].startswith('AK2'):

                    data["FA_STATUS"] = None
                    #allcapture.append(copy.deepcopy(data))
                    #print('---value: {}----{}'.format(val + ind, listOflines[val + ind]))
                    ind=1
                    while True:
                        if listOflines[val+ind].startswith('AK5'):
                            print('---value: {}----{}'.format(val + ind, listOflines[val + ind]))
                            data["FA_STATUS"] = listOflines[val + ind].split(elementDelimiter)[1]
                            #allcapture.append(copy.deepcopy(data))
                        if listOflines[val+ind].startswith('IEA'):
                            allcapture.append(copy.deepcopy(data))
                            break
                        else:
                            ind=ind+1



            print('line125--count---{}'.format(len(allcapture)))
            # ------------------------  ST * 997 --------------------------#
        # ------------------------EDI fAct UNB Parsing --------------->#
        if listOflines[value].startswith('UNB+'):
            print(listOflines[value].split(subementDelimeter))
            #print(listOflines[value].split(elementDelimiter))
            print(subementDelimeter,elementDelimiter,segmentTerminator)

            dataformat = listOflines[value].split(subementDelimeter)

            # print(dataformat)
            # print(data)
            #
            # print(dataformat[3].split(elementDelimiter)[1])

            data = {'INTERCHANGE_SENDER_ID': dataformat[2].split(elementDelimiter)[0],
                    'INTERCHANGE_RECEIVER_ID': dataformat[2].split(elementDelimiter)[1],
                    'Interchange date': dataformat[4].split(elementDelimiter)[0],
                    'interchange time': dataformat[4].split(elementDelimiter)[1],
                    "INTERCHANGE_CONTROL_NUMBER": dataformat[5],
                    "TRANSACTION_CONTROL_NUMBER": dataformat[4].split(elementDelimiter)[-1],
                    'TRANSACTION_STATUS': 1,
                    "DIRECTION": 'INBOUND', 'Sender qualifiers': dataformat[1].split(elementDelimiter)[1],
                    'receiver qualifiers': dataformat[3].split(elementDelimiter)[1]}
            #pprint.pprint(data)
            print(dataformat)
            data['INTERCHANGE_RECEIVER_ID']=dataformat[2].split(elementDelimiter)[1]
            # data['Interchange date'] = dataformat[3].split(subementDelimeter)[1]
            # data['interchange time'] = dataformat[4].split(subementDelimeter)[0]
            data["INTERCHANGE_CONTROL_NUMBER"] = dataformat[4].split(elementDelimiter)[-1]
            data["INTERCHANGE_CONTROL_NUMBER"] = dataformat[5]
            data["RECV_FILENAME"] = filename
            data['TRANSACTION_STATUS'] = 1
            data['TRANSACTION_CONTROL_NUMBER'] = dataformat[4].split(elementDelimiter)[-1]
            data["DIRECTION"] = 'INBOUND'

            #pprint.pprint(data)
            dataformat2 = listOflines[value + 1].split(elementDelimiter)
            print(dataformat2)
            data["TRANSACTION_TYPE"] = dataformat[2].split(elementDelimiter)[0] if len(dataformat2)>=24 else None
            data["TRANSACTION_ID"] = dataformat[2].split(elementDelimiter)[0] if len(dataformat2)>=24 else None
            #print(data)

            allcapture.append(copy.deepcopy(data))


    #print(len(allcapture))
    df = pd.DataFrame(allcapture, columns=colList)
    df['CREATE_DATE_TIME']=pd.datetime.now()
    df['ASN_DATE']=pd.to_datetime(df['ASN_DATE'])
    df['INVOICE_DATE']=pd.to_datetime(df['INVOICE_DATE'])
    df['LAST_UPDATE_DATE_TIME']=pd.to_datetime(df['LAST_UPDATE_DATE_TIME'])
    df['PO_DATE']=pd.to_datetime(df['PO_DATE'])
    #df['PO_AMOUNT']=df['PO_AMOUNT'].astype(int)
    #df['INVOICE_AMOUNT']=df['INVOICE_AMOUNT'].astype(int)
    df["PARTNER_NAME"]=df["PARTNER_NAME"].astype(str)
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

    print(df.head())
    print(df.index)


    f.close()
    return df




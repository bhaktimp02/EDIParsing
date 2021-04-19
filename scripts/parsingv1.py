import pandas as pd
import copy
from configs.constantsValue import colsumLists
import pprint
resetDict={}
allcapture = []
colList = colsumLists()
def dataParsing(filename, fpath):
    data = []
    print('{}\\{}'.format(fpath, filename))
    f = open('{}\\{}'.format(fpath, filename), 'r')
    texts = f.readlines()
    listOflines = []
    for line in texts:
        if line.startswith("ISA"):
            print(line)
            elementDelimiter = line[3]
            segmentTerminator = line[105]
            dataSplit = line.split(elementDelimiter)
            print(dataSplit)
            print(segmentTerminator)
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
                    "INTERCHANGE_CONTROL_NUMBER": dataSplit[10].strip(),
                    "RECV_FILENAME": filename,
                    "DIRECTION": 'INBOUND'}
            subementDelimeter=None
        if line.startswith("UNA") or line.startswith("UNB"):
            print(line)
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

    print(len(listOflines))
    return iterateLines(f,listOflines,segmentTerminator,subementDelimeter,elementDelimiter,filename,data)

def iterateLines(f,listOflines,segmentTerminator,subementDelimeter,elementDelimiter,filename,data):
    allcapture=[]

    for value in range(0, len(listOflines)):


        # ----------------- Group Parsing ---------------------------------#
        if listOflines[value].startswith('GS'):
            gsData = listOflines[value].split(elementDelimiter)
            # print(listOflines[value])
            # print(gsData)
            data["GROUP_SENDER_ID"] = gsData[2]
            data["GROUP_RECIEVER_ID"] = gsData[3]
            data["GROUP_CONTROL_NUMBER"] = gsData[6]
        # -------------------- ST * 810 Parsing----------------------------#
        if listOflines[value].startswith('ST*810*'):
            data["TRANSACTION_TYPE"]='810'
            data["TRANSACTION_ID"]=810
            #allcapture = allcapture
            print(listOflines[value])
            print('#---------------line 810------------------#')
            ind=0
            while True:
                if listOflines[value+ind].startswith('BIG'):
                    dataformat = listOflines[value+ind].split(elementDelimiter)
                    data['PURCHASE_ORDER_NUMBER'] = dataformat[4]
                    data['PO_DATE'] = dataformat[3]
                    data["INVOICE_DATE"] = dataformat[1]
                    data["INVOICE_NUMBER"] = dataformat[2]
                if listOflines[value+ind].startswith('TDS'):
                    print(listOflines[value+ind])
                    dataformat = listOflines[value+ind].split(elementDelimiter)
                    data["INVOICE_AMOUNT"] = dataformat[1]
                    allcapture.append(copy.deepcopy(data))
                    break
                else:
                    ind=ind+1
        # ------------------------  ST * 850 --------------------------#
        if listOflines[value].startswith('ST*850*'):
            print('#---------------line 850------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            data['PURCHASE_ORDER_NUMBER'] = dataformat[3]
            data['PO_DATE'] = dataformat[5]
            allcapture.append(copy.deepcopy(data))

        # ------------------------  ST * 856 --------------------------#

        if listOflines[value].startswith('ST*856*'):
            print(listOflines[value])
            data["TRANSACTION_CONTROL_NUMBER"]=listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"] = listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"] = listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS'] = 1

        elif listOflines[value].startswith('BSN'):
            data["ASN_NUMBER"]=listOflines[value].split(elementDelimiter)[2]
            data["ASN_DATE"]=listOflines[value].split(elementDelimiter)[3]
            allcapture.append(copy.deepcopy(data))
        elif listOflines[value].startswith('HL*') and listOflines[value].endswith('*O'):
            ind=1
            while True:
                print('inside while')
                if listOflines[value+ind].startswith('PRF'):
                    print(listOflines[value+ind])
                    data["PURCHASE_ORDER_NUMBER"] = listOflines[value+ind].split(elementDelimiter)[1]
                    data["PO_DATE"] = listOflines[value+ind].split(elementDelimiter)[4]
                    #print(data)
                    allcapture.append(copy.deepcopy(data))
                    break
                else:
                    ind=ind+1


        # ------------------------ End ST * 856 --------------------------#


        # ------------------------  ST * 997 --------------------------#
        if listOflines[value].startswith('ST*997*'):
            #print(listOflines[value])
            data["TRANSACTION_CONTROL_NUMBER"] = listOflines[value].split(elementDelimiter)[2]
            data["TRANSACTION_TYPE"]=listOflines[value].split(elementDelimiter)[1]
            data["TRANSACTION_ID"]=listOflines[value].split(elementDelimiter)[1]
            data['TRANSACTION_STATUS']=1
            data["ASN_NUMBER"] = None
            data["ASN_DATE"] = None

            ind = 1
            while True:
                print('inside while')
                if listOflines[value].startswith('AK2*'):
                    data["FA_STATUS"] = listOflines[value].split(elementDelimiter)[2]
                    # print(data)
                    allcapture.append(data)
                    break

                elif listOflines[value].startswith('ST*997*'):
                    allcapture.append(data)
                    break
                else:
                    ind = ind + 1
            # ------------------------  ST * 997 --------------------------#
        # ------------------------EDI fAct UNB Parsing --------------->#
        if listOflines[value].startswith('UNB+'):
            dataformat = listOflines[value].split(elementDelimiter)

            data = {'INTERCHANGE_SENDER_ID': dataformat[2].split(subementDelimeter)[0],
                    'INTERCHANGE_RECEIVER_ID': dataformat[3].split(subementDelimeter)[0],
                    'Interchange date': dataformat[4].split(subementDelimeter)[0],
                    'interchange time': dataformat[4].split(subementDelimeter)[1],
                    "INTERCHANGE_CONTROL_NUMBER": dataformat[5],
                    "TRANSACTION_CONTROL_NUMBER": dataformat[4].split(subementDelimeter)[-1],
                    'TRANSACTION_STATUS': 1,
                    "DIRECTION": 'INBOUND', 'Sender qualifiers': dataformat[1].split(subementDelimeter)[1],
                    'receiver qualifiers': dataformat[3].split(subementDelimeter)[1]}
            print(dataformat)
            data['INTERCHANGE_RECEIVER_ID']: dataformat[2].split(subementDelimeter)[1]
            data['Interchange date'] = dataformat[3].split(subementDelimeter)[1]
            data['interchange time'] = dataformat[4].split(subementDelimeter)[0]
            data["INTERCHANGE_CONTROL_NUMBER"] = dataformat[4].split(subementDelimeter)[-1]
            data["RECV_FILENAME"] = filename
            data['TRANSACTION_STATUS'] = 1
            data['TRANSACTION_CONTROL_NUMBER'] = dataformat[4].split(subementDelimeter)[-1]
            data["DIRECTION"] = 'INBOUND'
            dataformat2 = listOflines[value + 1].split(elementDelimiter)
            print(dataformat2)
            data["TRANSACTION_TYPE"] = dataformat2[2].split(subementDelimeter)[2]
            data["TRANSACTION_ID"] = dataformat2[1]
            allcapture.append(copy.deepcopy(data))

    df = pd.DataFrame(allcapture, columns=colList)
    df['CREATE_DATE_TIME']=pd.datetime.now()
    df['ASN_DATE']=pd.to_datetime(df['ASN_DATE'])
    df['INVOICE_DATE']=pd.to_datetime(df['INVOICE_DATE'])
    df['LAST_UPDATE_DATE_TIME']=pd.to_datetime(df['LAST_UPDATE_DATE_TIME'])
    df['PO_DATE']=pd.to_datetime(df['PO_DATE'])


    f.close()
    return df





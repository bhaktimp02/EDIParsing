import pandas as pd
import copy
from configs.constantsValue import colsumLists
import pdb as bp
#from ediParsingType import parsingUNA
import pprint


def dataParsing(filename, fpath):
    data = []
    print('{}\\{}'.format(fpath, filename))
    f = open('{}\\{}'.format(fpath, filename), 'r')
    texts = f.readlines()
    colList = colsumLists()
    listOflines = []
    for line in texts:
        # print(line)
        # print(len(line))
        if line.startswith("ISA"):
            print(line)
            elementDelimiter = line[3]
            segmentTerminator = line[105]
            # print('element segment: {}  && line segment : {}'.format(elementDelimiter,segmentTerminator))
            # print(line.split(elementDelimiter))
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
        if line.startswith("UNA"):
            print('--------entering in UNA----')
            #dataR = parsingUNA(line, data)
            elementDelimiter = line[4]
            segmentTerminator = line[8]
            subementDelimeter = line[3]
            dataSplit = (line).split(elementDelimiter)
            # dataSplit = line.split(elementDelimiter)

            # print(dataSplit)
            print(segmentTerminator, subementDelimeter, elementDelimiter)

        allLines = line.split(segmentTerminator)

        listOflines.extend(allLines[:-1])

    dataFinal = pd.DataFrame(columns=colList)
    print(dataFinal)
    allcapture = []
    data2 = {}
    valuee = 0
    # print(listOflines)
    for value in range(0, len(listOflines)):

        # ----------------- Group Parsing ---------------------------------#
        if listOflines[value].startswith('GS'):
            gsData = listOflines[value].split(elementDelimiter)
            # print(listOflines[value])
            # print(gsData)
            data["GROUP_SENDER_ID"] = gsData[2]
            data["GROUP_RECIEVER_ID"] = gsData[3]
            data["GROUP_CONTROL_NUMBER"] = gsData[6]
        # -------------------- ST * 850 Parsing----------------------------#
        if listOflines[value].startswith('ST*850*'):
            allcapture = allcapture
            print('#---------------line 850------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            data['PURCHASE_ORDER_NUMBER'] = dataformat[3]
            data['PO_DATE'] = dataformat[5]
            data["INVOICE_DATE"] = dataformat[5]
            data["INVOICE_NUMBER"] = dataformat[5]
            data["INVOICE_AMOUNT"] = dataformat[5]
            data2 = data
            # print(data2)
            allcapture.append(copy.deepcopy(data2))
        # ------------------------  ST * 810 --------------------------#
        if listOflines[value].startswith('ST*810*'):
            print('#---------------line 810------------------#')
            dataformat = listOflines[value + 1].split(elementDelimiter)
            # print(dataformat)
            data['PURCHASE_ORDER_NUMBER'] = dataformat[3]
            data['PO_DATE'] = dataformat[5]
        # ------------------------EDI fAct UNB Parsing --------------->#
        if listOflines[value].startswith('UNB+'):
            dataformat = listOflines[value].split(subementDelimeter)
            # print(dataformat)
            # print(data)
            #
            # print(dataformat[3].split(elementDelimiter)[1])
            data = {'INTERCHANGE_SENDER_ID': dataformat[1].split(elementDelimiter)[1],
                    'INTERCHANGE_RECEIVER_ID': dataformat[2].split(elementDelimiter)[1],
                    'Interchange date': dataformat[3].split(elementDelimiter)[1],
                    'interchange time': dataformat[4].split(elementDelimiter)[0],
                    "INTERCHANGE_CONTROL_NUMBER": dataformat[4].split(elementDelimiter)[-1],
                    "TRANSACTION_CONTROL_NUMBER": dataformat[4].split(elementDelimiter)[-1], 'TRANSACTION_STATUS': 1,
                    "DIRECTION": 'INBOUND', 'Sender qualifiers': dataformat[1].split(elementDelimiter)[1],
                    'receiver qualifiers': dataformat[2].split(elementDelimiter)[1]}
            data['INTERCHANGE_RECEIVER_ID']: dataformat[2].split(elementDelimiter)[1]
            data['Interchange date'] = dataformat[3].split(elementDelimiter)[1]
            data['interchange time'] = dataformat[4].split(elementDelimiter)[0]
            data["INTERCHANGE_CONTROL_NUMBER"] = dataformat[4].split(elementDelimiter)[-1]
            data["RECV_FILENAME"] = filename
            data['TRANSACTION_STATUS'] = 1
            data['TRANSACTION_CONTROL_NUMBER'] = dataformat[4].split(elementDelimiter)[-1]
            data["DIRECTION"] = 'INBOUND'

            #pprint.pprint(data)
            dataformat2 = listOflines[value + 1].split(subementDelimeter)
            #print(dataformat2)
            data["TRANSACTION_TYPE"] = dataformat2[0].split(elementDelimiter)[-1]
            data["TRANSACTION_ID"] = dataformat2[0].split(elementDelimiter)[-1]
            #print(data)
            allcapture.append(copy.deepcopy(data))


    df = pd.DataFrame(allcapture, columns=colList)
    df['CREATE_DATE_TIME']=pd.datetime.now()
    #del df['index']
    #df.to_csv('lib\output\{}.csv'.format(filename.replace('.','_')), index=False)
    f.close()
    return df





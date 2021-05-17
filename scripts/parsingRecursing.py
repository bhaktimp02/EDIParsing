import pdb as bp
import copy


def __isa(line,filename):
    print(line)
    print(len(line))
    line=line.lstrip(' ')
    elementDelimiter = line[3]
    #elementDelimiter=copy.deepcopy(line[3])
    if len(line)!=106:
        segmentTerminator = None
    else:
        print(line)
        print(line[-1])
        segmentTerminator=line[105]
    # print('element segment: {}  && line segment : {}'.format(elementDelimiter,segmentTerminator))
    # print(line.split(elementDelimiter))
    dataSplit = line.split(elementDelimiter)
    print(dataSplit)
    #print(elementDelimiter,segmentTerminator)
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
    subementDelimeter = None
    print('----------------{}-------------------------------'.format(line.strip(' ')))
    print('elementdelimeter:{} && segmentDelimketer:{} &&& sublemenetdelimeter:{}'
          .format(elementDelimiter, segmentTerminator, subementDelimeter))
    return(data,line[3])

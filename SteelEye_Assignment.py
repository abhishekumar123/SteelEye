# !/usr/bin/env python3


from urllib.request import urlopen
from xml.dom import minidom
from zipfile import ZipFile
import certifi
import ssl
import pandas as pd
import requests
from io import BytesIO
from lxml import etree
import datetime as dt
import time
import zipfile


# URL to download the xml file from
url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

# xml file to download into
xmlfile = 'download.xml'

#Logger Function
def log(func):
    def wrapper(*args):
        func_str = func.__name__
        print (func_str,args)
        args_str = ','.join(args)
        value = func(*args)
        with open('log.txt', 'a') as f:
            f.write(str(dt.datetime.now()) + "_" + str(func_str)+ "_")
            f.write(str(args_str) + "_")
            f.write(str(value))
            f.write('\n')
            #f.write(kwargs_str)
        return "Success"
    return wrapper

# DOWNLOAD XML
def download_xml(url, xmlfile):
    # fire the http request and download the file
    xml = open(xmlfile, "w+")
    xml.write(urlopen(url , context=ssl.create_default_context(cafile=certifi.where())).read().decode('utf-8'))
    xml.close()


# GET NODE TEXT
def getNodeText(node):
    nodelist = node.childNodes
    result = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            result.append(node.data)
    return ''.join(result)


# PARSE XML AND GET THE FIRST SUITABLE FILE'S URL LINK
def parse_xml_and_get_ziplink(xmlfile):
    print('Parsing xml file ' + xmlfile + ' to find the first download link whose file_type is DLTINS')

    xmldoc = minidom.parse(xmlfile)
    filelist = []

    doclist = xmldoc.getElementsByTagName('doc')

    for doc in doclist:
        strings_list = doc.getElementsByTagName('str')

        linkname = ''
        filename = ''
        filetype = ''

        for mystr in strings_list:

            if mystr.attributes['name'].value == 'download_link':
                linkname = getNodeText(mystr)
                print('download_link = ' + linkname)

            if mystr.attributes['name'].value == 'file_name':
                filename = getNodeText(mystr)
                print('file_name = ' + filename)

            if mystr.attributes['name'].value == 'file_type':
                filetype = getNodeText(mystr)
                print('file_type = ' + filetype)

        filelist.append({'filename': filename, 'filetype': filetype, 'download_link': linkname})

    # return the link for the first file of type DLTINS
    for filedict in filelist:
        if filedict['filetype'] == 'DLTINS':
            return (filedict['filename'], filedict['download_link'])






# CREATE CSV

def create_csv(datafile,csvfile):
    try:
        content = requests.get(datafile)
        zf = ZipFile(BytesIO(content.content))
        df = pd.DataFrame()
        for item in zf.namelist():
            df_temp = pd.DataFrame()
            print("File in zip: " + item)
            x = zf.open(item, 'r')
            # x.writelines('buffer.xml')
            # t = bs(x,"lxml")

            # CREATE CSV


            FinInstrmGnlAttrbts_Id = []
            FinInstrmGnlAttrbts_FullNm = []
            FinInstrmGnlAttrbts_ClssfctnTp = []
            FinInstrmGnlAttrbts_CmmdtyDerivInd = []
            FinInstrmGnlAttrbts_NtnlCcy = []
            Issr = []

            data_Id = ''
            data_FullNm = ''
            data_ClssfctnTp = ''
            data_CmmdtyDerivInd = ''
            data_NtnlCcy = ''
            data_Issr = ''

            for event, elem in etree.iterparse(x):

                # print ("Processing element tag = " + elem.tag)

                if elem.tag.endswith('}FinInstrmGnlAttrbts'):

                    for child in elem:
                        if child.tag.endswith("}Id"):
                            data_Id = child.text

                        elif child.tag.endswith("}FullNm"):
                            data_FullNm = child.text

                        elif child.tag.endswith("}ClssfctnTp"):
                            data_ClssfctnTp = child.text

                        elif child.tag.endswith("}CmmdtyDerivInd"):
                            data_CmmdtyDerivInd = child.text

                        elif child.tag.endswith("}NtnlCcy"):
                            data_NtnlCcy = child.text

                    elem.clear()

                elif elem.tag.endswith("}Issr"):
                    data_Issr = elem.text
                    FinInstrmGnlAttrbts_Id.append(data_Id)
                    FinInstrmGnlAttrbts_FullNm.append(data_FullNm)
                    FinInstrmGnlAttrbts_ClssfctnTp.append(data_ClssfctnTp)
                    FinInstrmGnlAttrbts_CmmdtyDerivInd.append(data_CmmdtyDerivInd)
                    FinInstrmGnlAttrbts_NtnlCcy.append(data_NtnlCcy)
                    Issr.append(data_Issr)
                    data_Id = ''
                    data_FullNm = ''
                    data_ClssfctnTp = ''
                    data_CmmdtyDerivInd = ''
                    data_NtnlCcy = ''
                    data_Issr = ''
                    elem.clear()

            df = pd.DataFrame({
                'FinInstrmGnlAttrbts.Id': FinInstrmGnlAttrbts_Id,
                'FinInstrmGnlAttrbts.FullNm': FinInstrmGnlAttrbts_FullNm,
                'FinInstrmGnlAttrbts.ClssfctnTp': FinInstrmGnlAttrbts_ClssfctnTp,
                'FinInstrmGnlAttrbts.CmmdtyDerivInd': FinInstrmGnlAttrbts_CmmdtyDerivInd,
                'FinInstrmGnlAttrbts_NtnlCcy': FinInstrmGnlAttrbts_NtnlCcy
            })

            #print("Saving the dataframe to csv file : " + csvfile)
            df.to_csv(csvfile, index=False)
        return "Success"
    except:
        return "Failure! please check the func"

    # print(df)


# MAIN PROGRAM

@log
def main():
    try:
        print('SteelEye Assignment Solution')

        # download xmlfile
        download_xml(url, xmlfile)
        print('Downloaded xml file : ' + xmlfile)

        # parse xml file
        (zipfilename, ziplink) = parse_xml_and_get_ziplink(xmlfile)

        print('Parsed xml file : ' + xmlfile)

        print('First DLTINS file name = ' + zipfilename)
        print('First DLTINS file link = ' + ziplink)

       

        print('Downloaded zip file : ' + zipfilename + "//"+ ziplink)

        # create the csv file
        datafile = ziplink
        csvfile = "OUTPUT" + str(time.time()) + ".csv"

        create_csv(datafile,csvfile)
        print("------------------------------------------------------------------------")
        print("CREATED THE FINAL RESULTS CSV FILE : " + csvfile)
        print("------------------------------------------------------------------------")
        return "Success"
    except:
        return "Please check some failure"
    # Skipping AWS Lambda and S3 as currently don't have active AWS account.Although S3 task could be done by Boto package.


if __name__ == "__main__":
    # calling main function
    main()

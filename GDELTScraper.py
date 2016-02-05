import os
from datetime import date, timedelta
import urllib
import sys
import zipfile
import requests
import json
import csv

'''
====================================================================
Scraper to download a '.CSV.zip' (actually tab-delimited) from GDELT's Daily Event Upload Repo.
Then uploads JSON to XDATA Kafka as Topic
========================================================================================
'''

'''
========================================================================================
GDELT addr: http://data.gdeltproject.org/events/index.html

Format of File : YYYYMMDD.export.CSV.zip (##.#MB) (MD5: @@@@@@@@@@@@@@@@)
File: YYYYMMDD.export.CSV ===> Tab-Delimited File Containing News Events separated by \n

58 attributes per line:
===========
GlobalEventID
Day
MonthYear
Year
FractionDate
===========
Actor1Code
Actor1Name
Actor1CountryCode
Actor1KyesterdaynGroupCode
Actor1EthnicCode
Actor1Religion1Code
Actor1Religion2Code
Actor1Type1Code
Actor1Type2Code
Actor1Type3Code
Actor2Code
Actor2Name
Actor2CountryCode
Actor2KyesterdaynGroupCode
Actor2EthnicCode
Actor2Religion1Code
Actor2Religion2Code
Actor2Type1Code
Actor2Type2Code
Actor2Type3Code
===========
IsRootEvent
EventCode
EventBaseCode
EventRootCode
QuadClass
GoldsteinScale
NumMentions
NumSources
NumArticles
AvgTone
===========
Actor1Geo_Type
Actor1Geo_Fullname
Actor1Geo_CountryCode
Actor1Ger_ADM1Code
Actor1Geo_Lat
Actor1Geo_Long
Actor1Geo_FeatureID
Actor2Geo_Type
Actor2Geo_Fullname
Actor2Geo_CountryCode
Actor2Ger_ADM1Code
Actor2Geo_Lat
Actor2Geo_Long
Actor2Geo_FeatureID
ActionGeo_Type
ActionGeo_Fullname
ActionGeo_CountryCode
ActionGer_ADM1Code
ActionGeo_Lat
ActionGeo_Long
ActionGeo_FeatureID
===========
DATEADDED
SOURCEURL
===========
Total: 58 Fields

========================================================================================
'''

yesterday = date.today() - timedelta(1)
print("Year: %d" % yesterday.year)	#2016
print("Month: %d" % yesterday.month)	#2
print("Day: %d" % yesterday.day)		#5
mth = ''
day = ''
if yesterday.month < 10: mth = '0' + str(yesterday.month)
if yesterday.day < 10: day = '0' + str(yesterday.day)
itemNo = int(str(yesterday.year) + mth + day)
print("Downloading %d" %itemNo)
url = "http://data.gdeltproject.org/events/"
fileForm = ".export.CSV.zip"
print("Downloading .zip...")

r = requests.get(url + str(itemNo) + fileForm)
with open("target.zip", "wb") as code:
	code.write(r.content)
print("Completed Download")
	
with zipfile.ZipFile('target.zip', 'r') as z:
	z.extractall()
print("Converting to JSON Format...")

items = 0
with open(str(itemNo) + '.export.CSV', 'rb') as f:
	reader=csv.reader(f,delimiter='\t')
	for line in reader:
		items+=1

jsfile = file('GDELT' + str(itemNo) + '.json','w')
jsfile.write('[\r\n')

with open(str(itemNo) + '.export.CSV', 'rb') as f:
 	reader=csv.reader(f,delimiter='\t')
 	itera = 0
 	for line in reader:
 		itera += 1
 		jsfile.write('\t{\r\n')
 	 	jsfile.write('\t\t\"GlobalEventID\": \"' + line[0] + '\",\r\n')
		jsfile.write('\t\t\"Day\": \"' + line[1] + '\",\r\n')
		jsfile.write('\t\t\"MonthYear\": \"' + line[2] + '\",\r\n')
		jsfile.write('\t\t\"Year\": \"' + line[3] + '\",\r\n')
		jsfile.write('\t\t\"FractionDate\": \"' + line[4] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Code\": \"' + line[5] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Name\": \"' + line[6] + '\",\r\n')
		jsfile.write('\t\t\"Actor1CountryCode\": \"' + line[7] + '\",\r\n')
		jsfile.write('\t\t\"Actor1KyesterdaynGroupCode\": \"' + line[8] + '\",\r\n')
		jsfile.write('\t\t\"Actor1EthnicCode\": \"' + line[9] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Religion1Code\": \"' + line[10] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Religion2Code\": \"' + line[11] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Type1Code\": \"' + line[12] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Type2Code\": \"' + line[13] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Type3Code\": \"' + line[14] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Code\": \"' + line[15] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Name\": \"' + line[16] + '\",\r\n')
		jsfile.write('\t\t\"Actor2CountryCode\": \"' + line[17] + '\",\r\n')
		jsfile.write('\t\t\"Actor2KyesterdaynGroupCode\": \"' + line[18] + '\",\r\n')
		jsfile.write('\t\t\"Actor2EthnicCode\": \"' + line[19] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Religion1Code\": \"' + line[20] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Religion2Code\": \"' + line[21] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Type1Code\": \"' + line[22] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Type2Code\": \"' + line[23] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Type3Code\": \"' + line[24] + '\",\r\n')
		jsfile.write('\t\t\"IsRootEvent\": \"' + line[25] + '\",\r\n')
		jsfile.write('\t\t\"EventCode\": \"' + line[26] + '\",\r\n')
		jsfile.write('\t\t\"EventBaseCode\": \"' + line[27] + '\",\r\n')
		jsfile.write('\t\t\"EventRootCode\": \"' + line[28] + '\",\r\n')
		jsfile.write('\t\t\"QuadClass\": \"' + line[29] + '\",\r\n')
		jsfile.write('\t\t\"GoldsteinScale\": \"' + line[30] + '\",\r\n')
		jsfile.write('\t\t\"NumMentions\": \"' + line[31] + '\",\r\n')
		jsfile.write('\t\t\"NumSources\": \"' + line[32] + '\",\r\n')
		jsfile.write('\t\t\"NumArticles\": \"' + line[33] + '\",\r\n')
		jsfile.write('\t\t\"AvgTone\": \"' + line[34] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_Type\": \"' + line[35] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_Fullname\": \"' + line[36] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_CountryCode\": \"' + line[37] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Ger_ADM1Code\": \"' + line[38] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_Lat\": \"' + line[39] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_Long\": \"' + line[40] + '\",\r\n')
		jsfile.write('\t\t\"Actor1Geo_FeatureID\": \"' + line[41] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_Type\": \"' + line[42] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_Fullname\": \"' + line[43] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_CountryCode\": \"' + line[44] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Ger_ADM1Code\": \"' + line[45] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_Lat\": \"' + line[46] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_Long\": \"' + line[47] + '\",\r\n')
		jsfile.write('\t\t\"Actor2Geo_FeatureID\": \"' + line[48] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_Type\": \"' + line[49] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_Fullname\": \"' + line[50] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_CountryCode\": \"' + line[51] + '\",\r\n')
		jsfile.write('\t\t\"ActionGer_ADM1Code\": \"' + line[52] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_Lat\": \"' + line[53] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_Long\": \"' + line[54] + '\",\r\n')
		jsfile.write('\t\t\"ActionGeo_FeatureID\": \"' + line[55] + '\",\r\n')
		jsfile.write('\t\t\"DATEADDED\": \"' + line[56] + '\",\r\n')
		jsfile.write('\t\t\"SOURCEURL\": \"' + line[57] + '\",\r\n')
		jsfile.write('\t}')

		if itera < items:
			jsfile.write(',\r\n')
		else:
			jsfile.write('\r\n')

jsfile.write(']')
jsfile.close()
print("Finished")

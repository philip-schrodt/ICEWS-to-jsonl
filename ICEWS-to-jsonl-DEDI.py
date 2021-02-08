"""
ICEWS_PROTEST_to_jsonl.py

Date-blocks and deduplicates (based on sentence) ICEWS records produced by ICEWS_PROTEST_select.py, producing the intermediate file 
protest-texts-dedup-scr.txt and converts to intermediate (pre-merge) JSONL records for input to ICEWS_PROT_merge.py

TO RUN PROGRAM:

python3 ICEWS_PROTEST_to_jsonl.py

REQUIRES:

countrynames-DEDI.txt (modified from text_to_CAMEO)

PROGRAMMING NOTES:

1. <20.11.10> Note that the output can produce a count of zero for a country if the only entry was outside the bounds of the month

SYSTEM REQUIREMENTS
This program has been successfully run under Mac OS 10.10.5; it is standard Python 3.7 so it should also run in Unix or Windows. 

PROVENANCE:
Programmer: Philip A. Schrodt
            Parus Analytics
            Charlottesville, VA, 22901 U.S.A.
            http://parusanalytics.com

Copyright (c) 2019	Philip A. Schrodt.	All rights reserved.

This code is covered under the MIT license: http://opensource.org/licenses/MIT

Report bugs to: schrodt735@gmail.com

REVISION HISTORY:
17-Apr-2019:	Initial version
12-Aug-2019:    Converted records to closer to final form; added date filtering; did ctry-date sort and dup[:-128] detection
14-Sep-2020:    Switched to single month selection following backdating

=========================================================================================================
"""

import utilDEDI2021
import datetime
import json
import re
import os

FILE_SUFFIX = "2018Q4"
FILE_SUFFIX = "2019Q12"
FILE_SUFFIX = "2018Q3"
FILE_SUFFIX = "2018Q1"
FILE_SUFFIX = "2020Q1partial"
FILE_SUFFIX = "2020Q1"
FILE_SUFFIX = "2019-June"
FILE_SUFFIX = utilDEDI2021.MONTH_INFIX[:4] + "-" + utilDEDI2021.MONTH_INFIX[-2:]

INFILE = "protest-texts-" + utilDEDI2021.MONTH_INFIX + utilDEDI2021.WEEK_SUFFIX + ".txt"
OUTFILE = "protest-records-" + utilDEDI2021.MONTH_INFIX + utilDEDI2021.WEEK_SUFFIX + ".jsonl"
DEDUPFILENAME = "protest-texts-dedup-scr.txt"   # intermediate file which is deleted

firstint = int(utilDEDI2021.MONTH_INFIX) * 100 + 1
lastint = firstint + [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][int(utilDEDI2021.MONTH_INFIX[-2:])] - 1


#ICEWS fields

EVENT_ID = 0
EVENT_DATE = 1
SOURCE_NAME = 2
SOURCE_SECTORS = 3
SOURCE_COUNTRY = 4
EVENT_TEXT = 5
INTENSITY = 6
TARGET_NAME = 7
TARGET_SECTORS = 8
TARGET_COUNTRY = 9
STORY_ID = 10
SENTENCE_NUMBER = 11
EVENT_SENTENCE = 12
PUBLISHER = 13
SOURCE = 14
HEADLINE = 15
CITY = 16
DISTRICT = 17
PROVINCE = 18
COUNTRY = 19
LATITUDE = 20
LONGITUDE = 21
PRODUCTID = 22
HOLDINGID = 23
LANGUAGE = -1

# new fields with the addition of CAMEO code sometime in May-2020
EVENT_ID = 0
EVENT_DATE = 1
SOURCE_NAME = 2
SOURCE_SECTORS = 3
SOURCE_COUNTRY = 4
EVENT_TEXT = 5
CAMEO_CODE = 6
INTENSITY = 7
TARGET_NAME = 8
TARGET_SECTORS = 9
TARGET_COUNTRY = 10
STORY_ID = 11
SENTENCE_NUMBER = 12
EVENT_SENTENCE = 13
PUBLISHER = 14
SOURCE = 15
HEADLINE = 16
CITY = 17
DISTRICT = 18
PROVINCE = 19
COUNTRY = 20
LATITUDE = 21
LONGITUDE = 22
PRODUCTID = 23
HOLDINGID = 24
LANGUAGE = 25  # this only works sometime after 2021.21, so there's an intermediate point where it won't

def init_CAMEO():
    CAMEO_eventcodes = {}   # conversion dictionaries
    
    try: 
        fin = open("CAMEO_codefile.txt",'r') 
    except IOError:
        print("\aError: Could not find the event code file CAMEO_codefile.txt")
        exit()	

    caseno = 1
    line = fin.readline()
    while len(line) > 0:
        if line.startswith('LABEL'):
            part = line[line.find(' ')+1:].partition(' ')
            CAMEO_eventcodes[part[2][:-1]] = part[0][:-1]
    #		print CAMEO_eventcodes[part[2][:-1]]
            caseno += 1
    #	if caseno > 32: break   # debugging exit 		
        line = fin.readline()
    return CAMEO_eventcodes


def init_languages():    
    try: 
        fin = open("ICEWS-sources.txt",'r') 
    except IOError:
        print("\aError: Could not find the event code file ICEWS-sources.txt")
        exit()	

    curlg = "--"
    publ_lang = {}   
    for line in fin:
        if len(line) < 4:
            curlg = line[:2]
            publ_lang[curlg] = []
        else:
            publ_lang[curlg].append(line[:-1])
    fin.close()
    return publ_lang

            
def get_MM_template(value):
    dayOfWeek = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
    oneday = datetime.timedelta(days = 1)
    data = {
        "id": value[EVENT_ID],
        "country": None,
        "ccode": None,
        "year": value[EVENT_DATE][:4],
        "region": None,
        "size": None,
        "date":  value[EVENT_DATE],
        "enddate":  value[EVENT_DATE],
        "protesterviolence": None,
        "continuation": None,
        "location": None,
        "sizeCategory": None,
        "protesteridentity":  value[SOURCE_NAME] + ": " + value[SOURCE_SECTORS]  + ": " + value[SOURCE_COUNTRY],
        "protesterdemand": None,
        "stateresponse": None,
        "eventText": value[EVENT_TEXT],
        "event": None,
        "headline": value[HEADLINE].replace("\\n",""),
        "text": value[EVENT_SENTENCE],
        "language": ["en"], 
        "publication": value[PUBLISHER] + ": " + value[SOURCE], 
        "citation": value[PRODUCTID] + ": " + value[HOLDINGID], 
        "coder": "Parus Analytics",
        "version": "1.0b1",
        "codedDate": datetime.datetime.now().strftime('%Y-%m-%d'),
        "comment": ""
        }
    if data['citation'] == ": ":
        data['citation'] = None
    if LANGUAGE > 0 and value[LANGUAGE] != "NULL":
        data['language'] = [value[LANGUAGE]]
    data['headline'] = data['headline'].replace("\u2018",'"').replace("\u2019",'"').replace("\u2013",'-').replace("\u2014",'--')
    txt = data['text']
    for substr in header_pats:
        if substr in txt:
            txt = txt[txt.find(substr) + len(substr):]
    data['text'] = txt.replace("\u2018",'"').replace("\u2019",'"').replace("\u2013",'-').replace("\u2014",'--')
    if data['eventText'] in CAMEO_convert:
        data['event'] = CAMEO_convert[data['eventText']]
    else:
        data['event'] = "000"
        Missing_eventcodes.append((data['eventText'], data['id']))

    for day in dayOfWeek:
        if "ON " + day in data['text'].upper() or "THIS " + day in data['text'].upper() or "LAST " + day in data['text'].upper():
            targ = day
            break
    else:
        return data
#    datetripl = [int(data['date'][:4]), int(data['date'][5:7]), int(data['date'][-2:])]
    thedate = datetime.datetime(int(data['date'][:4]), int(data['date'][5:7]), int(data['date'][-2:]))
    theweekday = dayOfWeek[thedate.weekday()]
    if theweekday != targ:
        data['text'] +=  " [autobackdated from " + data['date'] +"]"
        data['headline'] +=  " [autobackdated from " + data['date'] +"]"
        while theweekday != targ:  # first time through is known True FWIW...
            thedate = thedate - oneday
            theweekday = dayOfWeek[thedate.weekday()]
        data['date'] = thedate.isoformat()[:10]        
        data['enddate'] = data['date']      

    return data  


def init_countryloc(data, value):
    data['country'] = value[COUNTRY]
    if data['country'] == "the former Yugoslav Republic of Macedonia":  # modified 20.10.02
        data['country'] = "North Macedonia"
    data['ccode'] = countrynames.get(value[COUNTRY], "---")
    data['location'] = [{'city': value[CITY], 'district': value[DISTRICT], 'province': value[PROVINCE],
                        'country': value[COUNTRY], 'lat': value[LATITUDE],'long': value[LONGITUDE]}]
    data['region'] = countryregion[value[COUNTRY]]
    return data  


def intdate(datestr):
    return int(datestr[:4] + datestr[5:7] + datestr[8:10])


header_pats = [
    "[Computer selected and disseminated without OSE editorial intervention] ",
    ") -- ",
    ") - ",
    ") \u2013 ", # why doesn't \ need to be escaped in all of these?
    ") \u2014 ",
]

sents = {}
records = {}
nrec, ndup = 0, 0
with open(INFILE,"r") as fin:
    line = fin.readline() 
    while line:
        fullrec = line
        field = fullrec.split("\t")
        date = field[1]
        line = fin.readline() 
        line = fin.readline() 
        curdate = intdate(date)
        if curdate >= firstint and curdate <= lastint:
            sentln = line
            if date in records:
                if sentln in sents[date]:
#                    print("++", date, sentln[:-1])
                    ndup += 1
                else:
                    records[date].append(fullrec)
                    sents[date].append(sentln)
            else:
                records[date] = [fullrec]
                sents[date] = [sentln]
            nrec += 1
        line = fin.readline() 

print("Duplicate sentences:", ndup)
print("Writing", nrec - ndup, "intermediate records to", DEDUPFILENAME)
with open(DEDUPFILENAME, "w") as fout:
    for key, val in records.items():
        fout.write(key + "\n")
        for li in val:
            fout.write(li)
        fout.write("\n")
        
countrynames = {}  # translates country names to ISO-3166-alpha-3: note this has filled the missing COW entries for assorted islands
countryregion = {}
for ka, line in enumerate(open("countrynames-DEDI.txt","r")):
    part = line.split('\t')
    countrynames[part[0]] = part[1]

    try:
        cowcode = int(part[2])
    except:
        cowcode = -1     # handles cases where the region is set directly as text rather than through a code
        
    if cowcode == -1: 
        countryregion[part[0]] = part[2].replace("-", " ")
    elif cowcode >= 100 and  cowcode <= 165:
        countryregion[part[0]] = "South America" 
    elif cowcode >= 40 and  cowcode <= 95:
        countryregion[part[0]] = "Central America" 
    elif cowcode >= 20 and  cowcode <= 70:
        countryregion[part[0]] = "North America" 
    elif cowcode >= 200 and  cowcode <= 390:
        countryregion[part[0]] = "Europe" 
    elif cowcode >= 700 and  cowcode <= 910:
        countryregion[part[0]] = "Asia" 
    elif cowcode >= 600 and  cowcode <= 698:
        countryregion[part[0]] = "MENA" 
    elif cowcode >= 402 and  cowcode <= 590:
        countryregion[part[0]] = "Africa" 
    else:
        countryregion[part[0]] = "No assigned region" 
    
    """1. South America – country codes 100-165
    2. Central America – country codes 40-95
    3. North America – country codes 20-70
    4. Europe – country codes 200-390
    5. Asia – country codes 700-910
    6. MENA – country codes 600-698
    7. Africa – country codes 402-590"""

Missing_eventcodes = [] # records phrases not in the files
CAMEO_convert = init_CAMEO()
publish_lang  = init_languages()

kyld = 0
alldata = {}
for ka, line in enumerate(open(DEDUPFILENAME,"r")):
    if len(line) < 16:  # skips the date separators
        continue
    row = line[:-1].split("\t")
    dat1 = get_MM_template(row)
    dat1 = init_countryloc(dat1, row)
    if dat1['ccode'] in alldata:
        if dat1['date'] in alldata[dat1['ccode']]:
            alldata[dat1['ccode']][dat1['date']].append(dat1)
        else:
            alldata[dat1['ccode']][dat1['date']] = [dat1]
    else:
        alldata[dat1['ccode']] = {}
        alldata[dat1['ccode']][dat1['date']] = [dat1]
    for key, val in publish_lang.items():
        if dat1["publication"].partition(":")[0] in val:
            dat1["language"] = [key]            
    kyld += 1

if len(Missing_eventcodes) > 0:
    print("Missing event codes:")   # 20.09.14: hasn't ever been an issue in ICEWS  but hey, cheap check
    for li in Missing_eventcodes:   #           probably code imported from some RIDIR project, where this could be a problem due to 
        print(li)                   #           ongoing dictionary modification
else:
    print("All eventCodes were found")

os.remove(DEDUPFILENAME)

tarmo = utilDEDI2021.MONTH_INFIX[-2:]
ccount = {}
csets = []
nrec, ndup = 0, 0
with open(OUTFILE, "w") as fout:
    for ccode, adict in sorted(alldata.items()):
        if ccode == "USA":
            continue
        ccount[ccode] = 0
        for key, val in sorted(adict.items()):
            for rec in val: # check for duplicates
                wordList = re.sub("[^\w]", " ",  rec['text']).split()
                cwrd = set(wordList)
#                csets.append(cwrd)
                tt = rec['text'][-96:]
                ts = rec['text'][:48]
                theday = int(rec['date'][-2:])
                for cs in csets:
                    try:  # deals with divide by zero
                       if len(cs - cwrd)/len(cwrd) < 0.30 and len(cwrd - cs)/len(cs) < 0.30:
                            """if len(cs - cwrd)/len(cwrd) > 0.20 or len(cwrd - cs)/len(cs) > 0.20: ### debug
                                print(cs  + "\n" + cwrd + "\ndiff:" + str(cs - cwrd))
                            print(len(cs - cwrd)/len(cwrd), + len(cwrd - cs)/len(cs),'\n')"""
                            ndup += 1
                            break
                    except:
                        pass
                else:
                    csets.append(cwrd)
                    if rec['date'][5:7] == tarmo:  # single month select
                        fout.write(json.dumps(rec, indent=2, sort_keys=True ) + "\n")
                        ccount[ccode] += 1
                        nrec += 1

print("Ctry-date duplicates:", ndup)
print("Wrote", nrec, "new records to", OUTFILE)
sorted_x = sorted(ccount.items(), key=lambda kv: kv[1], reverse=True)
THRES = 16  # some old formatting stuff that does separate lines per country if count > THRES
nbin = THRES
for tu in sorted_x:
    if tu[1] >= THRES:
        print(tu[0], tu[1])
    else:
        if tu[1] != nbin:
            nbin = tu[1]
            print("\n" + str(nbin), end=" ")
        print(tu[0], end=" ")
print()
        


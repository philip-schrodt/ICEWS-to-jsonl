"""
utilDEDI2021.py

jsonl read/write for the DEDI programs and other utilities

NOTE 19.10.22: Those customized writeedit()/writesrc() -- originally there is provide a more readable 
               JSONL format for manual editing, which wasn't needed after a while -- were a bad idea,
               or at least should have used a more systematic (and compatible) way of dealing with None

Programmer: Philip A. Schrodt <schrodt735@gmail.com>
This code is covered under the MIT license: http://opensource.org/licenses/MIT

REVISION HISTORY:
31-Jul-2019:	Initial version
07-Aug-2019:    Added timestamp()
=========================================================================================================
"""

import datetime
import json

WEEK_SUFFIX = "Wk5"
MONTH_INFIX = "202101"
MONTH_SUFFIX = "-" + MONTH_INFIX + ".jsonl"

recOrder = ["ccode", "status", 
            "+date", "comment", "country", 
            "+id", "icewsid",
            "-headline", 
            "-text", 
            "+size", "sizeCategory", 
            "+protesterdemand", "stateresponse", 
            "+protest",  "protesterviolence", "protesteridentity",
            "+event", "eventText",
            "-location", 
            "+region", "version", "language",  "publication", "year", "enddate", "citation", "codedDate", "coder"]

srcOrder = ["ccode", "status", "+id",
            "+date", "comment", "country", "region", "event", "eventText",            
            "-headline", 
            "-text", 
            "+size", "sizeCategory", 
            "+protesterdemand", "stateresponse", 
            "+protesterviolence", "protesteridentity",
            "-location", 
            "+region", "version", "language", "publication", "year", "enddate", "citation"]

def read_file(filename):
    """ returns next record in a line-delimited JSON file """
    jstr = ""
    for line in open(filename, "r"):
        if line.startswith("}"):
#            print(jstr)   # debug: uncomment to find badly formed cases, or put this into a try/except
            adict = json.loads(jstr + "}")
            yield adict
            jstr = ""
        else:
            if "\t" in line:
                line = line.replace("\t", "\\t")
            jstr += line[:-1].strip()
            
def writeedit(rec, fout):
    """ Write combined record """
    fout.write('{\n')
    for fl in recOrder[:-1]:
        if fl.startswith("-"):
            fl = fl[1:]
            fout.write('\n"' + fl + '":\n')
            if fl == "location":
                fout.write(json.dumps(rec[fl]) + ",")
            else:
                fout.write(json.dumps(rec[fl], indent=2, sort_keys=True ) + ",")
                        
        elif fl.startswith("+"):
            fout.write("\n")
            fl = fl[1:]
            fout.write('"' + fl + '": "' + str(rec[fl])  + '", ')
        else:
            if fl == "eventText":
                fout.write(json.dumps(rec[fl]) + ",")
            else:
                fout.write('"' + fl + '": "' + str(rec[fl])  + '", ')
    fl = recOrder[-1]
    fout.write('"' + fl + '": "' + str(rec[fl])  + '"\n}\n')


def writesrc(rec, fout):
    """ Write original record """
    fout.write('{\n')
    for fl in srcOrder[:-1]:
        if fl.startswith("-"):
            fl = fl[1:]
            fout.write('\n"' + fl + '":\n')
            if fl == "location":
                fout.write(json.dumps(rec[fl]) + ",")
            else:
                fout.write(json.dumps(rec[fl], indent=2, sort_keys=True ) + ",")
                        
        elif fl.startswith("+"):
            fout.write("\n")
            fl = fl[1:]
            fout.write('"' + fl + '": "' + str(rec[fl])  + '", ')
        else:
            if fl == "eventText":
                fout.write('"eventText": '+ json.dumps(rec[fl]) + ",")
            else:
                fout.write('"' + fl + '": "' + str(rec[fl])  + '", ')
    fl = srcOrder[-1]  
    fout.write('"' + fl + '": "' + str(rec[fl])  + '"\n}\n')

    
def timestamp():
    return '-' + datetime.datetime.now().strftime("%Y%m%d")[2:] + "-" + datetime.datetime.now().strftime("%H%M%S") + ".jsonl"

    
def newdate(isodate, forward = False):
    """move the date back one day
    Note: Python 3.7 has a "datetime.fromisoformat()" function to do the conversion without the string conversions. Though now I've written them..."""
    if forward:
        thedate = datetime.date(int(isodate[:4]), int(isodate[5:7]), int(isodate[-2:])) + datetime.timedelta(days = 1)
    else:
        thedate = datetime.date(int(isodate[:4]), int(isodate[5:7]), int(isodate[-2:])) - datetime.timedelta(days = 1)
    return thedate.isoformat(), thedate
    

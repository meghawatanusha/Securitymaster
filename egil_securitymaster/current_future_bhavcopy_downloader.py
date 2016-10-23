import urllib2
import httplib, StringIO, zipfile
import os
import sys
import requests, zipfile, io
import datetime

now = datetime.datetime.now()

def downloadCSV(year="2013",mon="NOV",dd="06"):
    filename = "fo%s%s%sbhav.csv" % (dd,mon,year)
    url = "https://www.nseindia.com/content/historical/DERIVATIVES/%s/%s/%s.zip" % (year, mon, filename)

    print url
    print "Downloading %s ..." % (filename)
    result = requests.get(url)
    if (result.status_code != 404):
        z = zipfile.ZipFile(io.BytesIO(result.content))
        z.extractall()


def main(args):
       
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    now = datetime.datetime.now()
    while(start_date < now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
        downloadCSV(start_date.year,month.upper(),day)
        start_date += datetime.timedelta(days=1)

if __name__ == "__main__":
    
    print "Enter the date from which you want to downlaod the bhavcopies in yyyy mm dd format."
    main(sys.argv[1:])

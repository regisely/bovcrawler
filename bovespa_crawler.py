# Create PostgreSQL table with:
# CREATE TABLE dailyquotes (TIPREG INTEGER, DATAPREG DATE, CODBI VARCHAR(2), CODNEG VARCHAR(12), TPMERC INTEGER, NOMRES VARCHAR(12), ESPECI VARCHAR(10), PRAZOT VARCHAR(3), MODREF VARCHAR(4), PREABE DECIMAL(13,2), PREMAX DECIMAL(13,2), PREMIN DECIMAL(13,2), PREMED DECIMAL(13,2), PREULT DECIMAL(13,2), PREOFC DECIMAL(13,2), PREOFV DECIMAL(13,2), TOTNEG INTEGER, QUATOT BIGINT, VOLTOT DECIMAL(18,2), PREEXE DECIMAL(13,2), INDOPC INTEGER, DATVEN DATE, FATCOT INTEGER, PTOEXE DECIMAL(13,6), CODISI VARCHAR(12), DISMES INTEGER, PRIMARY KEY(DATAPREG, CODNEG, PRAZOT));

import psycopg2
import sys
import argparse
import csv
from urllib import urlopen
from zipfile import ZipFile, BadZipfile
from StringIO import StringIO
from datetime import date, timedelta

tday = date.today()
tday_url = "D" + tday.strftime("%d%m%Y")
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--date", help="date to download, e.g. A2000, M122006, D28102010", default = tday_url)
parser.add_argument("-o", "--csv", help="save data into csv file, e.g. -o quotes.csv")
args = parser.parse_args()

try:
    response = urlopen("http://www.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_" + args.date + ".ZIP")
    zipfile = ZipFile(StringIO(response.read()))
    txtfile = zipfile.namelist()[0]
    data = zipfile.open(txtfile).readlines()
except (IOError, BadZipfile), e:
    print 'Download Error: %s' % e
    sys.exit(1)

data = data[1:(len(data)-1)]
table = []
for line in data:
    table.append((int(line[0:2]), line[2:6] + '-' + line[6:8] + '-' + line[8:10], " ".join(line[10:12].split()), " ".join(line[12:24].split()), int(line[24:27]), " ".join(line[27:39].split()), " ".join(line[39:49].split()), " ".join(line[49:52].split()), " ".join(line[52:56].split()), float(line[56:69])/100, float(line[69:82])/100, float(line[82:95])/100, float(line[95:108])/100, float(line[108:121])/100, float(line[121:134])/100, float(line[134:147])/100, int(line[147:152]), int(line[152:170]), float(line[170:188])/100, float(line[188:201])/100, int(line[201:202]), line[202:206] + '-' + line[206:208] + '-' + line[208:210], int(line[210:217]), float(line[217:230])/1000000, " ".join(line[230:242].split()), int(line[242:245])))

if args.csv:
   with open(args.csv, "wb") as f:
       writer = csv.writer(f)
       writer.writerows(table)
else:
    try:
        conn = psycopg2.connect("dbname='d42gerp07cu8em' user='dcemakroeprcer' host='ec2-54-83-29-133.compute-1.amazonaws.com' password='2oAEdkG9Q6RHDhhgBNEhKhk70Q'")
        cur = conn.cursor()
        args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x) for x in table)
        cur.execute("INSERT INTO dailyquotes (TIPREG, DATAPREG, CODBI, CODNEG, TPMERC, NOMRES, ESPECI, PRAZOT, MODREF, PREABE, PREMAX, PREMIN, PREMED, PREULT, PREOFC, PREOFV, TOTNEG, QUATOT, VOLTOT, PREEXE, INDOPC, DATVEN, FATCOT, PTOEXE, CODISI, DISMES) VALUES " + args_str)
        conn.commit()
    except psycopg2.DatabaseError, e:
        if conn:
            conn.rollback()
            print 'Database Error: %s' % e
            sys.exit(1)
    finally:
        if conn:
            conn.close()

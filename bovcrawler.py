# BMFBovespa Daily Quotes Crawler 

import psycopg2
import sys
import argparse
import csv
import itertools
import json
from urllib import urlopen
from zipfile import ZipFile, BadZipfile
from StringIO import StringIO
from datetime import date, timedelta
from collections import OrderedDict

tday = date.today()
tday_url = "D" + tday.strftime("%d%m%Y")
parser = argparse.ArgumentParser(prog='bovcrawler', description='Downloads data from BMFBovespa historical quotes and saves into a PostgreSQL table, a csv file or a json file')
parser.add_argument("date", help="date to download, e.g. A2000, M122015, D17022016 (defaults to today)", nargs='?', default=tday_url)
parser.add_argument("--db", help="save data into a Postgres table named bovquotes using settings in .dbsettings", action="store_true")
parser.add_argument("--csv", help="save data as a csv file, e.g., --csv quotes.csv")
parser.add_argument("--json", help="save data as a json file, e.g., --json quotes.json")
parser.add_argument("--allyears", help="save all data from specified year to current year, e.g., A2010 --db --allyears", action="store_true")
args = parser.parse_args()

if args.date[0] not in ['A', 'M', 'D']:
    print "Error: date should start with A, M or D, e.g., D17022016"
    sys.exit(1)

if not any((args.db, args.csv, args.json)):
    parser.print_help()
    sys.exit(0)

if args.allyears:
    thisyear = int(tday.strftime("%Y"))
    args.date = ['A' + str(y) for y in range(int(args.date[1:]),(thisyear+1))]
else:
    args.date = [args.date]

for dateurl in args.date:    
    try:
        response = urlopen("http://www.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_" + dateurl + ".ZIP")
        zipfile = ZipFile(StringIO(response.read()))
        txtfile = zipfile.namelist()[0]
        data = zipfile.open(txtfile).readlines()
    except (IOError, BadZipfile) as e:
        print 'Download Error: %s' % e
        print "Check if date is valid..."
        sys.exit(1)

    data = data[1:(len(data)-1)]
    table = []
    for line in data:
        table.append((int(line[0:2]), line[2:6] + '-' + line[6:8] + '-' + line[8:10], " ".join(line[10:12].split()), " ".join(line[12:24].split()), int(line[24:27]), " ".join(line[27:39].split()), " ".join(line[39:49].split()), " ".join(line[49:52].split()), " ".join(line[52:56].split()), float(line[56:69])/100, float(line[69:82])/100, float(line[82:95])/100, float(line[95:108])/100, float(line[108:121])/100, float(line[121:134])/100, float(line[134:147])/100, int(line[147:152]), int(line[152:170]), float(line[170:188])/100, float(line[188:201])/100, int(line[201:202]), line[202:206] + '-' + line[206:208] + '-' + line[208:210], int(line[210:217]), float(line[217:230])/1000000, " ".join(line[230:242].split()), int(line[242:245])))

    if args.db:
        try:
            with open(".dbsettings", 'r') as f:
                dbset = f.read().replace('\n', ' ')
        except:
            print("Settings Error: you should enter database settings in a .dbsettings file")
            sys.exit(1)
        conn = None
        try:
            conn = psycopg2.connect(dbset)
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS bovquotes (TIPREG INTEGER, DATAPREG DATE, CODDBI VARCHAR(2), CODNEG VARCHAR(12), TPMERC INTEGER, NOMRES VARCHAR(12), ESPECI VARCHAR(10), PRAZOT VARCHAR(3), MODREF VARCHAR(4), PREABE DECIMAL(13,2), PREMAX DECIMAL(13,2), PREMIN DECIMAL(13,2), PREMED DECIMAL(13,2), PREULT DECIMAL(13,2), PREOFC DECIMAL(13,2), PREOFV DECIMAL(13,2), TOTNEG INTEGER, QUATOT BIGINT, VOLTOT DECIMAL(18,2), PREEXE DECIMAL(13,2), INDOPC INTEGER, DATVEN DATE, FATCOT INTEGER, PTOEXE DECIMAL(13,6), CODISI VARCHAR(12), DISMES INTEGER, PRIMARY KEY(TPMERC, CODDBI, CODNEG, DATAPREG, PRAZOT))")
            args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", x) for x in table)
            cur.execute("INSERT INTO bovquotes (TIPREG, DATAPREG, CODDBI, CODNEG, TPMERC, NOMRES, ESPECI, PRAZOT, MODREF, PREABE, PREMAX, PREMIN, PREMED, PREULT, PREOFC, PREOFV, TOTNEG, QUATOT, VOLTOT, PREEXE, INDOPC, DATVEN, FATCOT, PTOEXE, CODISI, DISMES) VALUES " + args_str)
            conn.commit()
        except psycopg2.DatabaseError as e:
            if conn is not None:
                conn.rollback()
                print 'Database Error: %s' % e
                sys.exit(1)
        finally:
            if conn is not None:
                conn.close()
    if args.csv:
        with open(args.csv, 'ab') as f:
            writer = csv.writer(f)
            writer.writerows(table)
    if args.json:
        keys_table = ['TIPREG', 'DATAPREG', 'CODDBI', 'CODNEG', 'TPMERC', 'NOMRES', 'ESPECI', 'PRAZOT', 'MODREF', 'PREABE', 'PREMAX', 'PREMIN', 'PREMED', 'PREULT', 'PREOFC', 'PREOFV', 'TOTNEG', 'QUATOT', 'VOLTOT', 'PREEXE', 'INDOPC', 'DATVEN', 'FATCOT', 'PTOEXE', 'CODISI', 'DISMES']
        table_dict = [dict(itertools.izip_longest(keys_table, t)) for t in table]
        table_dictord = [OrderedDict(sorted(item.iteritems(), key=lambda (k, v): keys_table.index(k))) for item in table_dict]
        keys = ['item' + str(n) for n in range(0,len(table))]
        table_json = dict(itertools.izip_longest(keys, table_dictord))
        table_jsonord = OrderedDict((k, table_json[k]) for k in keys)
        with open(args.json, 'ab') as f:
            json.dump(table_jsonord, f, indent=4)

# BMFBovespa Daily Quotes Crawler 

import psycopg2
import sys
import argparse
import csv
import itertools
import json
from urllib.request import urlopen
from zipfile import ZipFile, BadZipfile
from io import BytesIO
from datetime import date, timedelta
from collections import OrderedDict
from os.path import isfile

if __name__ == "__main__":

    # Define arguments
    tday = date.today()
    tday_url = "D" + tday.strftime("%d%m%Y")
    parser = argparse.ArgumentParser(
        prog="bovcrawler",
        description=("Downloads data from BMFBovespa historical quotes "
                     "and saves into a PostgreSQL table, a csv file or "
                     "a json file. You should add pSQL info in the "
                     ".dbsettings file."))
    parser.add_argument(
        "date", help=("date to download, e.g. A2000, M122015, "
                      "D17022016 (defaults to today)"),
        nargs='?', default=tday_url)
    parser.add_argument(
        "-d", "--db", help=("save data into a Postgres table named "
                            " bovquotes using settings in .dbsettings"),
        action="store_true")
    parser.add_argument(
        "-c", "--csv", help=("save data as a csv file, e.g., --csv "
                             "quotes.csv"))
    parser.add_argument(
        "-j", "--json", help=("save data as a json file, e.g., --json "
                              "quotes.json"))
    parser.add_argument("--to",
                        help=("save all data from first year/month to "
                              "the second, e.g., A2010 --to A2015 -d"))
    args = parser.parse_args()
    if not any((args.db, args.csv, args.json)):
        parser.print_help()
        sys.exit(0)

    # Define dates (limit possible values)
    if args.date[0] not in ['A', 'M', 'D']:
        print("Error: dates should start with A, M or D, e.g., D17022016")
        sys.exit(1)
    if args.to:
        if not (args.to[0] == args.date[0] or args.to[0] in ['A', 'M']):
            print("Error: TO date should start with A or M, according "
                  "to first date")
            sys.exit(1)
        elif args.to[0] == 'A':
            if not (len(args.to[1:]) == 4 and args.to[1:].isdigit() and
                    args.date[1:] < args.to[1:] <= tday.strftime("%Y")):
                print("Error: invalid date interval")
                sys.exit(1)
            years = [str(y) for y in
                     range(int(args.date[1:]), int(args.to[1:]) + 1)]
            args.date = ['A' + year for year in years]
        elif args.to[0] == 'M':
            if not (len(args.to[1:]) == 6 and args.to[1:].isdigit() and
                    (args.date[3:] + args.date[1:3]) <
                    (args.to[3:] + args.to[1:3]) <=
                    tday.strftime("%Y%m")):
                print("Error: invalid date interval")
                sys.exit(1)
            years = [str(y) for y in
                     range(int(args.date[3:]), int(args.to[3:]) + 1)]
            months = ["%.2d" % i for i in range(1, 13)]
            alldates = [month + year for year in years for
                        month in months]
            minit = alldates.index(args.date[1:])
            mend = alldates.index(args.to[1:])
            args.date = ['M' + m for m in alldates[minit:(mend + 1)]]
    else:
        args.date = [args.date]

    # Start loop through dates
    table = []
    keys_table = ['TIPREG', 'DATAPREG', 'CODDBI', 'CODNEG', 'TPMERC',
                  'NOMRES', 'ESPECI', 'PRAZOT', 'MODREF', 'PREABE',
                  'PREMAX', 'PREMIN', 'PREMED', 'PREULT', 'PREOFC',
                  'PREOFV', 'TOTNEG', 'QUATOT', 'VOLTOT', 'PREEXE',
                  'INDOPC', 'DATVEN', 'FATCOT', 'PTOEXE', 'CODISI',
                  'DISMES']
    for dateurl in args.date:    

        # Download file
        print("Working on", dateurl, "...")
        try:
            print("Downloading file...")
            response = urlopen(
                "http://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
                "COTAHIST_" + dateurl + ".ZIP")
            zipfile = ZipFile(BytesIO(response.read()))
            txtfile = zipfile.namelist()[0]
            data = zipfile.open(txtfile).readlines()
            print("[DONE]")
        except (IOError, BadZipfile) as e:
            print('Error: %s', e)
            sys.exit(1)

        # Load data in right format
        print("Loading file...")
        data = data[1:(len(data)-1)]
        for line in data:
            line = line.decode('utf8')
            table.append((
                int(line[0:2]),
                line[2:6] + '-' + line[6:8] + '-' + line[8:10],
                " ".join(line[10:12].split()),
                " ".join(line[12:24].split()), int(line[24:27]),
                " ".join(line[27:39].split()),
                " ".join(line[39:49].split()),
                " ".join(line[49:52].split()),
                " ".join(line[52:56].split()), float(line[56:69])/100,
                float(line[69:82])/100, float(line[82:95])/100,
                float(line[95:108])/100, float(line[108:121])/100,
                float(line[121:134])/100, float(line[134:147])/100,
                int(line[147:152]), int(line[152:170]),
                float(line[170:188])/100, float(line[188:201])/100,
                int(line[201:202]),
                line[202:206] + '-' + line[206:208] + '-' +
                line[208:210], int(line[210:217]),
                float(line[217:230])/1000000,
                " ".join(line[230:242].split()), int(line[242:245])))
        print("[DONE]")

    # Save data in PostgreSQL table
    if args.db:
        print("Saving data into pSQL table 'bovquotes'...")
        if isfile(".dbsettings_1"):
            with open(".dbsettings_1", 'r') as f:
                dbset = f.read().replace('\n', ' ')
        else:
            try:
                with open(".dbsettings", 'r') as f:
                    dbset = f.read().replace('\n', ' ')
            except:
                print("Error: missing file .dbsettings")
                sys.exit(1)
        conn = None
        try:
            conn = psycopg2.connect(dbset)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS bovquotes ("
                "TIPREG INTEGER, DATAPREG DATE, CODDBI VARCHAR(2), "
                "CODNEG VARCHAR(12), TPMERC INTEGER, "
                "NOMRES VARCHAR(12), ESPECI VARCHAR(10), "
                "PRAZOT VARCHAR(3), MODREF VARCHAR(4), "
                "PREABE DECIMAL(13,2), PREMAX DECIMAL(13,2), "
                "PREMIN DECIMAL(13,2), PREMED DECIMAL(13,2), "
                "PREULT DECIMAL(13,2), PREOFC DECIMAL(13,2), "
                "PREOFV DECIMAL(13,2), TOTNEG INTEGER, QUATOT BIGINT, "
                "VOLTOT DECIMAL(18,2), PREEXE DECIMAL(13,2), "
                "INDOPC INTEGER, DATVEN DATE, FATCOT INTEGER, "
                "PTOEXE DECIMAL(13,6), CODISI VARCHAR(12), "
                "DISMES INTEGER, "
                "PRIMARY KEY(TPMERC, CODDBI, CODNEG, DATAPREG, PRAZOT))")
            args_str = ','.join(cur.mogrify(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                x) for x in table)
            cur.execute("INSERT INTO bovquotes (" +
                        ", ".join(keys_table) + ") VALUES " + args_str)
            conn.commit()
            print("[DONE]")
        except psycopg2.DatabaseError as e:
            if conn is not None:
                conn.rollback()
            print('Error: %s', e)
            sys.exit(1)
        finally:
            if conn is not None:
                conn.close()

    # Save data in csv file
    if args.csv:
        print("Saving data in '", args.csv, "'...")
        table = [tuple(keys_table)] + table
        try:
            with open(args.csv, 'a') as f:
                writer = csv.writer(f)
                writer.writerows(table)
            print("[DONE]")
        except IOError as e:
            print('Error: %s', e)
            sys.exit(1)

    # Save data in json file
    if args.json:
        print("Saving data in '", args.json, "'...")
        table_dict = [dict(itertools.izip_longest(keys_table, t))
                      for t in table]
        table_dictord = [OrderedDict(sorted(
            item.iteritems(),
            key=lambda kv: keys_table.index(kv[0]))) for
                         item in table_dict]
        keys = ['item' + str(n) for n in range(0,len(table))]
        table_json = dict(
            itertools.izip_longest(keys, table_dictord))
        table_jsonord = OrderedDict((k, table_json[k]) for k in keys)
        try:
            with open(args.json, 'ab') as f:
                json.dump(table_jsonord, f, indent=4)
            print("[DONE]")
        except IOError as e:
            print('Error: %s', e)

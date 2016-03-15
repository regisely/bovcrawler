#!/bin/bash
python bovcrawler.py $(date -d "yesterday 13:00 " '+D%d%m%Y') --csv quotes.csv &> /dev/null
cat quotes.csv | grep $1, | cut -d ',' -f4,14 | sed s/,/' '/g
rm quotes.csv

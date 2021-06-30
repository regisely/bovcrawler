#!/bin/bash

if python bovcrawler.py $(date -d "yesterday 13:00" '+D%d%m%Y') --csv quotes.csv &> /dev/null || python bovcrawler.py $(date -d "2 days ago 13:00 " '+D%d%m%Y') --csv quotes.csv &> /dev/null || python bovcrawler.py $(date -d "3 days ago 13:00 " '+D%d%m%Y') --csv quotes.csv &> /dev/null || python bovcrawler.py $(date -d "4 days ago 13:00 " '+D%d%m%Y') --csv quotes.csv &> /dev/null; then
    cat quotes.csv | grep $1, | cut -d ',' -f4,14 | sed s/,/' '/g
    rm quotes.csv
else
    echo error
fi

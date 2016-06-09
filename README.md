# bovcrawler

*bovcrawler* is a crawler for BM&FBOVESPA daily equity prices, volume and other relevant data. It is written in Python 2.7. You can clone the repository by running:

```shell
git clone https://github.com/regisely/bovcrawler.git
```

Then you can have a list of the arguments for the `bovcrawler.py` script with:

```shell
python bovcrawler.py
```

The first argument is the date, which can be specified as a day (D17022016), a full month (M122015), a full year (A2000) or multiple months/years (A2010 --to A2015). Note that dates are in the Brazilian format (%d%m%Y).

The second argument is the format which the data is saved: -d or --db to save in a Postgres table; "-c file.csv" or "--csv file.csv" to save in a csv file; and "-j file.json" or "--json file.json" to save in a JSON file. The Postgres credentials are specified in the .dbsettings file. Note that data from all the equities of BM&FBOVESPA from the specified period of time will be downloaded.

An example of usage of `bovcrawler` is above (note that this will create a file called "quotes.csv" with all the data downloaded):

```shell
python bovcrawler.py D17022016 --csv quotes.csv
```

If you want to check the closing price from yesterday of a particular equity you can use a shell script for Linux called `getquote.sh` (Note that if the market was closed yesterday this script will throw an error):

```shell
./getquote.sh PETR4
```

Alternatively, there is a version of this script that uses a GUI:

```shell
python bov_gui.py
```

While the scripts `getquote.sh` and `bov_gui.py` will only work in Linux, the crawler `bovcrawler.py` should work in other systems as well.

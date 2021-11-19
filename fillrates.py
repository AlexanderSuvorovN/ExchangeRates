#
# https://blog.arturofm.com/install-python-3-7-on-ubuntu-16-04/
# https://stackoverflow.com/questions/49817590/installing-pip-on-ubuntu-16-04
# https://stackoverflow.com/questions/51468059/mysql-package-for-python-3-7
# https://www.geeksforgeeks.org/how-to-install-requests-in-python-for-windows-linux-mac/
# pip install requests bs4
# chmod ugo+x fillrates.py
# sudo nano /etc/crontab
#
# m h dom mon dow user command
# 0 7 * * * root python3.7 /var/www/www-root/data/www/fx.ekonom.spb.ru/utils/fillrates.py
#
# sudo service cron restart 
#
# USD / EUR
# https://www.exchangerates.org.uk/EUR-USD-exchange-rate-history.html
# https://www.exchangerates.org.uk/USD-EUR-exchange-rate-history.html
# 
# RUB / EUR
# https://www.exchangerates.org.uk/EUR-RUB-exchange-rate-history.html
# https://www.exchangerates.org.uk/RUB-EUR-exchange-rate-history.html
#
import time
import csv
import mysql.connector
import re
import requests
from bs4 import BeautifulSoup
import datetime
import calendar
#
#
#
def parseRate(currency, direction):
	page = requests.get(rates[currency]['url_'+direction])
	soup = BeautifulSoup(page.content, 'html.parser')
	table = soup.find(id='hist')
	table_rows = table.find_all('tr')
	stop = False
	for row in table_rows:
		if len(row.find_all('td')) == 3:
			row_cells = row.contents
			x = re.search('^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+([0-9]{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s([0-9]{4})$', row_cells[0].string.strip())
			if x != None:
				weekday = x[1].strip()
				day = int(x[2].strip())
				month = int(list(calendar.month_name).index(x[3].strip()))
				year = int(x[4].strip())
				date = datetime.datetime(year, month, day)
				if date <= rates[currency]['db_last_date']:
					break
			if direction == 'forward':
				x_from = rates[currency]['x_from'].upper()
				x_to = 'EUR'
			else:
				x_from = 'EUR'
				x_to = rates[currency]['x_from'].upper()
			pattern = '^'+str(int(round(rates[currency]['unit'], 0)))+'\s+'+x_to+'\s+=\s+([0-9]+\.[0-9]+)\s+'+x_from+'$';
			x = re.search(pattern, row_cells[1].string.strip())
			if x != None:
				rate = float(x[1])
			else:
				rate = None
			ix = date.strftime('%Y-%m-%d')
			if ix not in rates[currency]['data']:
				rates[currency]['data'][ix] = {}
			rates[currency]['data'][ix][direction+'_rate'] = rate;
#
#
#
def parseCurrency(currency):
	parseRate(currency, 'forward')
	parseRate(currency, 'reverse')
#
# 
# 
def updateCurrency(currency):
	for key in rates[currency]['data']:
		fxrec = {}
		fxrec['date'] = key
		fxrec['x_from'] = rates[currency]['x_from'].upper()
		fxrec['x_to'] = rates[currency]['x_to'].upper()
		fxrec['unit'] = rates[currency]['unit']
		fxrec['forward_rate'] = rates[currency]['data'][key]['forward_rate']
		fxrec['reverse_rate'] = rates[currency]['data'][key]['reverse_rate']
		query = "INSERT IGNORE INTO `fx_rates` (`date`, `x_from`, `x_to`, `unit`, `forward_rate`, `reverse_rate`) VALUES(%(date)s, %(x_from)s, %(x_to)s, %(unit)s, %(forward_rate)s, %(reverse_rate)s)"
		cursor.execute(query, fxrec)
		cnx.commit()
#
# MAIN
#
date_today = datetime.datetime.now()
print("Connecting to database...")
try:
	cnx = mysql.connector.connect(
		host="localhost",
		database="ekonom",
		user="ekonom",
		passwd="bV7vQ3yO0fwB7l")
except mysql.connector.Error as err:
	if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
		print("Something is wrong with your user name or password")
	elif err.errno == errorcode.ER_BAD_DB_ERROR:
		print("Database does not exist")
	else:
		print(err)
cursor = cnx.cursor(dictionary=True)
#
# Preliminary configuration
currencies = ['usd', 'rub', 'eur']
rates = {}
# USD
rates['usd'] = {}
rates['usd']['url_forward'] = 'https://www.exchangerates.org.uk/EUR-USD-exchange-rate-history.html'
rates['usd']['url_reverse'] = 'https://www.exchangerates.org.uk/USD-EUR-exchange-rate-history.html'
rates['usd']['x_from'] = 'usd'
rates['usd']['x_to'] = 'eko'
rates['usd']['unit'] = 1.0000
rates['usd']['data'] = {};
# RUB
rates['rub'] = {}
rates['rub']['url_forward'] = 'https://www.exchangerates.org.uk/EUR-RUB-exchange-rate-history.html'
rates['rub']['url_reverse'] = 'https://www.exchangerates.org.uk/RUB-EUR-exchange-rate-history.html'
rates['rub']['x_from'] = 'rub'
rates['rub']['x_to'] = 'eko'
rates['rub']['unit'] = 1.0000
rates['rub']['data'] = {};
# EUR
rates['eur'] = {}
rates['eur']['url_forward'] = None
rates['eur']['url_reverse'] = None
rates['eur']['x_from'] = 'eur'
rates['eur']['x_to'] = 'eko'
rates['eur']['unit'] = 1.0000
rates['eur']['data'] = {};
#
# Get latest dates for currencies
for currency in currencies:
	query = "SELECT `id`, `date`, x_from, x_to, unit, forward_rate, reverse_rate FROM `fx_rates` WHERE `x_from` = '"+currency.upper()+"' ORDER BY `date` DESC LIMIT 1"
	cursor.execute(query)
	fetch = cursor.fetchone()
	rates[currency]['db_last_date'] = fetch['date']
#
# Parse data
print(f'Parsing USD rates...')
parseCurrency('usd')
print(f'Parsing RUB rates...')
parseCurrency('rub')
print(f'Parsing complete.')
#
# Debug
#print(rates['usd']['db_last_date'])
#print(rates['rub']['db_last_date'])
#print(rates['usd']['data'])
#print(rates['rub']['data'])
#
# Generate EUR records if necessary
if date_today >= rates['eur']['db_last_date']:
	print(f'Got to update EUR records...')
	date = rates['eur']['db_last_date']
	# https://stackoverflow.com/questions/151199/how-to-calculate-number-of-days-between-two-given-dates
	for x in range(1, (date_today - rates['eur']['db_last_date']).days):
		# https://stackoverflow.com/questions/3240458/how-to-increment-a-datetime-by-one-day
		date += datetime.timedelta(days=1)
		ix = date.strftime('%Y-%m-%d')
		rates['eur']['data'][ix] = {}
		rates['eur']['data'][ix]['forward_rate'] = 1.0000;
		rates['eur']['data'][ix]['reverse_rate'] = 1.0000;
#
# Update database if necessary
for currency in currencies:
	count = len(rates[currency]['data'])
	if count > 0:
		print(f'Updating database for {currency.upper()} rates ({count})...')
		updateCurrency(currency)
	else:
		print(f'There are no {currency.upper()} rates to update into database.')
cursor.close()
cnx.close()
print('Complete.')
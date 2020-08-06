#!/usr/bin/env python
# coding: utf-8



print('импорт пакетов')
import pandas as pd
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
from datetime import datetime
from datetime import date, timedelta
import time
import numpy as np
from datetime import datetime as dt
import pandas.io.formats.format as pf
import contextlib
import os
import csv
import calendar


print('настрйоки дат')

import datetime
lastday = datetime.datetime.now()
lastday = lastday - timedelta(days=1)
lastday7 = lastday - timedelta(days=7)
lastday30 = lastday - timedelta(days=30)
lastday0 = datetime.datetime.now()
lastdayy = lastday.year
lastdaym = lastday.month
lastday =  lastday.strftime("%Y-%m-%d")
lastday7 =  lastday7.strftime("%Y-%m-%d")
lastday30 =  lastday30.strftime("%Y-%m-%d")
lastday0 =  lastday0.strftime("%Y-%m-%d")
if lastdaym<10:
    lastdayym = str(lastdayy)+ '-0'+str(lastdaym)
else:
    lastdayym = str(lastdayy)+'-' +str(lastdaym)


print('загрузка данных crm')

start_time = dt.now()
crm = pd.read_csv('crm.csv', sep=';', encoding='cp1251', header=0)
crm['CampaignName'] = crm['CampaignName'].str.lower()
crm['goal'] = crm['goal'].astype(int)
crm['transactions'] = crm['transactions'].astype(int)
crm['transaction_revenue'] = crm['transaction_revenue'].astype(float)
savedate = crm.sort_values('Date', ascending=True).reset_index(drop=True)['Date'][0]
crm = crm[crm.Date.str.contains(savedate)==False]
print(dt.now() - start_time)
crm.head()




print('загрузка данных ga')


start_time = dt.now()
gad = pd.read_csv('ga_new.csv', sep=';', encoding='utf8', header=0)
gad = gad[gad.SourceMedium.str.contains('google / cpc')==True]
gad = gad.groupby(['Date', 'CampaignName', 'Device', 'источник', 'поиск/сеть', 'продукт']).sum().reset_index()[['Date', 'CampaignName', 'Device', 'источник', 'поиск/сеть', 'продукт','gaimpressions','gaClicks','gaCost']].sort_values('Date', ascending=False)
gad['Device'] = gad['Device'].str.lower()
gad['CampaignName'] = gad['CampaignName'].str.lower()
gad['Date'] = gad['Date'].replace(to_replace =' 00:00:00', value ='', regex =True)
gad['CampaignName'] = gad['CampaignName'].replace(to_replace ='-flayt', value ='', regex =True)
gad['Device'] = gad['Device'].str.lower()
gad['Cost'] = (gad['gaCost']*1.2*1.1* 100).astype(int) / 100
gad.columns = gad.columns.str.replace('gaClicks','Clicks')

del gad['gaimpressions']
del gad['gaCost']
print(dt.now() - start_time)
gad.head()


print('загрузка данных директ')


start_time = dt.now()
yad = pd.read_csv('cashe_new.csv', sep=';', encoding='cp1251', header=0)
yad = yad.groupby(['Date', 'CampaignName', 'Device', 'источник', 'поиск/сеть', 'продукт']).sum().reset_index()[['Date', 'CampaignName', 'Device', 'источник', 'поиск/сеть', 'продукт','Impressions' , 'Clicks','Cost']].sort_values('Date', ascending=False)
yad['Device'] = yad['Device'].str.lower()
yad['CampaignName'] = yad['CampaignName'].str.lower()
print(dt.now() - start_time)
yad.head()


print('яндекс+ga+crm)


start_time = dt.now()
joined = yad.append(gad, ignore_index=False)
joined = joined.append(crm, ignore_index=False)
print(dt.now() - start_time)
joined.head(50)


print('обработка дат')


def convert_to_datetime(row):
    return dt.strptime(row['Date'], '%Y-%m-%d')


# In[9]:


start_time = dt.now()
joined = joined[joined.CampaignName.str.contains(r'rko', case=False)==False]
joined = joined[joined.CampaignName.str.contains(r'stream', case=False)==False]
joined = joined[joined.CampaignName.str.contains(r'РКО', case=False)==False]
joined = joined[joined.Date.str.contains(lastday0)==False]
joined['datetime'] = joined.apply(convert_to_datetime, axis=1)
joined['year'] = joined['datetime'].dt.year 
joined['month'] = joined['datetime'].dt.month 
joined['day'] = joined['datetime'].dt.day
joined['week'] = joined['datetime'].dt.week
joined['dayweek'] = joined['datetime'].dt.weekday
joined = joined.replace(np.nan, '0', regex=True)
joined['Clicks']=(joined['Clicks']*100).astype(int)/100
joined['transaction_revenue']=joined['transaction_revenue'].astype(int)
joined['Cost']=(joined['Cost']*100).astype(int)/100
joined['goal']=joined['goal'].astype(int)
joined['transactions']=joined['transactions'].astype(int)
joined['cpc'] = (joined['Cost'] / (joined['Clicks']+ 0.00000001)* 100).astype(int) / 100
joined['cr'] = (joined['goal'] / (joined['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joined['cpo'] = (joined['Cost'] / joined['goal']).replace((np.inf, -np.inf, np.nan), (joined['Cost'], joined['Cost'], joined['Cost'])).astype(int)
joined['cps'] = (joined['Cost'] / joined['transactions']*100).replace((np.inf, -np.inf, np.nan), (joined['Cost'], joined['Cost'], joined['Cost'])).astype(int)/100
joined['vps'] = ((joined['Cost'] / joined['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joined.to_csv('joined.csv', index=False, header=True, sep=';', encoding='cp1251')
joined.columns = joined.columns.str.replace('поиск/сеть','пс')
joined = joined[(joined['Date'] >= '2020-03-01')]
joined['продукт'] = np.where(joined['CampaignName'].str.contains("koronavirus", case=False, na=False), 'коронавирус', joined['продукт'])

print(dt.now() - start_time)
joined.head()


print('разделение рк на типы')


start_time = dt.now()
new_joined = joined.CampaignName.str.split('_', 0, expand=True)
joined['гео']= new_joined[1]
joined['тип']= new_joined[3]
joined['потип']= new_joined[4]
print(dt.now() - start_time)
joined.head()


print('статистика по дням')

start_time = dt.now()
joinedforday = joined
pd.options.display.float_format = '{:0,.2f}'.format
joinedforday['dayweek']=joinedforday['dayweek'].astype(str)
joinedforday = joinedforday.groupby(['Date', 'dayweek']).sum().reset_index()[['Date','dayweek', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Date', ascending=False).reset_index(drop=True)
joinedforday['cpc'] = (joinedforday['Cost'] / (joinedforday['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedforday['cr'] = (joinedforday['goal'] / (joinedforday['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedforday['cpo'] = (joinedforday['Cost'] / joinedforday['goal']).replace((np.inf, -np.inf, np.nan), (joinedforday['Cost'], joinedforday['Cost'], joinedforday['Cost'])).astype(int)
joinedforday['cps'] = (joinedforday['Cost'] / joinedforday['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedforday['Cost'], joinedforday['Cost'], joinedforday['Cost'])).astype(int)/100
joinedforday['vps'] = ((joinedforday['Cost'] / joinedforday['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedforday.cpo=(np.floor(joinedforday.cpo*100)/100).map('{:,.0f}'.format)
joinedforday.vps=(np.floor(joinedforday.vps*100)/100).map('{:,.0f}'.format)
joinedforday.cps=(np.floor(joinedforday.cps*100)/100).map('{:,.0f}'.format)
joinedforday.transactions=(np.floor(joinedforday.transactions*100)/100).map('{:,.0f}'.format)
joinedforday.transaction_revenue=(np.floor(joinedforday.transaction_revenue)).map('{:,.0f}'.format)
joinedforday.Clicks=(np.floor(joinedforday.Clicks*100)/100).map('{:,.0f}'.format)
joinedforday.Cost=(np.floor(joinedforday.Cost*100)/100).map('{:,.2f}'.format)
joinedforday.cr=(np.floor(joinedforday.cr*100)/100).map('{:,.2f}'.format)
joinedforday.goal=(np.floor(joinedforday.goal*100)/100).map('{:,.0f}'.format)

joinedforday.to_csv('joinedforday.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedforday.head(14)


print('данные по яндекс')


start_time = dt.now()
joinedfordayyandex = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordayyandex = joinedfordayyandex[joinedfordayyandex.источник.str.contains(r'yandex', case=False)==True]
joinedfordayyandex['dayweek']=joinedfordayyandex['dayweek'].astype(str)
joinedfordayyandex = joinedfordayyandex.groupby(['Date', 'dayweek']).sum().reset_index()[['Date','dayweek', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Date', ascending=False).reset_index(drop=True)
joinedfordayyandex['cpc'] = (joinedfordayyandex['Cost'] / (joinedfordayyandex['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordayyandex['cr'] = (joinedfordayyandex['goal'] / (joinedfordayyandex['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordayyandex['cpo'] = (joinedfordayyandex['Cost'] / joinedfordayyandex['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordayyandex['Cost'], joinedfordayyandex['Cost'], joinedfordayyandex['Cost'])).astype(int)
joinedfordayyandex['cps'] = (joinedfordayyandex['Cost'] / joinedfordayyandex['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordayyandex['Cost'], joinedfordayyandex['Cost'], joinedfordayyandex['Cost'])).astype(int) / 100
joinedfordayyandex['vps'] = ((joinedfordayyandex['Cost'] / joinedfordayyandex['transaction_revenue']).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int)/10
joinedfordayyandex.cpo=(np.floor(joinedfordayyandex.cpo*100)/100).map('{:,.0f}'.format)
joinedfordayyandex.vps=(np.floor(joinedfordayyandex.vps*100)/100).map('{:,.0f}'.format)
joinedfordayyandex.cps=(np.floor(joinedfordayyandex.cps*100)/100).map('{:,.0f}'.format)
joinedfordayyandex.transactions=(np.floor(joinedfordayyandex.transactions*100)/100).map('{:,.0f}'.format)
joinedfordayyandex.transaction_revenue=(np.floor(joinedfordayyandex.transaction_revenue)).map('{:,.0f}'.format)
joinedfordayyandex.Clicks=(np.floor(joinedfordayyandex.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordayyandex.Cost=(np.floor(joinedfordayyandex.Cost*100)/100).map('{:,.2f}'.format)
joinedfordayyandex.cr=(np.floor(joinedfordayyandex.cr*100)/100).map('{:,.2f}'.format)
joinedfordayyandex.goal=(np.floor(joinedfordayyandex.goal*100)/100).map('{:,.0f}'.format)

joinedfordayyandex.to_csv('joinedfordayyandex.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordayyandex.head(14)


print('данные по гугл')


start_time = dt.now()
joinedfordaygoogle = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaygoogle = joinedfordaygoogle[joinedfordaygoogle.источник.str.contains(r'google', case=False)==True]
joinedfordaygoogle['dayweek']=joinedfordaygoogle['dayweek'].astype(str)
joinedfordaygoogle = joinedfordaygoogle.groupby(['Date', 'dayweek']).sum().reset_index()[['Date','dayweek', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Date', ascending=False).reset_index(drop=True)
joinedfordaygoogle['cpc'] = (joinedfordaygoogle['Cost'] / (joinedfordaygoogle['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaygoogle['cr'] = (joinedfordaygoogle['goal'] / (joinedfordaygoogle['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaygoogle['cpo'] = (joinedfordaygoogle['Cost'] / joinedfordaygoogle['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaygoogle['Cost'], joinedfordaygoogle['Cost'], joinedfordaygoogle['Cost'])).astype(int)
joinedfordaygoogle['cps'] = (joinedfordaygoogle['Cost'] / joinedfordaygoogle['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaygoogle['Cost'], joinedfordaygoogle['Cost'], joinedfordaygoogle['Cost'])).astype(int)/100
joinedfordaygoogle['vps'] = ((joinedfordaygoogle['Cost'] / joinedfordaygoogle['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaygoogle.cpo=(np.floor(joinedfordaygoogle.cpo)).map('{:,.0f}'.format)
joinedfordaygoogle.vps=(np.floor(joinedfordaygoogle.vps*100)/100).map('{:,.0f}'.format)
joinedfordaygoogle.cps=(np.floor(joinedfordaygoogle.cps)).map('{:,.0f}'.format)
joinedfordaygoogle.transactions=(np.floor(joinedfordaygoogle.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaygoogle.transaction_revenue=(np.floor(joinedfordaygoogle.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaygoogle.Clicks=(np.floor(joinedfordaygoogle.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaygoogle.Cost=(np.floor(joinedfordaygoogle.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaygoogle.cr=(np.floor(joinedfordaygoogle.cr*100)/100).map('{:,.2f}'.format)
joinedfordaygoogle.goal=(np.floor(joinedfordaygoogle.goal*100)/100).map('{:,.0f}'.format)

joinedfordaygoogle.to_csv('joinedfordaygoogle.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordaygoogle.head(14)


print('данные по источникам')


thismonth = datetime.datetime.now()
thismonth = thismonth - timedelta(days=1)
thismonth = thismonth.strftime("%Y-%m-01")
thismonth




start_time = dt.now()
joinedforsource = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedforsource = joinedforsource[(joinedforsource['Date'] >= thismonth)]
joinedforsource['dayweek']=joinedforsource['dayweek'].astype(str)
joinedforsource = joinedforsource.groupby(['источник']).sum().reset_index()[['источник', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('источник', ascending=False).reset_index(drop=True)
joinedforsource['cpc'] = (joinedforsource['Cost'] / (joinedforsource['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedforsource['cr'] = (joinedforsource['goal'] / (joinedforsource['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedforsource['cpo'] = (joinedforsource['Cost'] / joinedforsource['goal']).replace((np.inf, -np.inf, np.nan), (joinedforsource['Cost'], joinedforsource['Cost'], joinedforsource['Cost'])).astype(int)
joinedforsource['cps'] = (joinedforsource['Cost'] / joinedforsource['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedforsource['Cost'], joinedforsource['Cost'], joinedforsource['Cost'])).astype(int)/100
joinedforsource['vps'] = ((joinedforsource['Cost'] / joinedforsource['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedforsource.cpo=(np.floor(joinedforsource.cpo)).map('{:,.0f}'.format)
joinedforsource.vps=(np.floor(joinedforsource.vps*100)/100).map('{:,.0f}'.format)
joinedforsource.cps=(np.floor(joinedforsource.cps)).map('{:,.0f}'.format)
joinedforsource.transactions=(np.floor(joinedforsource.transactions*100)/100).map('{:,.0f}'.format)
joinedforsource.transaction_revenue=(np.floor(joinedforsource.transaction_revenue)).map('{:,.0f}'.format)
joinedforsource.Clicks=(np.floor(joinedforsource.Clicks*100)/100).map('{:,.0f}'.format)
joinedforsource.Cost=(np.floor(joinedforsource.Cost*100)/100).map('{:,.2f}'.format)
joinedforsource.cr=(np.floor(joinedforsource.cr*100)/100).map('{:,.2f}'.format)
joinedforsource.goal=(np.floor(joinedforsource.goal*100)/100).map('{:,.0f}'.format)

joinedforsource.to_csv('joinedforsource.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedforsource.head(14)


print('данные по продуктам')

start_time = dt.now()
joinedfordayprod = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordayprod = joinedfordayprod[joinedfordayprod.Date.str.contains(lastday, case=False)==True]
joinedfordayprod['dayweek']=joinedfordayprod['dayweek'].astype(str)
joinedfordayprod = joinedfordayprod.groupby(['Date', 'продукт']).sum().reset_index()[['Date','продукт', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Date', ascending=False).reset_index(drop=True)
joinedfordayprod['cpc'] = (joinedfordayprod['Cost'] / (joinedfordayprod['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordayprod['cr'] = (joinedfordayprod['goal'] / (joinedfordayprod['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordayprod['cpo'] = (joinedfordayprod['Cost'] / joinedfordayprod['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordayprod['Cost'], joinedfordayprod['Cost'], joinedfordayprod['Cost'])).astype(int)
joinedfordayprod['cps'] = (joinedfordayprod['Cost'] / joinedfordayprod['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordayprod['Cost'], joinedfordayprod['Cost'], joinedfordayprod['Cost'])).astype(int)/100
joinedfordayprod['vps'] = ((joinedfordayprod['Cost'] / joinedfordayprod['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordayprod.cpo=(np.floor(joinedfordayprod.cpo)).map('{:,.0f}'.format)
joinedfordayprod.vps=(np.floor(joinedfordayprod.vps*100)/100).map('{:,.0f}'.format)
joinedfordayprod.cps=(np.floor(joinedfordayprod.cps)).map('{:,.0f}'.format)
joinedfordayprod.transactions=(np.floor(joinedfordayprod.transactions*100)/100).map('{:,.0f}'.format)
joinedfordayprod.transaction_revenue=(np.floor(joinedfordayprod.transaction_revenue)).map('{:,.0f}'.format)
joinedfordayprod.Clicks=(np.floor(joinedfordayprod.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordayprod.Cost=(np.floor(joinedfordayprod.Cost*100)/100).map('{:,.2f}'.format)
joinedfordayprod.cr=(np.floor(joinedfordayprod.cr*100)/100).map('{:,.2f}'.format)
joinedfordayprod.goal=(np.floor(joinedfordayprod.goal*100)/100).map('{:,.0f}'.format)





joinedfordaycampaign30day = joined
joinedfordaycampaign30day = joinedfordaycampaign30day[(joinedfordaycampaign30day['Date'] >= thismonth)]
pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaycampaign30day['dayweek']=joinedfordaycampaign30day['dayweek'].astype(str)
joinedfordaycampaign30day = joinedfordaycampaign30day.groupby(['продукт']).sum().reset_index()[['продукт', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Cost', ascending=False).reset_index(drop=True)
joinedfordaycampaign30day['cpc'] = (joinedfordaycampaign30day['Cost'] / (joinedfordaycampaign30day['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaycampaign30day['cr'] = (joinedfordaycampaign30day['goal'] / (joinedfordaycampaign30day['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaycampaign30day['cpo'] = (joinedfordaycampaign30day['Cost'] / joinedfordaycampaign30day['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign30day['Cost'], joinedfordaycampaign30day['Cost'], joinedfordaycampaign30day['Cost'])).astype(int)
joinedfordaycampaign30day['cps'] = (joinedfordaycampaign30day['Cost'] / joinedfordaycampaign30day['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign30day['Cost'], joinedfordaycampaign30day['Cost'], joinedfordaycampaign30day['Cost'])).astype(int)/100
joinedfordaycampaign30day['vps'] = ((joinedfordaycampaign30day['Cost'] / joinedfordaycampaign30day['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaycampaign30day.cpo=(np.floor(joinedfordaycampaign30day.cpo)).map('{:,.0f}'.format)
joinedfordaycampaign30day.vps=(np.floor(joinedfordaycampaign30day.vps*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign30day.cps=(np.floor(joinedfordaycampaign30day.cps)).map('{:,.0f}'.format)
joinedfordaycampaign30day.transactions=(np.floor(joinedfordaycampaign30day.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign30day.transaction_revenue=(np.floor(joinedfordaycampaign30day.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaycampaign30day.Clicks=(np.floor(joinedfordaycampaign30day.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign30day.Cost=(np.floor(joinedfordaycampaign30day.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign30day.cr=(np.floor(joinedfordaycampaign30day.cr*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign30day.goal=(np.floor(joinedfordaycampaign30day.goal*100)/100).map('{:,.0f}'.format)

joinedfordayprod = joinedfordayprod.merge(joinedfordaycampaign30day, on=['продукт'] , how='outer')

joinedfordayprod.drop(["transactions_x", 'transaction_revenue_x', 'vps_x'], axis = 1, inplace = True)

 



joinedfordayprod.to_csv('joinedfordayprod.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordayprod.head(14)


print('не классифицируемые данные')


start_time = dt.now()
joinedfordaynd = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaynd = joinedfordaynd[joinedfordaynd.Date.str.contains(lastday, case=False)==True]
joinedfordaynd = joinedfordaynd[joinedfordaynd.продукт.str.contains('Брендовые', case=False)==False]
joinedfordaynd = joinedfordaynd[joinedfordaynd.продукт.str.contains('НД', case=False)==True]
joinedfordaynd['dayweek']=joinedfordaynd['dayweek'].astype(str)
joinedfordaynd = joinedfordaynd.groupby(['источник', 'CampaignName']).sum().reset_index()[['источник','CampaignName', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Cost', ascending=False).reset_index(drop=True)
joinedfordaynd['cpc'] = (joinedfordaynd['Cost'] / (joinedfordaynd['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaynd['cr'] = (joinedfordaynd['goal'] / (joinedfordaynd['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaynd['cpo'] = (joinedfordaynd['Cost'] / joinedfordaynd['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaynd['Cost'], joinedfordaynd['Cost'], joinedfordaynd['Cost'])).astype(int)
joinedfordaynd['cps'] = (joinedfordaynd['Cost'] / joinedfordaynd['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaynd['Cost'], joinedfordaynd['Cost'], joinedfordaynd['Cost'])).astype(int)/100
joinedfordaynd['vps'] = ((joinedfordaynd['Cost'] / joinedfordaynd['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaynd.cpo=(np.floor(joinedfordaynd.cpo)).map('{:,.0f}'.format)
joinedfordaynd.vps=(np.floor(joinedfordaynd.vps*100)/100).map('{:,.0f}'.format)
joinedfordaynd.cps=(np.floor(joinedfordaynd.cps)).map('{:,.0f}'.format)
joinedfordaynd.transactions=(np.floor(joinedfordaynd.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaynd.transaction_revenue=(np.floor(joinedfordaynd.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaynd.Clicks=(np.floor(joinedfordaynd.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaynd.Cost=(np.floor(joinedfordaynd.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaynd.cr=(np.floor(joinedfordaynd.cr*100)/100).map('{:,.2f}'.format)
joinedfordaynd.goal=(np.floor(joinedfordaynd.goal*100)/100).map('{:,.0f}'.format)

joinedfordaynd.to_csv('joinedfordaynd.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordaynd


print('данные по неделям')


start_time = dt.now()
joinedforweek = joined
pd.options.display.float_format = '{:0,.2f}'.format
joinedforweek['dayweek']=joinedforweek['dayweek'].astype(str)
joinedforweek = joinedforweek.groupby(['year', 'week']).sum().reset_index()[['year','week', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('week', ascending=False).reset_index(drop=True)
joinedforweek['cpc'] = (joinedforweek['Cost'] / (joinedforweek['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedforweek['cr'] = (joinedforweek['goal'] / (joinedforweek['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedforweek['cpo'] = (joinedforweek['Cost'] / joinedforweek['goal']).replace((np.inf, -np.inf, np.nan), (joinedforweek['Cost'], joinedforweek['Cost'], joinedforweek['Cost'])).astype(int)
joinedforweek['cps'] = (joinedforweek['Cost'] / joinedforweek['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedforweek['Cost'], joinedforweek['Cost'], joinedforweek['Cost'])).astype(int)/100
joinedforweek['vps'] = ((joinedforweek['Cost'] / joinedforweek['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedforweek.cpo=(np.floor(joinedforweek.cpo*100)/100).map('{:,.0f}'.format)
joinedforweek.cps=(np.floor(joinedforweek.cps*100)/100).map('{:,.0f}'.format)
joinedforweek.vps=(np.floor(joinedforweek.vps*100)/100).map('{:,.0f}'.format)
joinedforweek.transactions=(np.floor(joinedforweek.transactions*100)/100).map('{:,.0f}'.format)
joinedforweek.transaction_revenue=(np.floor(joinedforweek.transaction_revenue)).map('{:,.0f}'.format)
joinedforweek.Clicks=(np.floor(joinedforweek.Clicks*100)/100).map('{:,.0f}'.format)
joinedforweek.Cost=(np.floor(joinedforweek.Cost*100)/100).map('{:,.2f}'.format)
joinedforweek.cr=(np.floor(joinedforweek.cr*100)/100).map('{:,.2f}'.format)
joinedforweek.goal=(np.floor(joinedforweek.goal*100)/100).map('{:,.0f}'.format)

joinedforweek.to_csv('joinedforweek.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedforweek.head(14)


print('данные по месяцам')


start_time = dt.now()
joinedformonth = joined
pd.options.display.float_format = '{:0,.2f}'.format
joinedformonth['dayweek']=joinedformonth['dayweek'].astype(str)
joinedformonth = joinedformonth.groupby(['year', 'month']).sum().reset_index()[['year','month', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('month', ascending=False).reset_index(drop=True)
joinedformonth['cpc'] = (joinedformonth['Cost'] / (joinedformonth['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedformonth['cr'] = (joinedformonth['goal'] / (joinedformonth['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedformonth['cpo'] = (joinedformonth['Cost'] / joinedformonth['goal']).replace((np.inf, -np.inf, np.nan), (joinedformonth['Cost'], joinedformonth['Cost'], joinedformonth['Cost'])).astype(int)
joinedformonth['cps'] = (joinedformonth['Cost'] / joinedformonth['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedformonth['Cost'], joinedformonth['Cost'], joinedformonth['Cost'])).astype(int)/100
joinedformonth['vps'] = ((joinedformonth['Cost'] / joinedformonth['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedformonth.cpo=(np.floor(joinedformonth.cpo*100)/100).map('{:,.0f}'.format)
joinedformonth.cps=(np.floor(joinedformonth.cps*100)/100).map('{:,.0f}'.format)
joinedformonth.vps=(np.floor(joinedformonth.vps*100)/100).map('{:,.0f}'.format)
joinedformonth.transactions=(np.floor(joinedformonth.transactions*100)/100).map('{:,.0f}'.format)
joinedformonth.transaction_revenue=(np.floor(joinedformonth.transaction_revenue)).map('{:,.0f}'.format)
joinedformonth.Clicks=(np.floor(joinedformonth.Clicks*100)/100).map('{:,.0f}'.format)
joinedformonth.Cost=(np.floor(joinedformonth.Cost*100)/100).map('{:,.2f}'.format)
joinedformonth.cr=(np.floor(joinedformonth.cr*100)/100).map('{:,.2f}'.format)
joinedformonth.goal=(np.floor(joinedformonth.goal*100)/100).map('{:,.0f}'.format)

joinedformonth.to_csv('joinedformonth.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedformonth.head(14)


print('данные по кампаниям')


start_time = dt.now()
joinedfordaycampaign = joined

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaycampaign = joinedfordaycampaign[joinedfordaycampaign.продукт.str.contains('НД', case=False)==False]
joinedfordaycampaign = joinedfordaycampaign[joinedfordaycampaign.Date.str.contains(lastday, case=False)==True]
joinedfordaycampaign['dayweek']=joinedfordaycampaign['dayweek'].astype(str)
joinedfordaycampaign = joinedfordaycampaign.groupby(['источник', 'CampaignName']).sum().reset_index()[['источник','CampaignName', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Cost', ascending=False).reset_index(drop=True)
joinedfordaycampaign['cpc'] = (joinedfordaycampaign['Cost'] / (joinedfordaycampaign['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaycampaign['cr'] = (joinedfordaycampaign['goal'] / (joinedfordaycampaign['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaycampaign['cpo'] = (joinedfordaycampaign['Cost'] / joinedfordaycampaign['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'])).astype(int)
joinedfordaycampaign['cps'] = (joinedfordaycampaign['Cost'] / joinedfordaycampaign['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'])).astype(int)/100
joinedfordaycampaign['vps'] = ((joinedfordaycampaign['Cost'] / joinedfordaycampaign['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaycampaign.cpo=(np.floor(joinedfordaycampaign.cpo)).map('{:,.0f}'.format)
joinedfordaycampaign.vps=(np.floor(joinedfordaycampaign.vps*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.cps=(np.floor(joinedfordaycampaign.cps)).map('{:,.0f}'.format)
joinedfordaycampaign.transactions=(np.floor(joinedfordaycampaign.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.transaction_revenue=(np.floor(joinedfordaycampaign.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaycampaign.Clicks=(np.floor(joinedfordaycampaign.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.Cost=(np.floor(joinedfordaycampaign.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign.cr=(np.floor(joinedfordaycampaign.cr*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign.goal=(np.floor(joinedfordaycampaign.goal*100)/100).map('{:,.0f}'.format)


joinedfordaycampaign7day = joined
joinedfordaycampaign7day = joinedfordaycampaign7day[joinedfordaycampaign7day.продукт.str.contains('НД', case=False)==False]
joinedfordaycampaign7day = joinedfordaycampaign7day[(joinedfordaycampaign7day['Date'] > lastday7)]
pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaycampaign7day['dayweek']=joinedfordaycampaign7day['dayweek'].astype(str)
joinedfordaycampaign7day = joinedfordaycampaign7day.groupby(['источник', 'CampaignName']).sum().reset_index()[['источник','CampaignName', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Cost', ascending=False).reset_index(drop=True)
joinedfordaycampaign7day['cpc'] = (joinedfordaycampaign7day['Cost'] / (joinedfordaycampaign7day['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaycampaign7day['cr'] = (joinedfordaycampaign7day['goal'] / (joinedfordaycampaign7day['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaycampaign7day['cpo'] = (joinedfordaycampaign7day['Cost'] / joinedfordaycampaign7day['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign7day['Cost'], joinedfordaycampaign7day['Cost'], joinedfordaycampaign7day['Cost'])).astype(int)
joinedfordaycampaign7day['cps'] = (joinedfordaycampaign7day['Cost'] / joinedfordaycampaign7day['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign7day['Cost'], joinedfordaycampaign7day['Cost'], joinedfordaycampaign7day['Cost'])).astype(int)/100
joinedfordaycampaign7day['vps'] = ((joinedfordaycampaign7day['Cost'] / joinedfordaycampaign7day['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaycampaign7day.cpo=(np.floor(joinedfordaycampaign7day.cpo)).map('{:,.0f}'.format)
joinedfordaycampaign7day.vps=(np.floor(joinedfordaycampaign7day.vps*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign7day.cps=(np.floor(joinedfordaycampaign7day.cps)).map('{:,.0f}'.format)
joinedfordaycampaign7day.transactions=(np.floor(joinedfordaycampaign7day.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign7day.transaction_revenue=(np.floor(joinedfordaycampaign7day.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaycampaign7day.Clicks=(np.floor(joinedfordaycampaign7day.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign7day.Cost=(np.floor(joinedfordaycampaign7day.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign7day.cr=(np.floor(joinedfordaycampaign7day.cr*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign7day.goal=(np.floor(joinedfordaycampaign7day.goal*100)/100).map('{:,.0f}'.format)

joinedfordaycampaign = joinedfordaycampaign.merge(joinedfordaycampaign7day, on=['источник', 'CampaignName'] , how='outer')

joinedfordaycampaign.drop(["vps_x", "transactions_x", 'transaction_revenue_x'], axis = 1, inplace = True)

 

joinedfordaycampaign.to_csv(r'joinedfordaycampaign.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordaycampaign.head(51)


print('все данные')


start_time = dt.now()
joinedfordaycampaign = joined
joinedfordaycampaign['dayweek']=joinedfordaycampaign['dayweek'].astype(str)
joinedfordaycampaign['year']=joinedfordaycampaign['year'].astype(str)
joinedfordaycampaign['month']=joinedfordaycampaign['month'].astype(str)
joinedfordaycampaign['week']=joinedfordaycampaign['week'].astype(str)
joinedfordaycampaign['day']=joinedfordaycampaign['week'].astype(str)

pd.options.display.float_format = '{:0,.2f}'.format
joinedfordaycampaign['dayweek']=joinedfordaycampaign['dayweek'].astype(str)
joinedfordaycampaign = joinedfordaycampaign.groupby(['Date','year','month','week','day','dayweek','CampaignName','Device','источник','пс','продукт']).sum().reset_index()[['Date','year','month','week','day','dayweek','CampaignName','Device','источник','пс','продукт', 'Clicks','Cost','goal','transactions','transaction_revenue']].sort_values('Date', ascending=False).reset_index(drop=True)
joinedfordaycampaign['cpc'] = (joinedfordaycampaign['Cost'] / (joinedfordaycampaign['Clicks']+ 0.00000001)* 100).astype(int) / 100
joinedfordaycampaign['cr'] = (joinedfordaycampaign['goal'] / (joinedfordaycampaign['Clicks'])* 10000).replace((np.inf, -np.inf, np.nan), (0, 0, 0)).astype(int) / 100
joinedfordaycampaign['cpo'] = (joinedfordaycampaign['Cost'] / joinedfordaycampaign['goal']).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'])).astype(int)
joinedfordaycampaign['cps'] = (joinedfordaycampaign['Cost'] / joinedfordaycampaign['transactions']*100).replace((np.inf, -np.inf, np.nan), (joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'], joinedfordaycampaign['Cost'])).astype(int)/100
joinedfordaycampaign['vps'] = ((joinedfordaycampaign['Cost'] / joinedfordaycampaign['transaction_revenue']*10).replace((np.inf, -np.inf, np.nan), (0, 0, 0))* 10000).astype(int) / 100
joinedfordaycampaign.cpo=(np.floor(joinedfordaycampaign.cpo*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.vps=(np.floor(joinedfordaycampaign.vps*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.cps=(np.floor(joinedfordaycampaign.cps*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.transactions=(np.floor(joinedfordaycampaign.transactions*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.transaction_revenue=(np.floor(joinedfordaycampaign.transaction_revenue)).map('{:,.0f}'.format)
joinedfordaycampaign.Clicks=(np.floor(joinedfordaycampaign.Clicks*100)/100).map('{:,.0f}'.format)
joinedfordaycampaign.Cost=(np.floor(joinedfordaycampaign.Cost*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign.cr=(np.floor(joinedfordaycampaign.cr*100)/100).map('{:,.2f}'.format)
joinedfordaycampaign.goal=(np.floor(joinedfordaycampaign.goal*100)/100).map('{:,.0f}'.format)

joinedfordaycampaign.to_csv(r'joinedfordaycampaignall.csv', index=False, header=True, sep=';', encoding='cp1251')
print(dt.now() - start_time)
joinedfordaycampaign.head(14)


print('формирование рассылки')


import smtplib

from email.mime.text import MIMEText
from email.header import Header

# Настройки
mail_sender = 'xxx@gmail.com'
mail_receiver = ['123@yandex.ru', '123@gmail.com']

username = 'xxx@gmail.com'
password = 'xxx'
server = smtplib.SMTP('smtp.gmail.com:587')

# Формируем тело письма
subject = u'тест: Ежедевный отчет'

forday = """ <html> <head></head><p> динамика: <body> {0} </body> </html> """.format(joinedforday.head(14).to_html()) 
fordayy = """ <html> <head></head><p> динамика яндекс: <body> {0} </body> </html> """.format(joinedfordayyandex.head(14).to_html())
fordayg = """ <html> <head></head><p> динамика google: <body> {0} </body> </html> """.format(joinedfordaygoogle.head(14).to_html())
forweek = """ <html> <head></head><p> по неделям: <body> {0} </body> </html> """.format(joinedforweek.to_html())
formonth = """ <html> <head></head><p> по месяцам: <body> {0} </body> </html> """.format(joinedformonth.to_html())
forsource = """ <html> <head></head><p> по источникам за месяц: <body> {0} </body> </html> """.format(joinedforsource.to_html())
fortype = """ <html> <head></head><p> по продуктам за день/месяц: <body> {0} </body> </html> """.format(joinedfordayprod.head(20).to_html())
fornd = """ <html> <head></head><p> по НД кампаниям за вчера: <body> {0} </body> </html> """.format(joinedfordaynd.head(25).to_html())
forcampaign = """ <html> <head></head><p> по кампаниям за вчера\за 7 дней: <body> {0} </body> </html> """.format(joinedfordaycampaign.head(50).to_html())



msg = MIMEText(forday + fordayy + fordayg+ forweek+formonth + forsource + fortype + fornd +forcampaign, 'html') 
msg['Subject'] = Header(subject, 'utf-8')

# Отпавляем письмо
server.starttls()
server.ehlo()
server.login(username, password)
server.sendmail(mail_sender, mail_receiver, msg.as_string())
server.quit()







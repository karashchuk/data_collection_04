from pprint import pprint
import requests
import re
from lxml import html
from datetime import datetime
import pandas as pd
import time
from pymongo import MongoClient
import json


headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'}
news_dict = []

#MAIL.RU

main_link = 'https://news.mail.ru'
response = requests.get(main_link).text
root = html.fromstring(response)
#создаем список ссылок на новости 
hrefs = root.xpath("//div[contains(@class,'daynews ')]//div[contains(@class,'daynews__item')]/a[@href]/@href\
                   | //div[contains(@class,'daynews ')]/..//li[contains(@class,'list__item')]/a[@href]/@href\
                   | //div[contains(@class,'cols__column_small_percent-50')]//a[@class='newsitem__title link-holder']/@href\
                   | //div[contains(@class,'cols__column_small_percent-50')]//li[contains(@class,'list__item')]//a[@href]/@href")
#Обрабатываем ссылки и забираем данные заходя в каждую новость
for item in hrefs:
    if item[:4] == 'http': #иногда в ссылке дают не относительную а полную ссылку
        href = item
    else:
        href = main_link + item
    mdic = {}
    resp1 = requests.get(href).text
    article = html.fromstring(resp1) #парсим  каждую статью
    #собираем данные для словаря
    head1 = article.xpath("//div[contains(@class,'article')]//span[@class='hdr__text']/h1[@class='hdr__inner']/text()") 
    if head1: #при быстрой обработке иногда проваливание в статью не проходит- сайт ругается что подозрение на автомат
        mdic['name'] = head1[0]
        dt = article.xpath("//div[contains(@class,'article')]//div[@class='breadcrumbs breadcrumbs_article js-ago-wrapper']//span[@datetime]/@datetime")
        dt1 = dt[0].replace('T',' ').split('+')[0]
        dtt = (datetime.strptime(dt1, '%Y-%m-%d %H:%M:%S')) #переводим в формат дата-время 
        #print (dtt)
        mdic['dt'] = dtt 
        mdic['href'] = href
        source = article.xpath("//div[contains(@class,'article')]//span[ @class='breadcrumbs__item'][2]//a[@href]/span/text()")
        mdic['source'] = source[0]
    #print (mdic)
        news_dict.append(mdic)
    else:
        pass
    time.sleep(1/2)

print('mail.ru  - ', len(news_dict), '  новостей')

#YANDEX.RU
main_link = 'https://yandex.ru'
response = requests.get(main_link+'/news').text
root = html.fromstring(response)
# Собираем список новостей на первой странице
items = root.xpath("//div[@class='page-content']//table[@class='stories-set__items']//h2[@class='story__title']\
                   | //div[@class='story__content']//h2[@class='story__title']")
# разбираем список на данные для словаря
for item in items:
    hr = item.xpath("./a[@href]/@href")[0]
    if hr[:4] == 'http': #иногда в ссылке дают не относительную а полную ссылку
        href = hr
    else:
        href = main_link + hr
    mdic = {}
    mdic['href'] = href
    mdic['name'] = item.xpath("./a[@href]/text()")[0]
    # Дату и источник надо разобрать из текста
    txt = item.xpath("./../../..//div[@class='story__date']/text()")[0]
    #pprint(txt)
    dtt = txt.split()[-1]
    now = datetime.now()
    if txt.split()[-2:-1][0]=='в': # Если возлле времени стоит "вчера в " надо брать вчерашнюю дату, иначе - сегодня
        src = (' '.join(txt.split()[0:-3]))
        dt = '{:%Y-%m-}{}'.format(now, now.day - 1) + " " + dtt + ':00'
    else:
        src = (' '.join(txt.split()[0:-1]))
        dt = '{:%Y-%m-%d}'.format(now) + " " + dtt + ':00'
    mdic['dt'] = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') #переводим в формат дата-время  
    mdic['source'] = src   
    news_dict.append(mdic)
print('yandex.ru и mail.ru - ', len(news_dict), '  новостей')


#LENTA.RU

main_link = 'https://lenta.ru'
response = requests.get(main_link).text
root = html.fromstring(response)
#создаем список ссылок на новости 
hrefs = root.xpath("//div[contains(@class,'item')]//a[contains(@href,'news' )\
                    and not(contains(@class,'topic-title-pic__link js-dh'))]/@href")
#Обрабатываем ссылки и забираем данные заходя в каждую новость
for item in hrefs:
    if item[:4] == 'http': # это внешние кросс-ссылки поэтому пропускаем
        pass 
    else:
        href = main_link + item
    #print(href)
    mdic = {}
    resp1 = requests.get(href).text
    article = html.fromstring(resp1) #выбираем каждую статью
    #собираем данные для словаря
    head1 = article.xpath("//div[@class = 'b-topic__header js-topic__header']/h1/text()")
    mdic['name'] = head1[0]
    dt = article.xpath("//div[@class = 'b-topic__header js-topic__header']//time[@datetime]/@datetime")
    dt1 = dt[0].replace('T',' ').split('+')[0]
    mdic['dt'] = datetime.strptime(dt1, '%Y-%m-%d %H:%M:%S')
    mdic['href'] = href
    mdic['source'] = 'lenta.ru'
    news_dict.append(mdic)

print('ИТОГО', len(news_dict), '  новостей')
#pprint(news_dict)
#df = pd.DataFrame(news_dict)
#df1 = df[['dt','source','name']]
#pprint (df1)
#df.to_csv("news.csv", sep=";", index = False)

#Сложим все новости в MongoDB базу "news"

client = MongoClient('localhost', 27017)
db = client['news']
collection = db.mynews
collection.insert_many(news_dict)

#выгрузим еще в JSON
#with open("news.json", "w") as f:
#    json.dump(news_dict, f, sort_keys=True, indent=2)

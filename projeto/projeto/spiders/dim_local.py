import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re
import json
import unicodedata
from datetime import datetime
import pandas as pd
import psycopg2


class DimLocalItem(scrapy.Item):
    Nm_Municipio = scrapy.Field()
    Sg_Estado = scrapy.Field()


class LocalSpider(scrapy.Spider):
    name = 'local'
    allowed_domains = ['*']
    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    }

    def __init__(self):
        self.lista_anos = ['2014', '2015', '2016', '2017', '2018', '2019']

    def start_requests(self):
        url_ini = 'https://transparencia.tce.sp.gov.br/api/json/municipios'
        req_ini = Request(url=url_ini, headers=self.headers, dont_filter=True, callback=self.parse_cities)
        yield req_ini

    def parse_cities(self, response):
        js = json.loads(response.body)
        for c in js:
            cidade = unicodedata.normalize('NFD', c['municipio_extenso']).encode('ascii', 'ignore').decode('utf-8')
            item_local = DimLocalItem()
            item_local['Nm_Municipio'] = cidade
            item_local['Sg_Estado'] = 'SP'
            yield item_local

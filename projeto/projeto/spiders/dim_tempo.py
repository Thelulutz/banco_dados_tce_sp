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
import holidays


class DimTempoItem(scrapy.Item):
    Data = scrapy.Field()
    Ano = scrapy.Field()
    Nr_Semestre = scrapy.Field()
    Nr_Trimestre = scrapy.Field()
    Nr_Mes = scrapy.Field()
    Nm_Mes = scrapy.Field()
    Nr_Dia = scrapy.Field()
    Nm_Dia = scrapy.Field()
    Ev_Especial = scrapy.Field()


class TempoSpider(scrapy.Spider):
    name = 'tempo'
    allowed_domains = ['*']
    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    }

    def __init__(self):
        self.lista_anos = ['2014', '2015', '2016', '2017', '2018', '2019']
        feriados = holidays.Brazil()
        self.lista_feriados = feriados['2020-01-01':'2020-12-31']

    def start_requests(self):
        url_ini = 'https://transparencia.tce.sp.gov.br/api/json/municipios'
        req_ini = Request(url=url_ini, headers=self.headers, dont_filter=True, callback=self.parse_cities)
        yield req_ini

    def parse_cities(self, response):
        js = json.loads(response.body)
        for c in js:
            cidade = unicodedata.normalize('NFD', c['municipio_extenso']).encode('ascii', 'ignore').decode('utf-8')
            cod_city = c['municipio']
            for ano in self.lista_anos:
                for mes in range(1, 13):
                    url_despesa = 'https://transparencia.tce.sp.gov.br/api/json/despesas/{}/{}/{}'.format(cod_city, ano, str(mes))
                    meta = {
                        'cidade': cidade,
                        'ano': ano,
                    }
                    req_despesa = Request(url=url_despesa, headers=self.headers, meta=meta, dont_filter=True, callback=self.parse_despesa)
                    yield req_despesa

    def parse_despesa(self, response):
        js = json.loads(response.body)
        for lici in js:
            data_emissao = lici['dt_emissao_despesa']
            date_evento = datetime.strptime(data_emissao, '%d/%m/%Y')
            ano = date_evento.year
            nr_mes = date_evento.month
            nr_dia = date_evento.day
            if nr_mes >= 7:
                nr_semestre = 2
            else:
                nr_semestre = 1
            if nr_mes <= 4:
                nr_trimestre = 1
            elif nr_mes <= 8:
                nr_trimestre = 2
            else:
                nr_trimestre = 3
            if date_evento in self.lista_feriados:
                ev_especial = 'S'
            else:
                ev_especial = 'N'
            if nr_mes == 1:
                nm_mes = 'Janeiro'
            elif nr_mes == 2:
                nm_mes = 'Fevereiro'
            elif nr_mes == 3:
                nm_mes = 'Marco'
            elif nr_mes == 4:
                nm_mes = 'Abril'
            elif nr_mes == 5:
                nm_mes = 'Maio'
            elif nr_mes == 6:
                nm_mes = 'Junho'
            elif nr_mes == 7:
                nm_mes = 'Julho'
            elif nr_mes == 8:
                nm_mes = 'Agosto'
            elif nr_mes == 9:
                nm_mes = 'Setembro'
            elif nr_mes == 10:
                nm_mes = 'Novembro'
            else:
                nm_mes = 'Dezembro'
            if date_evento.weekday() == 0:
                nm_dia = 'Segunda-Feira'
            elif date_evento.weekday() == 1:
                nm_dia = 'Terca-Feira'
            elif date_evento.weekday() == 2:
                nm_dia = 'Quarta-Feira'
            elif date_evento.weekday() == 3:
                nm_dia = 'Quinta-Feira'
            elif date_evento.weekday() == 4:
                nm_dia = 'Sexta-Feira'
            elif date_evento.weekday() == 5:
                nm_dia = 'Sabado'
            else:
                nm_dia = 'Domingo'

            item_tempo = DimTempoItem()
            item_tempo['Data'] = data_emissao
            item_tempo['Ano'] = ano
            item_tempo['Nr_Semestre'] = nr_semestre
            item_tempo['Nr_Trimestre'] = nr_trimestre
            item_tempo['Nr_Mes'] = nr_mes
            item_tempo['Nm_Mes'] = nm_mes
            item_tempo['Nr_Dia'] = nr_dia
            item_tempo['Nm_Dia'] = nm_dia
            item_tempo['Ev_Especial'] = ev_especial
            yield item_tempo

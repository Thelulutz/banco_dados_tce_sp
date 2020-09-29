import scrapy
from scrapy import Request, FormRequest, Selector
from projeto.items import TceDespesasItem
import re
import json
import unicodedata
from datetime import datetime
import pandas as pd
import psycopg2

class TceDespesasSpide(scrapy.Spider):
    name = 'tce_despesas'
    allowed_domains = ['*']
    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    }


    def __init__(self):
        self.date_now = datetime.now().strftime('%d/%m/%Y')
        self.lista_anos = ['2014', '2015', '2016', '2017', '2018', '2019']


    def start_requests(self):
        url_ini = 'https://transparencia.tce.sp.gov.br/api/json/municipios'
        req_ini = Request(url=url_ini, headers=self.headers, dont_filter=True, callback=self.parse_cities)
        yield req_ini


    def parse_cities(self, response):
        js = json.loads(response.body)
        for c in js:
            cidade = unicodedata.normalize('NFD', c['municipio_extenso']).encode('ascii','ignore').decode('utf-8')
            cod_city = c['municipio']
            for ano in self.lista_anos:
                for mes in range(1,13):
                    url_despesa = 'https://transparencia.tce.sp.gov.br/api/json/despesas/{}/{}/{}'.format(cod_city, ano, str(mes))
                    meta = {
                        'cidade':cidade,
                        'ano':ano,
                    }
                    req_despesa = Request(url=url_despesa, headers=self.headers, meta=meta, dont_filter=True, callback=self.parse_despesa)
                    yield req_despesa


    def parse_despesa(self, response):
        js = json.loads(response.body)
        cidade = response.meta['cidade']
        ano = response.meta['ano']
        for lici in js:
            orgao = lici['orgao']
            orgao = unicodedata.normalize('NFD', orgao).encode('ascii','ignore').decode('ascii','ignore')
            mes = lici['mes']
            evento = lici['evento']
            evento = unicodedata.normalize('NFD', evento).encode('ascii','ignore').decode('ascii','ignore')
            nr_empenho = lici['nr_empenho']
            nr_empenho = unicodedata.normalize('NFD', nr_empenho).encode('ascii','ignore').decode('ascii','ignore')
            id_fornecedor = lici['id_fornecedor']
            id_fornecedor = unicodedata.normalize('NFD', id_fornecedor).encode('ascii','ignore').decode('ascii','ignore')
            fornecedor = lici['nm_fornecedor']
            fornecedor = unicodedata.normalize('NFD', fornecedor).encode('ascii','ignore').decode('ascii','ignore')
            data_emissao = lici['dt_emissao_despesa']
            valor = lici['vl_despesa']
            valor = float(valor.replace(',','.'))

            item_despesa = TceDespesasItem()
            item_despesa['Cidade'] = cidade
            item_despesa['Orgao'] = orgao
            item_despesa['Ano'] = ano
            item_despesa['Mes'] = mes
            item_despesa['Evento'] = evento
            item_despesa['Num_Empenho'] = nr_empenho
            item_despesa['ID_Fornecedor'] = id_fornecedor
            item_despesa['NM_Fornecedor'] = fornecedor
            item_despesa['Dt_Emissao_Despesa'] = data_emissao
            item_despesa['Valor_Despesa'] = valor
            yield item_despesa

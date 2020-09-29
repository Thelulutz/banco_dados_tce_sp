# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


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


class DimGastoItem(scrapy.Item):
    Nr_Empenho = scrapy.Field()
    Vl_Gasto = scrapy.Field()


class DimFornecedorItem(scrapy.Item):
    Cd_Fornecedor = scrapy.Field()
    Nm_Fornecedor = scrapy.Field()
    Nr_CNPJ = scrapy.Field()


class DimOrgaoPublicoItem(scrapy.Item):
    Nm_Orgao = scrapy.Field()


class DimLocalItem(scrapy.Item):
    Nm_Municipio = scrapy.Field()
    Sg_Estado = scrapy.Field()

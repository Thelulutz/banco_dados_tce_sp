# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import psycopg2


class TempoPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""insert into dim_tempo(dt_data, ano, nr_semestre, nr_trimestre, nr_mes, nm_mes, nr_dia, nm_dia, evento_especial) 
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                on conflict (dt_data)
                                do nothing;""",
                             (item['Data'],
                              item['Ano'],
                              item['Nr_Semestre'],
                              item['Nr_Trimestre'],
                              item['Nr_Mes'],
                              item['Nm_Mes'],
                              item['Nr_dia'],
                              item['Nm_Dia'],
                              item['Ev_Especial']
                              ))
            self.connection.commit()
            return item
        except:
            pass


class GastoPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""insert into dim_gasto(nr_empenho, vl_gasto) values(%s,%s);""",
                             (item['Nr_Empenho'],
                              item['Vl_Gasto']
                              ))
            self.connection.commit()
            return item
        except:
            pass


class FornecedorPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.connection.commit()
            self.cur.execute("""insert into dim_fornecedor(cod_fornecedor, nm_fornecedor, nr_cnpj) 
                                values(%s,%s,%s)
                                on conflict (nm_fornecedor)
                                do nothing ;""",
                             (item['Cd_Fornecedor'],
                              item['Nm_Fornecedor'],
                              item['Nr_CNPJ']
                              ))
            return item
        except:
            pass


class OrgaoPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""insert into dim_orgao_publico(nm_orgao) 
                                values(%s) 
                                on conflict (nm_orgao)
                                do nothing ;""",
                             (item['Nm_Orgao']
                              ))
            self.connection.commit()
            return item
        except:
            pass


class LocalPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""insert into dim_local(nm_municipio, sg_estado) 
                                values(%s,%s) 
                                on conflict (nm_municipio) 
                                do nothing ;""",
                             (item['Nm_Municipio'],
                              item['Sg_Estado']
                              ))
            self.connection.commit()
            return item
        except:
            pass


class ReceitaPipeline(object):

    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = 'TCEfiap'  # your password
        database = 'projeto'
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            self.cur.execute("""insert into dim_receita(ft_receita, vl_receita) values(%s,%s);""",
                             (item['Ft_Receita'],
                              item['Vl_Receita']
                              ))
            self.connection.commit()
            return item
        except:
            pass
# Banco de Dados - Portal Transparência TCE-SP

Esse projeto é para montar um banco de dados com os dados coletados no portal transparência do tribunal de contas do estado de São Paulo.  

  

## Ferramentas Utilizadas

- **Banco de dados**: PostgreSQL
- **Crawler**: Scrapy
- **Padronização e Inserção dos dados**: Pandas e Psycopg2  

  

## Banco de Dados

Na pasta sql contém as tabelas criadas para armazenar os dados.  
As variáveis VARCHAR em sua maior parte foi extrapolado o número máximo de caracteres.  

  

## Pasta Projeto

Nessa pasta se encontra os scripts em python para:

- Criação de scrapy Items
- Pipelines para inserção dos dados nas tabelas
- Scripts de requests na api do portal transparência
- Utilização de bibliotecas para a padronização dos dados

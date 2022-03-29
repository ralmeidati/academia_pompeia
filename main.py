#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import streamlit as st
import mysql.connector
from sqlalchemy import create_engine
import pymysql

def _carga():
    con = mysql.connector.connect(host='academiadb1.mysql.database.azure.com',
                              database='academia',
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"],)
    cursor = con.cursor()
    df = pd.read_sql("SELECT * FROM DW_1\
                WHERE NOT EXISTS(SELECT * FROM DW_2 WHERE DW_1.`Código de cliente` = DW_2.`Código de cliente`)\
                limit 100", con);
    cursor.close()
    con.close()
    df.drop('index', axis=1, inplace = True)
    
    sqlEngine = create_engine('mysql+pymysql://st.secrets["db_username"]:st.secrets["db_password"]@academiadb1.mysql.database.azure.com:3306/academia', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    df.to_sql('dw_2', dbConnection, index=False,if_exists='append');
    dbConnection.close()

def _reset():
    con = mysql.connector.connect(host='academiadb1.mysql.database.azure.com',
                              database='academia',
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"],)
    cursor = con.cursor()
    df = pd.read_sql("SELECT * FROM DW_1 limit 100", con);
    cursor.close()
    con.close()
    df.drop('index', axis=1, inplace = True)
    sqlEngine = create_engine('mysql+pymysql://st.secrets["db_username"]:st.secrets["db_password"]@academiadb1.mysql.database.azure.com:3306/academia', pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    df.to_sql('dw_2', dbConnection, index=False, if_exists='replace');
    dbConnection.close()


con = mysql.connector.connect(host='academiadb1.mysql.database.azure.com',
                              database='academia',
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"],)
cursor = con.cursor()

df = pd.read_sql("SELECT * FROM dw_2", con);

cursor.close()
con.close()

datas = df['data última presença']
periodo = []
temp = []
for d in datas:
    temp = str(datetime.date(d))
    periodo.append(temp)
datas = pd.Series(periodo)
periodo = datas.unique()


consultores = df[['Consultor']].groupby('Consultor').agg(Total=('Consultor','count')).sort_values(by='Total', ascending=False)
consultor = consultores.head(1)

professores = df[['Professor']].groupby('Professor').agg(Total=('Professor','count')).sort_values(by='Total', ascending=False)
professores.head(8)
professor = professores[1:2]

temp = pd.DataFrame()
temp[['modalidade1','modalidade2','modalidade3']] = df['Modalidade'].str.split('+', expand = True)
correcao = {'MUSCULAÇÃO ': 'MUSCULAÇÃO',' MUSCULAÇÃO':'MUSCULAÇÃO',' MUSCULAÇÃO ':'MUSCULAÇÃO','FUNCIONAL ':'FUNCIONAL',' FUNCIONAL':'FUNCIONAL','NATAÇÃO ':'NATAÇÃO',' NATAÇÃO':'NATAÇÃO','-':'PLANO COMPLETO'}
temp.replace(correcao, inplace= True)

modalidades = pd.DataFrame()
modalidades['Total'] = pd.concat([temp['modalidade1'],temp['modalidade2'],temp['modalidade3']], ignore_index=True)
modalidades = pd.DataFrame(modalidades['Total'].value_counts())

st.title("Academia Pompeia")

st.sidebar.button('Carregar mais dados', help='Clique para realizar a simulação de entrada de dados', on_click=_carga)

sidebar_selection = st.sidebar.radio(
    "Selecione uma opção:",
    ['Qual é o cliente que não vai a mais tempo?','O consultor que tem mais clientes?','Professor que tem mais alunos?','Qual é a modalidade, mais consumida?','Encontrar inconsistencias no cadastro das modalidades?'],)

if sidebar_selection == 'Qual é o cliente que não vai a mais tempo?':
  st.markdown('## Qual é o cliente que não vai a mais tempo?')
  data_inicial = st.select_slider('Selecione a data inicial:',options=periodo)
  clientes = df[['Código de cliente']]
  clientes['data última presença'] = datas
  cliente_sedentario = clientes[(clientes['data última presença'] >= data_inicial)]
  cliente_sedentario = cliente_sedentario.head(1)
  cliente_sedentario

if sidebar_selection == 'O consultor que tem mais clientes?':
  st.markdown('## O consultor que tem mais clientes?')
  st.write('Consultor com mais clientes ativos dentro do período')  
  data_inicial, data_final = st.select_slider('Selecione o período:', options=periodo, value=(periodo.min(), periodo.max()))
  consultores = df[['Consultor']]
  consultores['data última presença'] = datas
  consultores = consultores[(consultores['data última presença'] >= data_inicial) & (consultores['data última presença'] <= data_final)]
  consultores = consultores.groupby('Consultor').agg(Total=('Consultor','count')).sort_values(by='Total', ascending=False)
  consultor = consultores.head(1)
  consultor
  st.bar_chart(consultores,width=0, height=400)  
  
if sidebar_selection == 'Professor que tem mais alunos?':
  st.markdown('## Professor que tem mais alunos?')
  professor
  st.bar_chart(professores,width=0, height=400)
  
if sidebar_selection == 'Qual é a modalidade, mais consumida?':
  st.markdown('## Qual é a modalidade, mais consumida?')
  modalidades[0:2]
  st.bar_chart(modalidades,width=0, height=400)
  
if sidebar_selection == 'Encontrar inconsistencias no cadastro das modalidades?':
  st.write("Em análise...")

st.sidebar.button('RESET', help='Clique para voltar ao estado inicial', on_click=_reset)


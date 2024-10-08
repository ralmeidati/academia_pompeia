#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import streamlit as st
import mysql.connector
from sqlalchemy import create_engine
import pymysql
Sqlhost = "sql10.freesqldatabase.com"
Database_name = "sql10734914"


def _carga():
    con = mysql.connector.connect(host=Sqlhost,
                              database=Database_name,
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"])
    cursor = con.cursor()
    df = pd.read_sql("SELECT * FROM DW_completo\
                WHERE NOT EXISTS(SELECT * FROM DW_producao WHERE DW_completo.`Código de cliente` = DW_producao.`Código de cliente`)\
                ORDER BY `data última presença`\
                limit 100", con);
    cursor.close()
    con.close()
    df.drop('index', axis=1, inplace = True)
    
    sqlEngine = create_engine(st.secrets["remotemysql_auth"], pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    df.to_sql('DW_producao', dbConnection, index=False,if_exists='append');
    dbConnection.close()

def _reset():
    con = mysql.connector.connect(host=Sqlhost,
                              database=Database_name,
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"])
    cursor = con.cursor()
    df = pd.read_sql("SELECT * FROM DW_completo limit 100", con);
    cursor.close()
    con.close()
    df.drop('index', axis=1, inplace = True)
    sqlEngine = create_engine(st.secrets["remotemysql_auth"], pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    df.to_sql('DW_producao', dbConnection, index=False, if_exists='replace');
    dbConnection.close()


# Consulta inicial ao DW_producao para carregar o programa:

con = mysql.connector.connect(host=Sqlhost,
                              database=Database_name,
                              user=st.secrets["db_username"],
                              password=st.secrets["db_password"])
cursor = con.cursor()

df = pd.read_sql("SELECT * FROM DW_producao", con);

cursor.close()
con.close()

datas = pd.to_datetime(df['data última presença'], dayfirst=True)
periodo = []
temp = []
for d in datas:
    temp = str(datetime.date(d))
    periodo.append(temp)
datas = pd.Series(periodo)
periodo = datas.unique()

hoje = pd.to_datetime(date.today(), dayfirst=True)
Calc_idades = df[['Nascimento']]
#Acrescento uma coluna no Calc_idades preenchendo com a data de hoje
for idade in Calc_idades:    
    Calc_idades['data_atual'] = hoje
df['idade'] = (Calc_idades.data_atual[0:len(Calc_idades)] - Calc_idades.Nascimento[0:len(Calc_idades)]) // timedelta(days=365.2425)

# O que é exibido no navegador começa aqui:

st.title("Academia Pompeia")

st.sidebar.button('Carregar mais dados', help='Clique para realizar a simulação de entrada de dados', on_click=_carga)

sidebar_selection = st.sidebar.radio(
    "Selecione uma opção:",
    ['Qual é o cliente que não vai a mais tempo?',
     'O consultor que tem mais clientes?',
     'Professor que tem mais alunos?',
     'Qual é a modalidade, mais consumida?',
     'Como conheceu a academia',
     'Encontrar inconsistencias no cadastro das modalidades?'])

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
  professores = df[['Professor']].groupby('Professor').agg(Total=('Professor','count')).sort_values(by='Total', ascending=False)
  professores.head(8)
  professor = professores[1:2]
  professor
  st.bar_chart(professores,width=0, height=400)
  
if sidebar_selection == 'Qual é a modalidade, mais consumida?':
  st.markdown('## Qual é a modalidade, mais consumida?')
  st.write('Modalidades dentro do período')  
  data_inicial, data_final = st.select_slider('Selecione o período:', options=periodo, value=(periodo.min(), periodo.max()))
  temp = pd.DataFrame({'datas':datas})
  temp[['modalidade1','modalidade2','modalidade3']] = df['Modalidade'].str.split('+', expand = True)
  correcao = {'MUSCULAÇÃO ': 'MUSCULAÇÃO',' MUSCULAÇÃO':'MUSCULAÇÃO',' MUSCULAÇÃO ':'MUSCULAÇÃO','FUNCIONAL ':'FUNCIONAL',' FUNCIONAL':'FUNCIONAL','NATAÇÃO ':'NATAÇÃO',' NATAÇÃO':'NATAÇÃO','-':'PLANO COMPLETO'}
  temp.replace(correcao, inplace= True)
  temp = temp[(temp['datas'] >= data_inicial) & (temp['datas'] <= data_final)]
  modalidades = pd.DataFrame()
  modalidades['Total'] = pd.concat([temp['modalidade1'],temp['modalidade2'],temp['modalidade3']], ignore_index=True)
  modalidades = pd.DataFrame(modalidades['Total'].value_counts())
  modalidades[0:2]
  st.bar_chart(modalidades,width=0, height=400)

if sidebar_selection == 'Como conheceu a academia':
  df['Como Conheceu'].replace({'-':'NÃO INFORMADO'}, inplace = True)
  como_conheceu = df['Como Conheceu'].value_counts()
  st.bar_chart(como_conheceu,width=0, height=400)

if sidebar_selection == 'Encontrar inconsistencias no cadastro das modalidades?':    
  st.write("Clientes cadastrados como Natação Infantil que estão acima da idade")
  select_1 = df[df['Modalidade'].str.contains('NATAÇÃO INFANTIL', na=False)]
  select_1 = select_1[['Código de cliente','idade','Modalidade']]
  select_1 = select_1.query('idade > 13').sort_values(by=['idade'])
  select_1
  
  st.write("Clientes cadastrados como Natação que estão abaixo da idade")
  select_2 = df[df['Modalidade'].str.contains('NATAÇÃO', na=False)]
  select_2 = select_2[~select_2['Modalidade'].str.contains('INFANTIL', na=False)]
  select_2 = select_2[['Código de cliente','idade','Modalidade']]
  select_2 = select_2.query('idade < 13').sort_values(by=['idade'])
  select_2

  st.write("Clientes cadastrados como Musculação que estão abaixo da idade")
  select_3 = df[df['Modalidade'].str.contains('MUSCULAÇÃO', na=False)]
  select_3 = select_3[['Código de cliente','idade','Modalidade']]
  select_3 = select_3.query('idade < 16').sort_values(by=['idade'])
  select_3


    
st.sidebar.button('RESET', help='Clique para voltar ao estado inicial', on_click=_reset)

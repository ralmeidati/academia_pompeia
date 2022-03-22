#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

df = pd.read_csv('pompeia.csv', sep = ';', usecols= ['Código','Nascimento','Sexo','Consultor','Professor','Último Status','Modalidade'])
new_col_names = {'Código':'Código de cliente'}
df = df.rename(new_col_names, axis = 'columns')

#Dividir a coluna do ultimo status em duas novas colunas no dataframe original, exluindo a coluna original
df[['data última presença', 'hora última presença']] = df["Último Status"].str.split(' ', expand = True)
df.drop(columns =["Último Status"], inplace = True) 

# Convertendo strings para date e time
df['Nascimento'] = pd.to_datetime(df['Nascimento'], format="%d/%m/%Y")
df['data última presença'] = pd.to_datetime(df['data última presença'], format="%d/%m/%Y")
df['hora última presença'] = pd.to_datetime(df['hora última presença'], format="%H:%M")

#O cliente sedentário é o primeiro registro na ordenação por data do último status
cliente_sedentario = df[['Código de cliente','data última presença']].sort_values(by=['data última presença'])
cliente_sedentario = cliente_sedentario.head(1)

consultores = df[['Consultor']].groupby('Consultor').agg(Total=('Consultor','count')).sort_values(by='Total', ascending=False)
consultor = consultores.head(1)

correcao = {'-':'SEM PROFESSOR'}
df['Professor'].replace(correcao, inplace= True)
professores = df[['Professor']].groupby('Professor').agg(Total=('Professor','count')).sort_values(by='Total', ascending=False)
professores.head(8)
professor = professores[1:2]

correcao = {'FUNCIONAL + MUSCULAÇÃO, NATAÇÃO':'FUNCIONAL + MUSCULAÇÃO + NATAÇÃO'}
df['Modalidade'].replace(correcao, inplace= True)
temp = pd.DataFrame()
temp[['modalidade1','modalidade2','modalidade3']] = df['Modalidade'].str.split('+', expand = True)

correcao = {'MUSCULAÇÃO ': 'MUSCULAÇÃO',' MUSCULAÇÃO':'MUSCULAÇÃO',' MUSCULAÇÃO ':'MUSCULAÇÃO','FUNCIONAL ':'FUNCIONAL',' FUNCIONAL':'FUNCIONAL','NATAÇÃO ':'NATAÇÃO',' NATAÇÃO':'NATAÇÃO','-':'PLANO COMPLETO'}
temp.replace(correcao, inplace= True)

modalidades = pd.DataFrame()
modalidades['Total'] = pd.concat([temp['modalidade1'],temp['modalidade2'],temp['modalidade3']], ignore_index=True)
modalidades = pd.DataFrame(modalidades['Total'].value_counts())

st.title("Academia Pompeia")

sidebar_selection = st.sidebar.radio(
    "Selecione uma opção:",
    ['Qual é o cliente que não vai a mais tempo?','O consultor que tem mais clientes?','Professor que tem mais alunos?','Qual é a modalidade, mais consumida?','Encontrar inconsistencias no cadastro das modalidades?'],)

if sidebar_selection == 'Qual é o cliente que não vai a mais tempo?':
  st.markdown('## Qual é o cliente que não vai a mais tempo?')
  cliente_sedentario
if sidebar_selection == 'O consultor que tem mais clientes?':
  st.markdown('## O consultor que tem mais clientes?')
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


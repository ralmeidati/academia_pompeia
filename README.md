# Projeto de análise de dados para a Academia Pompeia.
Esta é uma simulação com dados reais da Academia Pompéia.

A academia ainda não possui um sistema de BI, mas nos disponibilizou um export dos dados (estático) para que pudessemos analisar respondendo as perguntas:

1. Qual é o cliente que não vai a mais tempo (Para ter um melhor relacionamento com os clientes);
2. O consultor que tem mais clientes (Para criar novas estratégias de estimulos para os consultores);
3. Professor que tem mais alunos (Para não sobrecarregar um unico professor, gerir melhor a distribuição de alunos);
4. Qual é a modalidade, mais consumida (Para saber qual a modalidade é carro chefe da academia);
5. Encontrar inconsistencia no cadastro das modalidades (Natação infantil, Natação e Musculação).


Esse é um snapshot do banco original:

![Banco original](/home/ralmeida/jupyter/academia_pompeia/img/banco_original.png  "Banco original")

Então foi criada uma tarefa (Dag) no Airflow para replicar as atualizações banco de produção no nosso DataWarehouse com os dados já tratados prontos para análise.

No nosso projeto, foi criado um DW no Azure com todos os dados disponibilizados pela academia e um DW auxiliar para mostrar a aplicação em funcionamento,
com botões de carga de dados e reset para simular a entrada de dados com a passagem dos meses, mas esses botões são desnecessários em um cenário real onde o Airflow ficaria responsável pela carga diária dos dados.


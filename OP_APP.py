import streamlit as st
from streamlit_msal import Msal
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import numpy as np
import datetime as datetime
from io import StringIO
import matplotlib.pyplot as plt


# Acessando os segredos do Streamlit
client_id = st.secrets['azure']["client_id"]
authority = st.secrets['azure']["authority"]
connection_string = st.secrets['azure']["connection_string"]
container_name = st.secrets['azure']['container_name']

# Mapeamentos princiais
acoes = ['Tenax Acoes A FIC FIA','Tenax Acoes Alocadores FIC FIA','Tenax Acoes FIC FIA','Tenax Acoes Institucional A FIC FIA','Tenax Acoes Master FIA','Tenax Acoes Master Institucional FIA','TX A Acoes FIA','Tenax Equity Hedge FIM']
macro = ['Tenax Macro A FIC FIM','Tenax Macro Alocadores FIC FIM','Tenax Macro FIC FIM','Tenax Macro Master FIM']
total_return = ['Tenax Total Return A FIC FIM','Tenax Total Return Alocadores FIC FIM','Tenax Total Return FIC FIM','Tenax Total Return Master FIM','Tenax Total Return Prev FIE','Tenax Total Return Prev Master FIFE','Tenax TR FIC FIA','Tenax TR Master FIA']
renda_fixa = ['Geri Tenax FI RF','Synta Tenax FI RF','Tenax Renda Fixa LP']
credito_privado = ['Tenax RFA Incentivado FIF CIC','Tenax RFA Incentivado Master FIF','Tenax RFA Prev Master FIFE']

de_para_corretoras = {85:'BTG',
                        3:'XP',
                        114:'ITAU',
                        39:'BRADESCO',
                        107:'TERRA',
                        127:'TULLET',
                        122:'LIQUIDEZ',
                        1982:'MODAL',
                        23:'NECTON',
                        8:'UBS',
                        92:'RENASCENCA',
                        45:'CREDIT SUISSE',
                        16:'JP MORGAN',
                        6003:'C6',
                        120:'GENIAL',
                        27:'SANTANDER',
                        77:'CITIBANK',
                        13:'BOFA',
                        238:'GOLDMAN',
                        1099:'INTER',
                        1130:'STONEX',
                        40:'MORGAN STANLEY',
                        59:'SAFRA'}

CLASSE_PRODUTOS_BOVESPA = ['BDR Options','BDR Unsponsored','Equity','Equity Options','Equity Receipts','Equity Subscription','ETF BR','ETF BR ISHARES OFF','ETF BR Receipts','ETF Options','IBOV Options']
CLASSE_PRODUTOS_BMF = ['AUDUSD Futures - BMF','CPM Options - BMF','DAP Future','DI1Future','DIF','DII','DR1','EURUSD Futures - BMF','GBPUSD Futures - BMF','IBOVSPFuture','IDIOptionCall','IDIOptionPut','IR1','S&P500 Future Options - BMF','S&P500 Futures - BMF','US T-Note 10 BMF','USDBRLFuture','USDBRLOptionCall','USDBRLOptionPut','USDCAD Futures - BMF','USDCLP Futures - BMF','USDCNH Futures - BMF','USDJPY Futures - BMF','USDMXN Futures - BMF','USDZAR Futures - BMF']
CLASSE_PRODUTOS_CREDITO = ['CDB DI Spread','Compromissada - Título Privado','Compromissada CDI','CRA','CRI','Debenture','FIDC DI Spread','Letra Financeira DI Spread']
CLASSE_PRODUTOS_TITULOS_PUBLICOS = ['LFT','NTN-B']
CLASSE_PRODUTOS_OFF = ['30-Day Fed Funds Futures','90 Day Bank Accepted Bill - ASX','AUD Fixed-Float SWAP','AUD/USD Futures - CME','AUD/USD Options - CME','CAD Fixed-Float SWAP','CAD/USD Futures - CME','CAD/USD Options - CME','Canadian Bank Accept 3M Fut','Canadian Bond Futures 10Y - MSE','Canadian Bond Futures 2Y - MSE','Cash','CHF/USD Futures - CME','CLP Fixed-Float SWAP','COP Fixed-Float SWAP','Copper Future - CMX','Copper Future Options - CMX','Currencies Digital Options','Currencies Forward','Currencies NDF','Currencies NDO','Currencies Options','DAX Index Future - EUREX','DJ Euro STOXX 50 Future - EUREX','EUR ESTR OIS','EUR/USD Futures - CME','EUR/USD Options - CME','Euribor Futures','Euribor Futures Options','Euro-Bund Future Options - EUREX','Euro-Bund Futures - EUREX','Eurodollar Futures','Eurodollar Futures Options','Euro-Schatz Futures - EUREX','GBP/USD Futures - CME','GBP/USD Options - CME','Gold Future  - CMX','Iron Ore 62 Fe TSI - SGX','JGB 10-year Futures - TSE','JPY/USD Futures - CME','JPY/USD Options - CME','MSCI Emerging Mkt Index Future - ICE','MXN TIIE SWAP','MXN/USD Futures - CME','NASDAQ-100 E-mini Futures - CME','NASDAQ-100 E-mini Options - CME','Provisions and Costs','Russell 2000 Index Mini Futures - CME','Russell 2000 Index Options - CME','S&P500 E-mini Futures','S&P500 E-mini Options','Swap CAD OIS','Swap FixedxCPI','Swap GBP OIS','Swap USD OIS','Three-Month CORRA Futures - MSE','Three-Month SOFR Future Options - CME','Three-Month SOFR Futures - CME','Three-Month SONIA Futures - ICE','Three-Month SONIA Futures Options - ICE','US Treasury Bond Future','US Treasury Bond Future Options','VIX Futures - CBOE','WTI Crude Oil Future','WTI Crude Oil Future Options','ZAR/USD Futures - CME']

st.set_page_config(layout='wide')

col4, col5 = st.columns(2)

#! '''Funções responsáveis por carregar os arquivos:'''
def load_tables_blob(arquivo,separador=None):
    # Inicializando BlobServiceClient com a connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Pegando o container
    container_client = blob_service_client.get_container_client(container_name)
    # Nome do arquivo no Blob
    arquivo = arquivo
    # Obtendo o BlobClient para o arquivo específico
    blob_client = container_client.get_blob_client(arquivo)
    # Baixando o conteúdo do blob
    downloaded_blob = blob_client.download_blob()
    # Lendo o conteúdo do blob
    blob_content = downloaded_blob.content_as_text()
    
    if separador is None:
        df = pd.read_csv(StringIO(blob_content)) 
    elif separador == 'xlsx':
        blob_content = downloaded_blob.readall()
        df = pd.read_excel(io.BytesIO(blob_content))
    else:
        df = pd.read_csv(StringIO(blob_content), delimiter=separador) 
    return df

def load_nav_and_shares(data_selecionada_DBY):
    arquivo = f'HistoricalFundsNAVandShare-24Feb2022-{data_selecionada_DBY}.txt'
    HIST_NAV = load_tables_blob(arquivo,'\t')
    return HIST_NAV

def load_mapeamento_setorial():
    arquivo = 'TABELA_AUXILIAR.csv'
    classificacao_setorial = load_tables_blob(arquivo)
    classificacao_setorial.drop_duplicates(inplace=True)
    return classificacao_setorial

def load_primitivas(data_selecionada_DBY):
    arquivo = f'FundOverviewByPrimitive_{data_selecionada_DBY}.txt'
    primitivas = load_tables_blob(arquivo,'\t')
    return primitivas

def load_trades_lote(data_selecionada_DBY):
    arquivo = f'FundsTrades-{data_selecionada_DBY}.txt'
    trades_lote = load_tables_blob(arquivo,'\t')
    return trades_lote

def load_trades_off(data_selecionada_DBY):
    arquivo = f'IntradayTradeReport {data_selecionada_DBY}.csv'
    TRADES_OFF = load_tables_blob(arquivo,',')
    return TRADES_OFF

def load_de_para_b3(data_selecionada_DBY):
    arquivo = f'InstrumentsConsolidatedFile_{data_selecionada_DBY}_1.csv'
    de_para_b3 = load_tables_blob(arquivo,',')
    de_para_b3 = dict(zip(de_para_b3['TckrSymb'], de_para_b3['TickerBMF']))
    return de_para_b3

def load_trades_clearing(data_selecionada_DBY):
    arquivo = f'allocations {data_selecionada_DBY}.csv'
    trades_clearing = load_tables_blob(arquivo,',')
    return trades_clearing

def load_passivo_btg(data_selecionada_DBY):
    arquivo = f'PASSIVO_BTG_{data_selecionada_DBY}.csv'
    passivo_btg = load_tables_blob(arquivo,',')
    return passivo_btg

def load_passivo_intrag(data_selecionada_DBY):
    arquivo = f'PASSIVO_INTRAG_{data_selecionada_DBY}.csv'
    passivo_intrag = load_tables_blob(arquivo,',')
    return passivo_intrag

def load_mapeamento_passivo(PASSIVO_CONCATENADO):
    arquivo = f'Passivo_Mapeamento_Fundos.csv'
    MAPEAMENTO_FUNDOS = load_tables_blob(arquivo,';')
    arquivo = f'Passivo_Mapeamento_Passivo.csv'
    MAPEAMENTO_PASSIVO = load_tables_blob(arquivo,';')
    MAPEAMENTO_PASSIVO.drop_duplicates(inplace=True)

    PASSIVO_CONCATENADO['Estratégia'] = PASSIVO_CONCATENADO['CNPJ do Fundo'].map(MAPEAMENTO_FUNDOS.set_index('CNPJ')['Estrategia'])
    PASSIVO_CONCATENADO['Classificação'] = PASSIVO_CONCATENADO['CNPJ do Fundo'].map(MAPEAMENTO_FUNDOS.set_index('CNPJ')['Fundos'])
    PASSIVO_CONCATENADO['Nome Padrão'] = PASSIVO_CONCATENADO['CNPJ do Fundo'].map(MAPEAMENTO_FUNDOS.set_index('CNPJ')['Nome Padrao'])
    
    PASSIVO_CONCATENADO['Alocador Final'] = PASSIVO_CONCATENADO['Cotista'].map(MAPEAMENTO_PASSIVO.set_index('Nome do cotista')['Alocador Final'])
    PASSIVO_CONCATENADO['Classificação do Cliente'] = PASSIVO_CONCATENADO['Cotista'].map(MAPEAMENTO_PASSIVO.set_index('Nome do cotista')['Classificacao do Cliente'])
    PASSIVO_CONCATENADO['Classificação do Cotista'] = PASSIVO_CONCATENADO['Cotista'].map(MAPEAMENTO_PASSIVO.set_index('Nome do cotista')['Classificacao do Cotista'])

    return PASSIVO_CONCATENADO
    
#!'''Funções responsáveis por tratar os arquivos:'''
def remover_underline(value):
    if len(value) >= 3 and value[-3] == '_':
        return value[:-3]  # Remove o antipenúltimo caractere e tudo após ele
    return value

def tratar_trades_clearing_off(TRADES_OFF):
    TRADES_OFF = TRADES_OFF[['BBG Code 1','B/S','QTY','Trade Price']]
    TRADES_OFF.rename(columns={'BBG Code 1': 'Product','Trade Price':'PM_CLEARING'}, inplace=True)
    TRADES_OFF['Side'] = TRADES_OFF['B/S'].apply(lambda x: 'Buy' if x==1 else 'Sell')
    TRADES_OFF['Dealer'] = 'BOFA'
    TRADES_OFF['Product'] = TRADES_OFF['Product'].str.rstrip()
    TRADES_OFF = calcular_preco_medio_bofa(TRADES_OFF)

    return TRADES_OFF

def tratar_trades_lote(TRADES_LOTE):
    LISTA_BACK_RISC_COMPLIANCE = ['Adriano Bartolomeu','Vicente Fletes', 'Eduardo Teixeira', 'Aline Marins', 'Vitor Chiba','Nilson Kaneko', 'Orlando Gomes'] 
    TRADES_LOTE = TRADES_LOTE[(TRADES_LOTE['ProductClass'] != 'Provisions and Costs') & (~TRADES_LOTE['Trader'].isin(LISTA_BACK_RISC_COMPLIANCE)) & ((TRADES_LOTE['IsReplicatedTrade'] == False)) & (~TRADES_LOTE['Trading Desk'].str.contains('Rateio')) & (TRADES_LOTE['Dealer'] != 'LOTE45')]
    TRADES_LOTE = TRADES_LOTE[['Trading Desk','ProductClass','Product', 'Amount','Price','FinancialPrice','Trader','Dealer','FinancialSettle']]
    TRADES_LOTE['Side'] = TRADES_LOTE['Amount'].apply(lambda x: 'Buy' if x > 0 else 'Sell')
    TRADES_LOTE['Financeiro'] = TRADES_LOTE['Amount'] * TRADES_LOTE['Price']    
    TRADES_LOTE['Product'] = TRADES_LOTE['Product'].apply(remover_underline)
    
    #Calculando preço médio BOVESPA
    TRADES_LOTE_BOVESPA = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BOVESPA)]

    TRADES_LOTE_PM = TRADES_LOTE_BOVESPA.groupby(['Dealer','ProductClass','Product','Side']).agg(
        Quantidade_Boleta_Lote45=('Amount', 'sum'),
        Financeiro=('Financeiro', 'sum'
    )).reset_index()
    TRADES_LOTE_PM['Quantidade_Boleta_Lote45'] = abs(TRADES_LOTE_PM['Quantidade_Boleta_Lote45'])
    TRADES_LOTE_PM['PM_LOTE'] = round(abs(TRADES_LOTE_PM['Financeiro'] / TRADES_LOTE_PM['Quantidade_Boleta_Lote45']),4)

    #Calculando preço médio BMF
    TRADES_LOTE_BMF = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BMF)]
    TRADES_LOTE_BMF = TRADES_LOTE_BMF.groupby(['Dealer','ProductClass','Product','Side','Price']).agg(
        Quantidade_Boleta_Lote45=('Amount', 'sum'),
        Financeiro=('Financeiro', 'sum'
    )).reset_index()
    TRADES_LOTE_BMF['Quantidade_Boleta_Lote45'] = abs(TRADES_LOTE_BMF['Quantidade_Boleta_Lote45'])
    TRADES_LOTE_BMF['PM_LOTE'] = round(abs(TRADES_LOTE_BMF['Financeiro'] / TRADES_LOTE_BMF['Quantidade_Boleta_Lote45']),4)

    #Calculando preço médio OFF
    TRADES_LOTE_OFF = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_OFF)]
    TRADES_LOTE_OFF = TRADES_LOTE_OFF.groupby(['Dealer','ProductClass','Product','Side','Price']).agg(
        Quantidade_Boleta_Lote45=('Amount', 'sum'),
        Financeiro=('Financeiro','sum'
    )).reset_index()
    TRADES_LOTE_OFF['Quantidade_Boleta_Lote45'] = abs(TRADES_LOTE_OFF['Quantidade_Boleta_Lote45'])
    TRADES_LOTE_OFF['PM_LOTE'] = round(abs(TRADES_LOTE_OFF['Financeiro'] / TRADES_LOTE_OFF['Quantidade_Boleta_Lote45']),8)
    TRADES_LOTE_OFF = ajustar_multiplicadores(TRADES_LOTE_OFF)

    #Concatenando
    TRADES_LOTE_FINAL = pd.concat([TRADES_LOTE_PM,TRADES_LOTE_BMF,TRADES_LOTE_OFF])
    TRADES_LOTE_FINAL['FONTE'] = 'LOTE45'

    return TRADES_LOTE_FINAL[['ProductClass','Product','Side','PM_LOTE','Quantidade_Boleta_Lote45','Dealer','FONTE']]

def tratar_trades_clearing(TRADES_CLEARING):
    # Mapeando Corretoras
    TRADES_CLEARING['Entering Firm'] = TRADES_CLEARING['Entering Firm'].replace(de_para_corretoras)
    TRADES_CLEARING = TRADES_CLEARING[['Exchange', 'Symbol', 'Side', 'Price', 'Qty', 'Entering Firm']]

    # Função para tratar cada exchange
    def tratar_exchange(exchange_code):
        trades_exchange = TRADES_CLEARING[TRADES_CLEARING['Exchange'] == exchange_code].copy()
        if exchange_code == 'XBSP':
            trades_exchange['Symbol'] = trades_exchange['Symbol'].apply(lambda x: x[:-1] if x.endswith('F') else x)
            return calcular_preco_medio_clearing_bovespa(trades_exchange, exchange_code)
        else:
            return calcular_preco_medio_clearing_bmf(trades_exchange, exchange_code)
    # Tratando as exchanges BOVESPA e BMF
    trades_bovespa_pm = tratar_exchange('XBSP')
    trades_bmf_pm = tratar_exchange('XBMF')

    # Concatenando resultados
    TRADES_CLEARING = pd.concat([trades_bovespa_pm, trades_bmf_pm])
    TRADES_CLEARING['FONTE'] = 'CLEARING'
    return TRADES_CLEARING

def ajustar_multiplicadores(TRADES_LOTE_OFF):
    CLASSES_COM_PRICELOT10 = ['MXN/USD Futures - CME']
    
    cond1 = TRADES_LOTE_OFF['ProductClass'].isin(CLASSES_COM_PRICELOT10)
    TRADES_LOTE_OFF.loc[cond1,'PM_LOTE'] = round(TRADES_LOTE_OFF['PM_LOTE']*10,8)
    
    return TRADES_LOTE_OFF

def tratar_passivo_btg(PASSIVO_BTG):
    PASSIVO_BTG['Saldo Atual'] = PASSIVO_BTG['Saldo de cotas'] * PASSIVO_BTG['Valor da cota do dia']
    PASSIVO_BTG_PIVOTADO = PASSIVO_BTG.groupby(['Nome da classe/subclasse','CNPJ da classe','Nome do cotista','CPF/CNPJ do cotista','Distribuidor']).agg(
        Saldo = ('Saldo Atual','sum')
    ).reset_index()

    PASSIVO_BTG_PIVOTADO.rename(columns={
        'Nome da classe/subclasse':'Fundo',
        'CNPJ da classe':'CNPJ do Fundo',
        'Nome do cotista':'Cotista'}, inplace=True)
    return PASSIVO_BTG_PIVOTADO

def tratar_passivo_intrag(PASSIVO_INTRAG):
    PASSIVO_INTRAG_PIVOTADO = PASSIVO_INTRAG.groupby(['Nome_do_Fundo_Passivo','CNPJ_do_Fundo_Passivo','Nome_do_Cotista','CPF_CNPJ_do_Cotista','Nome_do_Distribuidor']).agg(
        Saldo = ('Valor_Patrimonio_Liquido','sum')
    ).reset_index()

    PASSIVO_INTRAG_PIVOTADO.rename(columns={
        'Nome_do_Fundo_Passivo':'Fundo',
        'CNPJ_do_Fundo_Passivo':'CNPJ do Fundo',
        'Nome_do_Cotista':'Cotista',
        'CPF_CNPJ_do_Cotista':'CPF/CNPJ do cotista',
        'Nome_do_Distribuidor':'Distribuidor'}, inplace=True)
    return PASSIVO_INTRAG_PIVOTADO

#! '''Funções responsáveis por realizar cálculos os arquivos:'''
def calcular_preco_medio_clearing_bovespa(trades, exchange):
    trades['Financeiro'] = trades['Qty'] * trades['Price']
    trades_pm = trades.groupby(['Exchange', 'Entering Firm', 'Symbol', 'Side']).agg(
        Quantidade_Operada_CLEARING=('Qty', 'sum'),
        Financeiro=('Financeiro', 'sum')
    ).reset_index()
    trades_pm['PM_CLEARING'] = trades_pm['Financeiro'] / trades_pm['Quantidade_Operada_CLEARING']
    return trades_pm[['Exchange', 'Symbol', 'Side', 'PM_CLEARING', 'Quantidade_Operada_CLEARING', 'Entering Firm']]    

def calcular_preco_medio_clearing_bmf(trades, exchange):
    trades['Financeiro'] = trades['Qty'] * trades['Price']
    trades_pm = trades.groupby(['Exchange', 'Entering Firm', 'Symbol', 'Side','Price']).agg(
        Quantidade_Operada_CLEARING=('Qty', 'sum'),
        Financeiro=('Financeiro', 'sum')
    ).reset_index()
    trades_pm['PM_CLEARING'] = trades_pm['Price']
    return trades_pm[['Exchange', 'Symbol', 'Side', 'PM_CLEARING', 'Quantidade_Operada_CLEARING', 'Entering Firm']]

def calcular_preco_medio_bofa(TRADES_OFF):

    TRADES_OFF['Financeiro'] = TRADES_OFF['QTY'] * TRADES_OFF['PM_CLEARING']
    TRADES_OFF = TRADES_OFF.groupby(['Dealer', 'Product', 'Side','PM_CLEARING']).agg(
        Quantidade_Operada_CLEARING=('QTY', 'sum'),
        Financeiro=('Financeiro', 'sum')
    ).reset_index()
    TRADES_OFF['FONTE'] = 'CLEARING'
    return TRADES_OFF

#!'''Funções de Pivot'''
def pivot_table_resumo(df,familia_de_fundos):
    # Filtra o DataFrame com base nas 'acoes' e reorganiza as colunas
    df_filtrado = df[df['TradingDesk'].isin(familia_de_fundos)][['TradingDesk','PL_D1_PCT', 'PL_MTD_PCT', 'PL_YTD_PCT', 'PL_INCEPT_PCT']]

    # Renomeia as colunas
    df_filtrado = df_filtrado.rename(columns={
        'PL_D1_PCT': 'D1',
        'PL_MTD_PCT': 'MTD',
        'PL_YTD_PCT': 'YTD',
        'PL_INCEPT_PCT': 'INCEPT'
    })

    # Cria uma tabela pivot, somando os valores e multiplicando por 100
    pivot_df = pd.pivot_table(df_filtrado, index='TradingDesk', aggfunc='sum') * 100
    pivot_df = pivot_df[['D1','MTD','YTD','INCEPT']]
    return pivot_df

def pivot_table_attribution(df,fundo_desejado):
    # Filtra o DataFrame com base nas 'acoes' e reorganiza as colunas
    df_filtrado = df[df['TradingDesk']==fundo_desejado][['Quebra_Relatorio','Setores','PL_D1_PCT', 'PL_MTD_PCT', 'PL_YTD_PCT', 'PL_INCEPT_PCT']]

    # Renomeia as colunas
    df_filtrado = df_filtrado.rename(columns={
        'PL_D1_PCT': 'D1',
        'PL_MTD_PCT': 'MTD',
        'PL_YTD_PCT': 'YTD',
        'PL_INCEPT_PCT': 'INCEPT'
    })
    # Cria uma tabela pivot, somando os valores e multiplicando por 100
    pivot_df = pd.pivot_table(df_filtrado, index=['Quebra_Relatorio','Setores'], aggfunc='sum') * 100
    pivot_df.reset_index(inplace=True)
    pivot_df['Quebra_Relatorio'] = pivot_df['Quebra_Relatorio'].where(pivot_df['Quebra_Relatorio'].ne(pivot_df['Quebra_Relatorio'].shift()))
    pivot_df = round(pivot_df[['Quebra_Relatorio','Setores','D1','MTD','YTD','INCEPT']],2)
    pivot_df = pivot_df.fillna('-')
    return pivot_df

def base_tabela_final(TRADES_CLEARING,TRADES_LOTE,TRADES_OFF,DE_PARA_B3):
    # Renomeando as colunas 'Symbol' para 'Product' e 'Entering Firm' para 'Dealer'
    TRADES_CLEARING.rename(columns={'Symbol': 'Product', 'Entering Firm': 'Dealer'}, inplace=True)
    TRADES_CLEARING['Product'] = TRADES_CLEARING['Product'].replace(DE_PARA_B3)
    # Fazendo o merge das duas tabelas usando Product, Side e Dealer como chave
    
    # BOVESPA
    TRADES_LOTE_BOVESPA = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BOVESPA)]
    TRADES_CLEARING_BOVESPA = TRADES_CLEARING[TRADES_CLEARING['Exchange'] == 'XBSP']
    df_comparacao_BOVESPA = pd.merge(TRADES_CLEARING_BOVESPA, TRADES_LOTE_BOVESPA, how='outer', on=['Product', 'Side', 'Dealer'])
    
    #BMF
    TRADES_LOTE_BMF = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BMF)]
    TRADES_CLEARING_BMF = TRADES_CLEARING[TRADES_CLEARING['Exchange'] == 'XBMF']
    TRADES_CLEARING_BMF['PM'] = TRADES_CLEARING_BMF['PM_CLEARING']
    TRADES_LOTE_BMF['PM'] = TRADES_LOTE_BMF['PM_LOTE']
    df_comparacao_BMF = pd.merge(TRADES_CLEARING_BMF, TRADES_LOTE_BMF, how='outer', on=['Product', 'Side', 'Dealer','PM'])

    #OFF
    TRADES_LOTE_OFF = TRADES_LOTE[TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_OFF)]
    TRADES_LOTE_OFF['PM'] = TRADES_LOTE_OFF['PM_LOTE']
    TRADES_OFF['PM'] = TRADES_OFF['PM_CLEARING']
    df_comparacao_OFF = pd.merge(TRADES_OFF,TRADES_LOTE_OFF, how='outer',on=['Product', 'Side', 'Dealer','PM'])

    df_comparacao = pd.concat([df_comparacao_BOVESPA,df_comparacao_BMF,df_comparacao_OFF])
    df_comparacao = df_comparacao.fillna(0)

    # Calculando as diferenças de quantidade e preço médio
    df_comparacao['Diferença_Quantidade'] = df_comparacao['Quantidade_Boleta_Lote45'] - df_comparacao['Quantidade_Operada_CLEARING']
    df_comparacao['Diferença_PM'] = df_comparacao['PM_LOTE'] - df_comparacao['PM_CLEARING']
    
    return df_comparacao

#!'''Funções relatório Passivo'''
def passivo(PASSIVO_BTG,PASSIVO_INTRAG):
    st.title('Análise Passivo - Tenax Capital')
    PASSIVO_CONCATENADO = pd.concat([PASSIVO_BTG,PASSIVO_INTRAG])
    PASSIVO_CONCATENADO = load_mapeamento_passivo(PASSIVO_CONCATENADO)

    with st.expander('Linhas sem Mapeamento'):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('Para Mapeamento das colunas "Estratégia", "Classificação" e "Nome Padrão", preencher o arquivo abaixo:')
            st.caption('T:\OPERACOES\DEV\\10.PASSIVO_TENAX\Bases_Auxiliares\Passivo_Mapeamento_Fundos.xlsx')
            
            st.dataframe(
                PASSIVO_CONCATENADO[['Fundo','CNPJ do Fundo','Estratégia','Classificação','Nome Padrão']][PASSIVO_CONCATENADO[['Estratégia','Classificação','Nome Padrão']].isna().any(axis=1)].drop_duplicates(),
                use_container_width=True,
                hide_index=True)
        with col2:
            st.markdown('Para Mapeamento das colunas "Alocador Final", "Classificação do Cliente" e "Classificação do Cotista", preencher o arquivo abaixo:')
            st.caption('T:\OPERACOES\DEV\\10.PASSIVO_TENAX\Bases_Auxiliares\Passivo_Mapeamento_Passivo.xlsx')
            st.dataframe(
            PASSIVO_CONCATENADO[['Cotista', 'CPF/CNPJ do cotista','Distribuidor','Alocador Final','Classificação do Cliente','Classificação do Cotista']][PASSIVO_CONCATENADO[['Alocador Final','Classificação do Cliente','Classificação do Cotista']].isna().any(axis=1)].drop_duplicates(),
                use_container_width=True,
                hide_index=True)
    
    @st.cache_data
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode("utf-8")

    csv = convert_df(PASSIVO_CONCATENADO)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name="large_df.csv",
        mime="text/csv",
    )

    tab_posicao_mensal, tab_consolidado_fundo, tab_consolidado_cotista = st.tabs(['Consolidado Mensal','Consolidado Fundo','Consolidado Cotista'])
    MASTERS = ['Tenax Acoes Master FIA','Tenax Acoes Master Institucional FIA','Tenax Macro Master FIM','Tenax Total Return Master FIM','Tenax Total Return Prev Master FIFE','Tenax TR Master FIA','Geri Tenax FI RF','Synta Tenax FI RF']
    PASSIVO_CONCATENADO = PASSIVO_CONCATENADO.fillna('Sem Mapeamento')
    PASSIVO_CONCATENADO = PASSIVO_CONCATENADO[~PASSIVO_CONCATENADO['Nome Padrão'].isin(MASTERS)]
    
    with tab_posicao_mensal:
        st.title('Posição Mensal')
        st.subheader('Alocador x Estratégia')
        tabela_Alocador_x_Estrategia = pd.pivot_table(PASSIVO_CONCATENADO, values='Saldo',index='Alocador Final',columns='Classificação', aggfunc='sum').round(0)
        tabela_Alocador_x_Estrategia['Total'] = tabela_Alocador_x_Estrategia.sum(axis=1)
        tabela_Alocador_x_Estrategia['% Total'] = round(tabela_Alocador_x_Estrategia.sum(axis=1)/np.sum(tabela_Alocador_x_Estrategia['Total'])*100,2)
        st.dataframe(tabela_Alocador_x_Estrategia,use_container_width=True)
        st.divider()
        # Tabelas consolidadas
        
        st.title('Consolidado')
        col1,col2,col3 = st.columns(3)
        with col1:
            st.subheader('PL x Estratégia')
            tabela_PL_x_Estrategia = PASSIVO_CONCATENADO.groupby('Estratégia').agg(PL_Total=('Saldo','sum')).reset_index()
            
            # # Converte 'PL_Total' e outras colunas para tipo numérico, substituindo erros por NaN
            # tabela_PL_x_Estrategia['PL_Total'] = pd.to_numeric(tabela_PL_x_Estrategia['PL_Total'], errors='coerce')

            # Realiza a soma e divisão com tratamento para valores NaN
            tabela_PL_x_Estrategia['% PL'] =tabela_PL_x_Estrategia.sum(axis=1) / np.sum(tabela_PL_x_Estrategia['PL_Total']) * 100
            st.dataframe(tabela_PL_x_Estrategia,hide_index=True)
        
        with col2:
            st.subheader('Fundos x PL')
            tabela_Fundo_x_PL = PASSIVO_CONCATENADO.groupby('Classificação').agg(PL_Total=('Saldo','sum')).reset_index()
            # Converte 'PL_Total' e outras colunas para tipo numérico, substituindo erros por NaN
            tabela_Fundo_x_PL['PL_Total'] = pd.to_numeric(tabela_Fundo_x_PL['PL_Total'], errors='coerce')

            # Cálculo do percentual do PL total
            tabela_Fundo_x_PL['% PL'] = round(
                tabela_Fundo_x_PL.sum(axis=1)/np.sum(tabela_Fundo_x_PL['PL_Total'])*100
                ,2
            )
            st.dataframe(tabela_Fundo_x_PL,hide_index=True)
        
        with col3:
            st.subheader('Passivo x PL')
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO.groupby('Classificação do Cliente').agg(PL_Total=('Saldo','sum')).reset_index()

            # Converte 'PL_Total' e outras colunas para tipo numérico, substituindo erros por NaN
            tabela_Passivo_x_PL['PL_Total'] = pd.to_numeric(tabela_Passivo_x_PL['PL_Total'], errors='coerce')

            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True)

        st.divider()

        st.title('Mapa do Passivo')
        tabela_Passivo_x_Estrategia = pd.pivot_table(PASSIVO_CONCATENADO, values='Saldo',index='Classificação do Cliente',columns='Classificação', aggfunc='sum').round(0)
        tabela_Passivo_x_Estrategia['Total'] = tabela_Passivo_x_Estrategia.sum(axis=1)
        tabela_Passivo_x_Estrategia['% Total'] = round(tabela_Passivo_x_Estrategia.sum(axis=1)/np.sum(tabela_Passivo_x_Estrategia['Total'])*100,2)
        st.dataframe(tabela_Passivo_x_Estrategia,use_container_width=True)
        st.divider()

        col4, col5 = st.columns(2)

        with col4:           
            st.subheader('Distribuição PL x Fundo')
            # Criando o gráfico de pizza
            plt.figure(figsize=(10, 10))
            wedges, texts, autotexts = plt.pie(
                tabela_Fundo_x_PL['% PL'],  # Valores percentuais para as fatias
                labels=tabela_Fundo_x_PL['Classificação'],  # Labels das fatias
                autopct='%1.1f%%',  # Mostra o percentual dentro da fatia
                startangle=90  # Rotaciona o gráfico para começar do topo
            )
            # plt.title('')

            # Adicionando a legenda
            plt.legend(wedges, tabela_Fundo_x_PL['Classificação'], title="Classificação", loc="lower left", bbox_to_anchor=(1, 0))

            # Exibindo o gráfico
            st.pyplot(plt)
            
        with col5:
            st.subheader('Distribuição PL x Tipo de cliente')
            # Criando o gráfico de pizza
            plt.figure(figsize=(10, 10))
            wedges, texts, autotexts = plt.pie(
                tabela_Passivo_x_PL['% PL'],  # Valores percentuais para as fatias
                labels=tabela_Passivo_x_PL['Classificação do Cliente'],  # Labels das fatias
                autopct='%1.1f%%',  # Mostra o percentual dentro da fatia
                startangle=90  # Rotaciona o gráfico para começar do topo
            )
            # plt.title('Distribuição do PL por Fundo')

            # Adicionando a legenda
            plt.legend(wedges, tabela_Passivo_x_PL['Classificação do Cliente'], title="Classificação do Cliente", loc="lower left", bbox_to_anchor=(1, 0))

            # Exibindo o gráfico
            st.pyplot(plt)
            
            # PASSIVO_CONCATENADO.columns

    with tab_consolidado_fundo:
        st.subheader('Passivo x PL')
        FILTROS_DISPONIVEIS = ['Alocador Final','Distribuidor','Classificação do Cliente','Classificação do Cotista']
        filtro_visualizacao_col = st.selectbox('Tipo de visualização: ',FILTROS_DISPONIVEIS)
        col1, col2, col3,col4,col5 = st.columns(5)

        with col1: 
            st.title('Macro') 
            filtro_col1 = st.selectbox('Seleciona o fundo: ',[item for item in macro if item not in MASTERS])
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO[PASSIVO_CONCATENADO['Nome Padrão']==filtro_col1].groupby(filtro_visualizacao_col).agg(
                PL_Total=('Saldo','sum')
                                                                                   ).reset_index()
            
            tabela_Passivo_x_PL['PL_Total'] = round(tabela_Passivo_x_PL['PL_Total'],0)
            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True,use_container_width=True)
            
            
        with col2: 
            st.title('Total Return')
            filtro_col2 = st.selectbox('Seleciona o fundo: ',[item for item in total_return if item not in MASTERS])
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO[PASSIVO_CONCATENADO['Nome Padrão']==filtro_col2].groupby(filtro_visualizacao_col).agg(
                PL_Total=('Saldo','sum')
                                                                                   ).reset_index()
            
            tabela_Passivo_x_PL['PL_Total'] = round(tabela_Passivo_x_PL['PL_Total'],0)
            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True,use_container_width=True)

        with col3: 
            st.title('Ações')
            filtro_col3 = st.selectbox('Seleciona o fundo: ',[item for item in acoes if item not in MASTERS])
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO[PASSIVO_CONCATENADO['Nome Padrão']==filtro_col3].groupby(filtro_visualizacao_col).agg(
                PL_Total=('Saldo','sum')
                                                                                   ).reset_index()
            
            tabela_Passivo_x_PL['PL_Total'] = round(tabela_Passivo_x_PL['PL_Total'],0)
            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True,use_container_width=True)

        with col4: 
            st.title('Renda Fixa')
            filtro_col4 = st.selectbox('Seleciona o fundo: ',[item for item in renda_fixa if item not in MASTERS])
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO[PASSIVO_CONCATENADO['Nome Padrão']==filtro_col4].groupby(filtro_visualizacao_col).agg(
                PL_Total=('Saldo','sum')
                                                                                   ).reset_index()
            
            tabela_Passivo_x_PL['PL_Total'] = round(tabela_Passivo_x_PL['PL_Total'],0)
            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True,use_container_width=True)

        with col5: 
            st.title('Crédito')
            filtro_col5 = st.selectbox('Seleciona o fundo: ',[item for item in credito_privado if item not in MASTERS])
            tabela_Passivo_x_PL = PASSIVO_CONCATENADO[PASSIVO_CONCATENADO['Nome Padrão']==filtro_col5].groupby(filtro_visualizacao_col).agg(
                PL_Total=('Saldo','sum')
                                                                                   ).reset_index()
            
            tabela_Passivo_x_PL['PL_Total'] = round(tabela_Passivo_x_PL['PL_Total'],0)
            # Cálculo do percentual do PL total
            tabela_Passivo_x_PL['% PL'] = round(tabela_Passivo_x_PL.sum(axis=1)/np.sum(tabela_Passivo_x_PL['PL_Total'])*100,2)
            st.dataframe(tabela_Passivo_x_PL,hide_index=True,use_container_width=True)

#! '''Código base para o Batimento de Trades'''
def batimento_de_trades(TRADES_LOTE,TRADES_CLEARING,TRADES_OFF,DE_PARA_B3):
    
    st.title('Batimento de Trades')
    tabela_batimento = base_tabela_final(TRADES_CLEARING,TRADES_LOTE,TRADES_OFF,DE_PARA_B3)

    total_bmf = np.sum(TRADES_CLEARING['Quantidade_Operada_CLEARING'][TRADES_CLEARING['Exchange']=='XBMF'])
    total_bovespa = np.sum(TRADES_CLEARING['Quantidade_Operada_CLEARING'][TRADES_CLEARING['Exchange']=='XBSP'])
    total_off   = np.sum(TRADES_OFF['Quantidade_Operada_CLEARING'][TRADES_OFF['Dealer']=='BOFA'])

    total_bmf_lote = np.sum(TRADES_LOTE['Quantidade_Boleta_Lote45'][TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BMF)])
    total_bovespa_lote = np.sum(TRADES_LOTE['Quantidade_Boleta_Lote45'][TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_BOVESPA)])
    total_OFF_lote = np.sum(TRADES_LOTE['Quantidade_Boleta_Lote45'][TRADES_LOTE['ProductClass'].isin(CLASSE_PRODUTOS_OFF)])

    # trader = st.selectbox("Escolha o Trader",['XBMF','BVSP', 'OFFSHORE','TODOS'])
    col1,col2,col3 = st.columns(3)
    with col1:
        st.metric(label='Total BMF',value=total_bmf,delta=round(total_bmf_lote/total_bmf,2)*100)
    with col2:
        st.metric(label='Total BOVESPA',value=total_bovespa,delta=round(total_bovespa_lote/total_bovespa,2)*100)
    with col3:
        st.metric(label='Total OFFSHORE',value=total_off,delta=round(total_OFF_lote/total_off,2)*100)

    col4,col5,col6,col7 = st.columns(4)
    with col4:
        mercado = st.selectbox("Escolha o mercado",['TODOS','XBMF','XBSP', 'OFFSHORE'])
    with col5:
        filtrar_erros = st.selectbox('Status:',['TODOS','OK','ERRO'])
    with col6:
        corretoras = ['TODAS']
        corretoras.extend(set(tabela_batimento['Dealer']))
        filtrar_corretoras = st.selectbox('Corretora',corretoras)

    if mercado == 'TODOS':
        resumo_trades = tabela_batimento
    elif mercado =='OFFSHORE':
        resumo_trades = tabela_batimento[tabela_batimento['ProductClass'].isin(CLASSE_PRODUTOS_OFF)]
    elif mercado =='TÍTULO PÚBLICO':
        resumo_trades = tabela_batimento[tabela_batimento['ProductClass'].isin(CLASSE_PRODUTOS_TITULOS_PUBLICOS)]
    elif mercado =='CRÉDITO':
        resumo_trades = tabela_batimento[tabela_batimento['ProductClass'].isin(CLASSE_PRODUTOS_CREDITO)]
    else:
        resumo_trades = tabela_batimento[tabela_batimento['Exchange'] == mercado]

    if filtrar_erros=='ERRO':
        resumo_trades = resumo_trades[((round(resumo_trades['Diferença_Quantidade'],4)!=0) | (round(resumo_trades['Diferença_PM'],4)!=0))]
    if filtrar_erros=='OK':
        resumo_trades = resumo_trades[(resumo_trades['Diferença_Quantidade']==0) & (resumo_trades['Diferença_PM']==0)]
    
    if filtrar_corretoras!= 'TODAS':
        resumo_trades = resumo_trades[resumo_trades['Dealer']==filtrar_corretoras]

    resumo_trades.rename(columns={'Quantidade_Boleta_Lote45': 'Qtde Lote45','Quantidade_Operada_CLEARING':'Qtde Clearing','Diferença_Quantidade':'Dif Qtde','PM_LOTE':'Preço Médio LOTE45','PM_CLEARING':'Preço Médio Clearing','Diferença_PM':'Dif no Preço Médio'}, inplace=True)
    # Exibindo o DataFrame comparativo
    st.dataframe(resumo_trades[['Product', 'Side', 'Dealer', 'Qtde Lote45', 'Qtde Clearing', 'Dif Qtde', 'Preço Médio LOTE45', 'Preço Médio Clearing', 'Dif no Preço Médio']],hide_index=True,use_container_width=True,)

def render_sidebar(auth_data):

    if auth_data:
        st.sidebar.title('Escolha a funcionalidade')
        funcionalidades = ['Batimento de Trades', 'Passivo Tenax']
        return st.sidebar.selectbox('Relatórios disponíveis:', funcionalidades)
    else:
        st.sidebar.write("Você não está conectado")
        return None

def handle_batimento_de_trades(date):
    data_selecionada_DBY = date.strftime('%Y%m%d')
    TRADES_LOTE = tratar_trades_lote(load_trades_lote(data_selecionada_DBY))
    TRADES_CLEARING = tratar_trades_clearing(load_trades_clearing(data_selecionada_DBY))
    TRADES_OFF = tratar_trades_clearing_off(load_trades_off(data_selecionada_DBY))
    DE_PARA_B3 = load_de_para_b3(data_selecionada_DBY)
    batimento_de_trades(TRADES_LOTE, TRADES_CLEARING,TRADES_OFF,DE_PARA_B3)

def handle_passivo(date):
    data_selecionada_DBY = date.strftime('%Y%m%d')
    PASSIVO_BTG = tratar_passivo_btg(load_passivo_btg(data_selecionada_DBY))
    PASSIVO_INTRAG = tratar_passivo_intrag(load_passivo_intrag(data_selecionada_DBY))
    passivo(PASSIVO_BTG,PASSIVO_INTRAG)

def main():
    st.sidebar.image('assinaturas_TENAX_RGB-02-removebg-preview.png')
    with st.sidebar:
        auth_data = Msal.initialize_ui(
            client_id=client_id,
            authority=authority,
            scopes=["User.Read"],
            connecting_label="Connecting",
            disconnected_label="Disconnected",
            sign_in_label="Sign in",
            sign_out_label="Sign out"
        )

    funcionalidade = render_sidebar(auth_data)

    if funcionalidade == 'Batimento de Trades':
        date = st.sidebar.date_input("Informe a data desejada:", format='DD-MM-YYYY')
        handle_batimento_de_trades(date)
    elif funcionalidade == 'Passivo Tenax':
        date = st.sidebar.date_input("Informe a data desejada:", format='DD-MM-YYYY')
        handle_passivo(date)        
    if st.sidebar.button('Reprocessar'):
        st.rerun()

if __name__ == "__main__":
    main()


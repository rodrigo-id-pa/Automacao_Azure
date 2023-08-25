### INCIANDO O SCRIPT ###
from config import data_hora_atual, registrar_print
from getToken import check_credentials
from functions import *
from variables import id_pasta_log, lista_prints
import traceback
import pandas as pd
import datetime
import pyodbc
import io
import os
from ftplib import FTP
from py7zr import SevenZipFile
pd.options.mode.chained_assignment = None

# credenciais de acesso do Portal do Azure
user_id, tenant_id, client_id, client_credential = check_credentials()
data_ini = data_hora_atual()
registrar_print("FLOW FUNCTIONS REALIZADO COM SUCESSO")
pasta_id_output = ""  # id da pasta output do RPA
criar_pasta_log(user_id, pasta_id_output)  # criando a pasta log
registrar_print("INICIOU FLOW MAIN")

# OBS: Verifique no script Rascunho_RPA como acessar os diretorios
try:
    pasta_id_input = ""  # pasta Input
    listar_conteudo_pasta(user_id, pasta_id_input)

    format_ = 'xlsx'
    # id do arquivo xlsx na pasta Input
    arquivo_id_csv = ""
    pathInput = ler_arquivo(user_id, arquivo_id_csv, format_)
    dataframes = {}                    # Dicionário para armazenar os DataFrames
    data = datetime.datetime.now()           # data atual
    ano = int(data.strftime("%Y"))  # pegando apenas o ano como inteiro
    meses = ["01", "02", "03", "04", "05", "06", "07",
             "08", "09", "10", "11", "12"]       # array de meses
    url = pathInput['Value'][0]  # URL do servidor FTP
    diretorio = f"{pathInput['Value'][1]}{ano}"  # Diretório desejado
    ftp = FTP(url)
    ftp.login()  # Realiza o login no servidor FTP
    ftp.cwd(diretorio)  # Navega para o diretório desejado
    diretorios = ftp.nlst()  # Obtém a lista de diretórios no diretório atual
    # Ordena a lista de diretórios em ordem crescente
    diretorios_ordenados = sorted(diretorios)
    ultimo_diretorio = diretorios_ordenados[-1]  # Seleciona o último diretório
    ftp.cwd(ultimo_diretorio)  # Acessando a pasta atual
    arquivos = ftp.nlst()  # listando os arquivos

    # percorrendo cada arquivo
    for arquivo in arquivos:
        if arquivo.endswith('.7z'):  # Verifica se o arquivo é do tipo 7z
            with io.BytesIO() as mem_file:
                # Baixa o arquivo 7z em memória
                ftp.retrbinary('RETR ' + arquivo, mem_file.write)
                mem_file.seek(0)  # Reposiciona o ponteiro do arquivo no início
                with SevenZipFile(mem_file, mode='r') as archive:
                    archive.extractall()  # Extrai o conteúdo do arquivo 7z
                nome_arquivo_txt = os.path.splitext(
                    arquivo)[0] + '.txt'  # Obtém o nome do arquivo txt
                with open(nome_arquivo_txt, 'r', encoding='UTF-8') as file:
                    # Processa o arquivo txt conforme necessário, por exemplo, lendo como um DataFrame:
                    df = pd.read_csv(file, sep=';')
                # Remove o arquivo txt extraído do 7z
                os.remove(nome_arquivo_txt)
                # Armazena o DataFrame no dicionário
                nome_variavel = os.path.splitext(arquivo)[0].replace(
                    'CAGEDEST_', '')  # Define o nome da variável
                dataframes[nome_variavel] = df
    ftp.quit()  # Encerra a conexão com o servidor FTP

    # Obter a lista de dataframes
    lista_dataframes = list(dataframes.values())

    # Atribuir cada dataframe a uma variável pelo índice correspondente
    cagedxc = lista_dataframes[0]
    cagedfor = lista_dataframes[1]
    cagedmov = lista_dataframes[2]

    # remover acentuação dos dataframes
    cagedmov = remover_acentuacao_titulos(cagedmov)
    cagedxc = remover_acentuacao_titulos(cagedxc)
    cagedfor = remover_acentuacao_titulos(cagedfor)
    print("dataframes montados")

    dataframes = [cagedmov, cagedfor, cagedxc]  # lista com os dataframes
    colunas_numericas = ['idade']              # coluna idade
    # coluna valorsalariofixo', 'salario', 'horascontratuais'
    colunas_texto = ['valorsalariofixo', 'salario', 'horascontratuais']

    for df in dataframes:        # percorrendo os dataframes
        for coluna in colunas_numericas:          # percorrendo a coluna idade
            # substituindo os valores nulos por 0
            df[coluna].fillna(0, inplace=True)
            # convertendo coluna idade de float para int
            df[coluna] = df[coluna].astype('int64')
            df['mesano_referencia'] = ultimo_diretorio[-2:] + \
                ultimo_diretorio[:-2]  # Pegando mês de referência do FTP

        for coluna in colunas_texto:  # percorrendo as colunas tipo texto
            # substituindo os valores nulos por vazios ''
            df[coluna].fillna('', inplace=True)

    # substituindo o 0 por campo vazios da coluna idade para seguir o padrão as outras colunas
    cagedmov.idade = cagedmov.idade.replace(0, '')
    cagedxc.idade = cagedxc.idade.replace(0, '')
    cagedfor.idade = cagedfor.idade.replace(0, '')

    # criando coluna com data e hora da carga
    data_today = datetime.datetime.today().strftime("%A %d %B %y %H:%M")
    cagedmov['data_hora_carga'] = datetime.datetime.strptime(
        data_today, "%A %d %B %y %H:%M")
    cagedxc['data_hora_carga'] = datetime.datetime.strptime(
        data_today, "%A %d %B %y %H:%M")
    cagedfor['data_hora_carga'] = datetime.datetime.strptime(
        data_today, "%A %d %B %y %H:%M")

    # INJETANDO OS DADOS NO BANCO
    registrar_print('INSERINDO OS DADOS NO BANCO')

    # Conectando ao banco de dados
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=MEU_SERVIDOR;DATABASE=MEU_BANCO;Trusted_Connection=yes;TrustServerCertificate=Yes;Encrypt=no;')

    # Nome das tabelas no banco de dados
    tb_cagedmov = 'cagedmov'
    tb_cagedexc = 'cagedexc'
    tb_cagedfor = 'cagedfor'

    # Criando um cursor para executar as operações no banco de dados
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Limpando as tabelas
    tabelas = [tb_cagedmov, tb_cagedexc, tb_cagedfor]

    # Verificar e criar as tabelas se não existirem
    for tabela in tabelas:
        table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabela}'"
        cursor.execute(table_exists_query)
        table_exists = cursor.fetchone()[0]
        if table_exists == 0:
            if tabela == tb_cagedexc:
                create_table_query = f"""
                              CREATE TABLE [dbo].{tabela}(
                                    [competenciamov] [varchar](50) NULL,
                                    [regiao] [varchar](50) NULL,
                                    [uf] [varchar](50) NULL,
                                    [municipio] [varchar](50) NULL,
                                    [secao] [varchar](50) NULL,
                                    [subclasse] [varchar](50) NULL,
                                    [saldomovimentacao] [varchar](50) NULL,
                                    [cbo2002ocupacao] [varchar](50) NULL,
                                    [categoria] [varchar](50) NULL,
                                    [graudeinstrucao] [varchar](50) NULL,
                                    [idade] [varchar](50) NULL,
                                    [horascontratuais] [varchar](50) NULL,
                                    [racacor] [varchar](50) NULL,
                                    [sexo] [varchar](50) NULL,
                                    [tipoempregador] [varchar](50) NULL,
                                    [tipoestabelecimento] [varchar](50) NULL,
                                    [tipomovimentacao] [varchar](50) NULL,
                                    [tipodedeficiencia] [varchar](50) NULL,
                                    [indtrabintermitente] [varchar](50) NULL,
                                    [indtrabparcial] [varchar](50) NULL,
                                    [salario] [varchar](50) NULL,
                                    [tamestabjan] [varchar](50) NULL,
                                    [indicadoraprendiz] [varchar](50) NULL,
                                    [origemdainformacao] [varchar](50) NULL,
                                    [competenciadec] [varchar](50) NULL,
                                    [competenciaexc] [varchar](50) NULL,
                                    [indicadordeexclusao] [varchar](50) NULL,
                                    [indicadordeforadoprazo] [varchar](50) NULL,
                                    [unidadesalariocodigo] [varchar](50) NULL,
                                    [valorsalariofixo] [varchar](50) NULL,
                                    [mesano_referencia] [varchar](10) NULL,
                                    [data_hora_carga] [smalldatetime] NULL
                              ) ON [PRIMARY]
                              ALTER TABLE [dbo].{tabela} ADD DEFAULT (getdate()) FOR [data_hora_carga]
                        """
                cursor.execute(create_table_query)
                print(f"Tabela {tabela} criada com sucesso.")
            else:
                if tabela == tb_cagedmov or tabela == tb_cagedfor:
                    create_table_query = f"""
                                    CREATE TABLE [dbo].{tabela}(
                                          [competenciamov] [varchar](50) NULL,
                                          [regiao] [varchar](50) NULL,
                                          [uf] [varchar](50) NULL,
                                          [municipio] [varchar](50) NULL,
                                          [secao] [varchar](50) NULL,
                                          [subclasse] [varchar](50) NULL,
                                          [saldomovimentacao] [varchar](50) NULL,
                                          [cbo2002ocupacao] [varchar](50) NULL,
                                          [categoria] [varchar](50) NULL,
                                          [graudeinstrucao] [varchar](50) NULL,
                                          [idade] [varchar](50) NULL,
                                          [horascontratuais] [varchar](50) NULL,
                                          [racacor] [varchar](50) NULL,
                                          [sexo] [varchar](50) NULL,
                                          [tipoempregador] [varchar](50) NULL,
                                          [tipoestabelecimento] [varchar](50) NULL,
                                          [tipomovimentacao] [varchar](50) NULL,
                                          [tipodedeficiencia] [varchar](50) NULL,
                                          [indtrabintermitente] [varchar](50) NULL,
                                          [indtrabparcial] [varchar](50) NULL,
                                          [salario] [varchar](50) NULL,
                                          [tamestabjan] [varchar](50) NULL,
                                          [indicadoraprendiz] [varchar](50) NULL,
                                          [origemdainformacao] [varchar](50) NULL,
                                          [competenciadec] [varchar](50) NULL,
                                          [indicadordeforadoprazo] [varchar](50) NULL,
                                          [unidadesalariocodigo] [varchar](50) NULL,
                                          [valorsalariofixo] [varchar](50) NULL,
                                          [mesano_referencia] [varchar](10) NULL,
                                          [data_hora_carga] [smalldatetime] NULL
                                    ) ON [PRIMARY]
                                    ALTER TABLE [dbo].{tabela} ADD DEFAULT (getdate()) FOR [data_hora_carga]
                              """
                    cursor.execute(create_table_query)
                    print(f"Tabela {tabela} criada com sucesso.")
        else:
            print(f"Tabela {tabela} já existe no banco de dados.")

    # Itera sobre cada tabela na lista
    for tabela in tabelas:
        cursor.execute(f'TRUNCATE TABLE {tabela}')

    # Chamar a função de inserção para cada DataFrame e tabela correspondente
    inserir_dados_cagedexc(cursor, tb_cagedexc, cagedxc)
    inserir_dados_cagedfor(cursor, tb_cagedfor, cagedfor)
    inserir_dados_cagedmov(cursor, tb_cagedmov, cagedmov)

    conn.commit()  # Confirmando as alterações no banco de dados
    conn.close()  # Fechando a conexão com o banco de dados
    print("dados na tabela CAGEDMOV injetado com sucesso.")
    registrar_print("DADOS INJETADOS COM SUCESSO.")

    # criando o txt log dentro da pasta log
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)
except Exception as e:
    data_error = data_hora_atual()
    traceback_str = traceback.format_exc()
    registrar_print(f"Ocorreu um erro:\n{traceback_str}, {data_error}, {e}")
    criar_log(user_id, pasta_id=id_pasta_log[0], lista_prints=lista_prints)

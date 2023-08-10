### INCIANDO O SCRIPT ###
from variables import id_pasta_log
from config import registrar_print, data_hora_atual
from getToken import check_credentials
import requests
import pandas as pd
from io import BytesIO
import msal
import tabula
from unidecode import unidecode
pd.options.mode.chained_assignment = None  # ignorar mensagens do pandas

# Obter as credenciais usando a função get_credentials
user_id, tenant_id, client_id, client_credential = check_credentials()
registrar_print("INICIOU O FLOW FUNCTIONS")

# função para obter o token do portal azure


def token():
    authority_url = f'https://login.microsoftonline.com/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=client_id,
        client_credential=client_credential
    )
    token = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"])
    return token

# obter o ID do drive (unidade) do OneDrive associado a um usuário específico no Microsoft Graph


def obter_drive_id(user_id):
    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if 'value' in data and len(data['value']) > 0:
            # Obtém o ID do primeiro drive (OneDrive pessoal) do usuário
            drive_id = data['value'][0]['id']
            return drive_id
        else:
            registrar_print("Usuário não tem nenhum drive associado.")
            return None
    else:
        registrar_print("Erro ao obter os drives do usuário.")
        return None

# função para ler todos os conteudos e pastas do oneddrive


def listar_conteudo_pasta(user_id, pasta_id):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        for item in data['value']:
            if 'folder' in item:
                print(f"Pasta: {item['name']} - ID: {item['id']}")
            else:
                print(
                    f"Arquivo: {item['name']} - ID: {item['id']} - Link de Download: {item['@microsoft.graph.downloadUrl']}")
    else:
        registrar_print("Erro ao listar o conteúdo da pasta.")

# função para ler os arquivos pdf, xlsx, csv ou txt


def ler_arquivo(user_id, arquivo_id, format_, delimitador=None):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        return

    token_info = token()
    if not token_info:
        print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        if format_ == 'csv':
            csv_data = response.content.decode('utf-8')
            df_csv = pd.read_csv(pd.compat.StringIO(
                csv_data), delimiter={delimitador})
            return df_csv
        elif format_ == 'xlsx':
            df_xlsx = pd.read_excel(BytesIO(response.content))
            return df_xlsx
        elif format_ == 'pdf':
            try:
                pdf_data = response.content
                dfs = tabula.read_pdf(
                    pdf_data, pages='all', multiple_tables=True)
                lista_dataframes = []    # Criar uma lista vazia para armazenar os DataFrames
                for i, df in enumerate(dfs, start=1):
                    print(f"Tabela {i}:")
                    print(df)
                    # Adicionar o DataFrame à lista
                    lista_dataframes.append(df)
                return lista_dataframes
            except Exception as e:
                print("Erro ao extrair tabelas do PDF:", e)
                return None
        elif format_ == 'txt':
            txt_data = response.content.decode('utf-8')
            txt_df = pd.read_table(pd.compat.StringIO(txt_data))
            return txt_df
        else:
            print(f"Formato {format_} não suportado.")
            return None
    else:
        print(
            f"Erro ao baixar o arquivo no formato {format_}: {response.status_code}, {response.text}")
        return None

# função para criar pasta log no output


def criar_pasta_log(user_id, pasta_id):
    global id_pasta_log
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return None

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return None

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(access_token)}

    data_hora = data_hora_atual()

    # Verificar se a pasta "log" já existe e removê-la, se for o caso
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code != 200:
        registrar_print("Erro ao listar itens da pasta pai.")
        return None

    for item in response_list_children.json().get('value', []):
        if item['name'] == data_hora:
            url_delete_folder = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{item['id']}"
            response_delete_folder = requests.delete(
                url_delete_folder, headers=headers)
            if response_delete_folder.status_code != 204:
                registrar_print("Erro ao excluir a pasta log.")
                return None

    # Criar a pasta "log" dentro da pasta pai com o nome da data e hora local
    url_create_folder = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": data_hora,
        "folder": {}
    }
    response_create_folder = requests.post(
        url_create_folder, headers=headers, json=data)

    if response_create_folder.status_code == 201:
        id_pasta_log.clear()
        pasta_log_id = response_create_folder.json().get('id')
        print(f"Pasta 'log' criada com sucesso no OneDrive.")
        return id_pasta_log.append(pasta_log_id)
    else:
        registrar_print("Erro ao criar a pasta log.")
        return None

# função para criar o log TXT


def criar_log(user_id, pasta_id, lista_prints):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    data_hora = data_hora_atual()
    nome_arquivo = f"{data_hora}.txt"

    # Verificar se o arquivo já existe na pasta pai
    url_list_children = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children?$select=id,name"
    response_list_children = requests.get(url_list_children, headers=headers)

    if response_list_children.status_code == 200:
        for item in response_list_children.json().get('value', []):
            if item['name'] == nome_arquivo:
                # Se o arquivo já existe, deletá-lo
                url_delete_file = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{item['id']}"
                response_delete_file = requests.delete(
                    url_delete_file, headers=headers)
                if response_delete_file.status_code == 204:
                    print(f"Arquivo '{nome_arquivo}' existente foi excluído.")
                else:
                    registrar_print("Erro ao excluir o arquivo existente.")

    # Criar o arquivo TXT diretamente na pasta pai com o mesmo nome da data e hora atual
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Cria o conteúdo do arquivo de texto com os registros da lista de prints
        conteudo_arquivo = '\n'.join(lista_prints) + '\n'

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")

# função para exportar qualquer dataframe em qualquer formato


def exportar_df(user_id, pasta_id, arquivo, extensao_arquivo, nome):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    # data_hora = data_hora_atual()
    nome_arquivo = f"{nome}.{extensao_arquivo}"

    # Criar o arquivo diretamente na pasta pai com o mesmo nome do DataFrame concatenado com a extensão desejada
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
    data = {
        "name": nome_arquivo,
        "@microsoft.graph.conflictBehavior": "rename",
        "file": {}
    }
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        arquivo_id = response.json().get('id')
        url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

        # Convertendo o DataFrame em uma string para enviar ao OneDrive
        conteudo_arquivo = str(arquivo)

        response_upload = requests.put(
            url_upload, headers=headers, data=conteudo_arquivo.encode('utf-8'))

        if response_upload.status_code == 200:
            print(f"Arquivo '{nome_arquivo}' criado com sucesso no OneDrive.")

# função para baixar qualquer arquivo online pela URL DIRETA


def baixar_arquivo_online(user_id, pasta_id, url_arquivo, nome_arquivo, extensao_arquivo):
    drive_id = obter_drive_id(user_id)
    if not drive_id:
        registrar_print("Erro ao obter ID do drive.")
        return

    token_info = token()
    if not token_info:
        registrar_print("Erro ao obter token de acesso.")
        return

    access_token = token_info['access_token']
    headers = {'Authorization': 'Bearer {}'.format(
        access_token), 'Content-Type': 'application/json'}

    # Fazer o download do arquivo da URL
    response_download = requests.get(url_arquivo)

    if response_download.status_code == 200:
        # Criar o arquivo diretamente na pasta pai com o nome do arquivo baixado
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{pasta_id}/children"
        data = {
            "name": f'{nome_arquivo}.{extensao_arquivo}',
            "@microsoft.graph.conflictBehavior": "rename",
            "file": {}
        }
        response_create = requests.post(url, headers=headers, json=data)

        if response_create.status_code == 201:
            arquivo_id = response_create.json().get('id')
            url_upload = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{arquivo_id}/content"

            conteudo_arquivo = response_download.content

            response_upload = requests.put(
                url_upload, headers=headers, data=conteudo_arquivo)

            if response_upload.status_code == 200:
                print(
                    f"Arquivo '{nome_arquivo}' baixado e salvo com sucesso no OneDrive.")
            else:
                registrar_print("Erro: Arquivo não foi baixado.")
        else:
            registrar_print("Erro: Arquivo não foi salvo na pasta log")
    else:
        registrar_print("Não foi encontrado a pasta log.")

# função para remover acentuação dos dataframes


def remover_acentuacao_titulos(df):
    df = df.rename(columns=lambda x: unidecode(x) if isinstance(x, str) else x)
    return df


def inserir_dados_cagedexc(cursor, table_name, data_frame):
    query = f"INSERT INTO {table_name} (competenciamov, regiao, uf, municipio, secao, subclasse, saldomovimentacao, \
            cbo2002ocupacao, categoria, graudeinstrucao, idade, horascontratuais, racacor, sexo, tipoempregador, \
            tipoestabelecimento, tipomovimentacao, tipodedeficiencia, indtrabintermitente, indtrabparcial, salario, \
            tamestabjan, indicadoraprendiz, origemdainformacao, competenciadec, competenciaexc, indicadordeexclusao, \
            indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo, mesano_referencia, data_hora_carga) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    data = [
        (
            row['competenciamov'], row['regiao'], row['uf'], row['municipio'], row['secao'], row['subclasse'],
            row['saldomovimentacao'], row['cbo2002ocupacao'], row['categoria'], row['graudeinstrucao'],
            row['idade'], str(
                row['horascontratuais']), row['racacor'], row['sexo'], row['tipoempregador'],
            row['tipoestabelecimento'], row['tipomovimentacao'], row['tipodedeficiencia'], row['indtrabintermitente'],
            row['indtrabparcial'], str(
                row['salario']), row['tamestabjan'], row['indicadoraprendiz'],
            row['origemdainformacao'], row['competenciadec'], row['competenciaexc'], row['indicadordeexclusao'],
            row['indicadordeforadoprazo'], row['unidadesalariocodigo'], str(
                row['valorsalariofixo']),
            row['mesano_referencia'], row['data_hora_carga']
        )
        for _, row in data_frame.iterrows()
    ]
    print(
        f'Iniciou a inserção da {table_name} em:', data_hora_atual())
    cursor.executemany(query, data)
    print(
        f'Dados na tabela {table_name} injetados com sucesso em:', data_hora_atual())


def inserir_dados_cagedfor(cursor, table_name, data_frame):
    query = f"INSERT INTO {table_name} (competenciamov, regiao, uf, municipio, secao, subclasse, saldomovimentacao, \
            cbo2002ocupacao, categoria, graudeinstrucao, idade, horascontratuais, racacor, sexo, tipoempregador, \
            tipoestabelecimento, tipomovimentacao, tipodedeficiencia, indtrabintermitente, indtrabparcial, salario, \
            tamestabjan, indicadoraprendiz, origemdainformacao, competenciadec, \
            indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo, mesano_referencia, data_hora_carga) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    data = [
        (
            row['competenciamov'], row['regiao'], row['uf'], row['municipio'], row['secao'], row['subclasse'],
            row['saldomovimentacao'], row['cbo2002ocupacao'], row['categoria'], row['graudeinstrucao'],
            row['idade'], str(
                row['horascontratuais']), row['racacor'], row['sexo'], row['tipoempregador'],
            row['tipoestabelecimento'], row['tipomovimentacao'], row['tipodedeficiencia'], row['indtrabintermitente'],
            row['indtrabparcial'], str(
                row['salario']), row['tamestabjan'], row['indicadoraprendiz'],
            row['origemdainformacao'], row['competenciadec'], row['indicadordeforadoprazo'],
            row['unidadesalariocodigo'], str(
                row['valorsalariofixo']), row['mesano_referencia'], row['data_hora_carga']
        )
        for _, row in data_frame.iterrows()
    ]
    print(
        f'Iniciou a inserção da {table_name} em:', data_hora_atual())
    cursor.executemany(query, data)
    print(
        f'Dados na tabela {table_name} injetados com sucesso em:', data_hora_atual())


def inserir_dados_cagedmov(cursor, table_name, data_frame):
    print(f'Iniciou a inserção da {table_name} em:', data_hora_atual())

    # Defina o tamanho máximo do lote que você deseja inserir de uma vez
    tamanho_do_lote = 10000

    # Agrupe os dados em lotes e insira usando executemany
    for batch_start in range(0, len(data_frame), tamanho_do_lote):
        batch = data_frame.iloc[batch_start:batch_start + tamanho_do_lote]

        insert_query = f"INSERT INTO {table_name} \
        (competenciamov, regiao, uf, municipio, secao, subclasse,\
        saldomovimentacao, cbo2002ocupacao, categoria, graudeinstrucao,\
        idade, horascontratuais, racacor, sexo, tipoempregador,\
        tipoestabelecimento, tipomovimentacao, tipodedeficiencia,\
        indtrabintermitente, indtrabparcial, salario, tamestabjan,\
        indicadoraprendiz, origemdainformacao, competenciadec,\
        indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo,\
        mesano_referencia, data_hora_carga) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        data = [(row['competenciamov'], row['regiao'], row['uf'], row['municipio'], row['secao'], row['subclasse'],
                           row['saldomovimentacao'], row['cbo2002ocupacao'], row['categoria'], row['graudeinstrucao'],
                           row['idade'], str(
                               row['horascontratuais']), row['racacor'], row['sexo'], row['tipoempregador'],
                           row['tipoestabelecimento'], row['tipomovimentacao'], row['tipodedeficiencia'], row['indtrabintermitente'],
                           row['indtrabparcial'], str(
                               row['salario']), row['tamestabjan'], row['indicadoraprendiz'], row['origemdainformacao'],
                           row['competenciadec'], row['indicadordeforadoprazo'], row['unidadesalariocodigo'], str(
                               row['valorsalariofixo']),
                           row['mesano_referencia'], row['data_hora_carga']) for _, row in batch.iterrows()]

        cursor.executemany(insert_query, data)

    print(
        f'Dados na tabela {table_name} injetados com sucesso em:', data_hora_atual())

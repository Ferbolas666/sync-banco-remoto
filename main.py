import time
import fdb
import os
import json
import requests
import traceback
from requests.exceptions import RequestException
from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/replicar")
async def replicar_dados(request: Request):
    dados = await request.json()
    print("📥 Dados recebidos:", dados)
    return {"status": "ok"}  # <- Altere isso para {"status": "success"} no local que realmente confirma a replicação


def ler_connection_txt(caminho_txt):
    config = {}
    with open(caminho_txt, "r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            if "=" in linha:
                chave, valor = linha.strip().split("=", 1)
                config[chave.strip().upper()] = valor.strip()
    return config

def parse_dados(dados_str):
    if not dados_str:
        return {}
    if isinstance(dados_str, dict):
        return dados_str
    if isinstance(dados_str, str) and ':' in dados_str:
        dados_dict = {}
        partes = [p.strip() for p in dados_str.split(',') if ':' in p]
        for parte in partes:
            try:
                chave, valor = [x.strip() for x in parte.split(':', 1)]
                valor = None if valor == '' else valor
                dados_dict[chave] = valor
            except ValueError:
                continue
        return dados_dict
    try:
        return json.loads(dados_str)
    except json.JSONDecodeError:
        return {"raw": dados_str}

def converter_valores(dados_dict):
    dados_ajustados = {}
    for key, value in dados_dict.items():
        if value is None:
            dados_ajustados[key] = None
        elif isinstance(value, str):
            if value.isdigit():
                dados_ajustados[key] = int(value)
            elif value.replace('.', '', 1).isdigit() and value.count('.') == 1:
                dados_ajustados[key] = float(value)
            elif value.lower() in ['true', 'false']:
                dados_ajustados[key] = value.lower() == 'true'
            else:
                dados_ajustados[key] = value
        else:
            dados_ajustados[key] = value
    return dados_ajustados

def monitorar_logs_remoto(host, port, database, user, password, api_url, intervalo_segundos=1):
    print("✅ Monitorando LOG_ALTERACOES no banco remoto e enviando dados para API...")
    print(f"Configurações: Host={host}, Port={port}, Database={database}, User={user}, API={api_url}")
    
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    last_id_log = 0

    while True:
        try:
            print("🔁 Tentando conectar ao banco remoto...")
            conn = fdb.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            cur = conn.cursor()
            print("✅ Conectado ao banco remoto com sucesso!")

            cur.execute("""
                SELECT ID, TABELA, OPERACAO, DADOS, DATA_ALTERACAO, ID_REGISTRO
                FROM LOG_ALTERACOES
                WHERE ID > ?
                ORDER BY ID
            """, (last_id_log,))

            rows = cur.fetchall()
            print(f"🔍 Encontrados {len(rows)} registros novos")

            if not rows:
                print("⏳ Nenhum novo registro encontrado, aguardando...")

            for id_log, tabela, operacao, dados, data_alteracao, id_registro in rows:
                log_msg = f"[{data_alteracao}] {operacao} em {tabela}, ID_LOG={id_log}, ID_REGISTRO={id_registro}"
                print(log_msg)

                dados_dict = parse_dados(dados)
                dados_ajustados = converter_valores(dados_dict)
                print(f"📊 Dados processados: {dados_ajustados}")

                payload = {
                    "id_log": id_log,
                    "tabela": tabela,
                    "operacao": operacao,
                    "dados": dados_ajustados,
                    "id_registro": id_registro,
                    "data_alteracao": str(data_alteracao)
                }

                try:
                    print(f"🚀 Enviando dados para API: {api_url}")
                    response = requests.post(
                        api_url,
                        json=payload,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        resposta_json = response.json()
                        if resposta_json.get("status") == "success":
                            print(f"✅ Dados replicados com sucesso! Removendo LOG_ALTERACOES ID = {id_log}")

                            cur.execute("""
                                DELETE FROM LOG_ALTERACOES
                                WHERE ID = ?
                            """, (id_log,))
                            conn.commit()
                            print(f"🧹 Log ID {id_log} removido com sucesso.")
                            last_id_log = id_log
                        else:
                            print(f"⚠️ API respondeu mas não confirmou sucesso: {resposta_json}")
                    else:
                        print(f"⚠️ Resposta inesperada da API: {response.status_code} - {response.text}")
                        response.raise_for_status()

                except RequestException as e:
                    print(f"❌ Erro ao enviar dados para a API: {str(e)}")
                    if hasattr(e, 'response') and e.response:
                        print(f"Detalhes do erro: {e.response.text}")

            cur.close()
            conn.close()
            print("🔒 Conexão com banco remoto fechada")

        except fdb.Error as e:
            print(f"🔥 Erro no Firebird: {e}")
            print(traceback.format_exc())
            time.sleep(5)
        except KeyboardInterrupt:
            print("⏹ Monitoramento interrompido manualmente.")
            break
        except Exception as e:
            print(f"💥 Erro inesperado: {e}")
            print(traceback.format_exc())
            time.sleep(5)

        print(f"⏱️ Aguardando {intervalo_segundos} segundos...")
        time.sleep(intervalo_segundos)

if __name__ == "__main__":
    caminho_txt = r"C:\CONEXAO\Connection_remote.txt"

    if not os.path.exists(caminho_txt):
        print(f"❌ Arquivo de configuração não encontrado: {caminho_txt}")
        exit(1)

    config = ler_connection_txt(caminho_txt)

    required_keys = ['HOST', 'PORT', 'DATABASE', 'USER', 'PASSWORD']
    missing_keys = [key for key in required_keys if key not in config]

    if missing_keys:
        print(f"❌ Chaves ausentes no arquivo de configuração: {', '.join(missing_keys)}")
        exit(1)

    api_url = "http://localhost:8000/replicar"  # Fora do Docker
    # api_url = "http://api-replicador:8000/replicar"  # Dentro da rede Docker

    print("⚙️ Configurações carregadas:")
    print(f" - Host: {config['HOST']}")
    print(f" - Port: {config['PORT']}")
    print(f" - Database: {config['DATABASE']}")
    print(f" - User: {config['USER']}")
    print(f" - API_URL: {api_url}")

    monitorar_logs_remoto(
        host=config['HOST'],
        port=int(config['PORT']),
        database=config['DATABASE'],
        user=config['USER'],
        password=config['PASSWORD'],
        api_url=api_url,
        intervalo_segundos=1
    )

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import fdb
import socket
from typing import Union
import logging
import json

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Mapeamento das chaves primárias por tabela
chave_primaria_por_tabela = {
    'VENDAS': 'COD_VENDA',
    'CLIENTES': 'COD_CLIENTE',
    'PRODUTOS': 'COD_PRODUTO',
}

class LogAlteracao(BaseModel):
    id_log: int
    tabela: str
    operacao: str
    dados: dict
    id_registro: Union[int, str]
    data_alteracao: str

def testar_conexao_firebird(host, port, timeout=10):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.error(f"Falha na conexão com {host}:{port} - {str(e)}")
        return False

@app.websocket("/ws/replicar")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            msg = await websocket.receive_text()
            try:
                log_dict = json.loads(msg)
                log = LogAlteracao(**log_dict)
            except Exception as e:
                await websocket.send_json({"status": "error", "detail": f"JSON inválido: {str(e)}"})
                continue

            host = "db-junior-repl-3.sp1.br.saveincloud.net.br"
            port = 16475
            database = "/opt/firebird/data/dados-junior-remoto.fdb"
            user_remoto = "SYSDBA"
            pass_remoto = "zyAhhI2tUSdIaG9d0Pa0"

            if not testar_conexao_firebird(host, port):
                await websocket.send_json({"status": "error", "detail": f"Não foi possível conectar ao servidor Firebird em {host}:{port}"})
                continue

            tabela = log.tabela.upper()
            if tabela not in chave_primaria_por_tabela:
                await websocket.send_json({"status": "error", "detail": f"Tabela inválida: {tabela}"})
                continue

            campo_id = chave_primaria_por_tabela[tabela]

            try:
                conn = fdb.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user_remoto,
                    password=pass_remoto,
                )
                cur = conn.cursor()

                dados_dict = log.dados
                operacao = log.operacao.upper()

                if operacao == 'INSERT':
                    campos = ', '.join(dados_dict.keys())
                    placeholders = ', '.join(['?'] * len(dados_dict))
                    sql = f"INSERT INTO {tabela} ({campos}) VALUES ({placeholders})"
                    cur.execute(sql, tuple(dados_dict.values()))

                elif operacao == 'UPDATE':
                    if not log.id_registro:
                        await websocket.send_json({"status": "error", "detail": "Campo 'id_registro' obrigatório para UPDATE"})
                        continue
                    set_clause = ', '.join([f"{k} = ?" for k in dados_dict])
                    sql = f"UPDATE {tabela} SET {set_clause} WHERE {campo_id} = ?"
                    valores = list(dados_dict.values()) + [log.id_registro]
                    cur.execute(sql, tuple(valores))

                elif operacao == 'DELETE':
                    if not log.id_registro:
                        await websocket.send_json({"status": "error", "detail": "Campo 'id_registro' obrigatório para DELETE"})
                        continue
                    sql = f"DELETE FROM {tabela} WHERE {campo_id} = ?"
                    cur.execute(sql, (log.id_registro,))

                else:
                    await websocket.send_json({"status": "error", "detail": "Operação não suportada"})
                    continue

                conn.commit()

                # Excluir o log correspondente da tabela LOG_ALTERACOES
                try:
                    sql_delete_log = "DELETE FROM LOG_ALTERACOES WHERE ID_LOG = ?"
                    cur.execute(sql_delete_log, (log.id_log,))
                    conn.commit()
                except Exception as e:
                    await websocket.send_json({"status": "warning", "detail": f"Operação replicada, mas falha ao excluir da LOG_ALTERACOES: {str(e)}"})

                await websocket.send_json({"status": "success", "msg": f"{operacao} na tabela {tabela}"})

            except fdb.fbcore.DatabaseError as e:
                await websocket.send_json({"status": "error", "detail": f"Erro de banco de dados: {str(e)}"})

            except Exception as e:
                await websocket.send_json({"status": "error", "detail": f"Erro inesperado: {str(e)}"})

            finally:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals():
                    conn.close()

    except WebSocketDisconnect:
        logger.info("Cliente desconectado do WebSocket")

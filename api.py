from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import fdb
import socket
from typing import Union
import logging
import json

# Configurar logging detalhado
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("replicador")

app = FastAPI()

class LogAlteracao(BaseModel):
    tabela: str
    operacao: str
    dados: dict
    id_registro: Union[int, str]
    data_alteracao: str

def testar_conexao_firebird(host, port, timeout=10):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            logger.info(f"Conexão com {host}:{port} OK")
            return True
    except Exception as e:
        logger.error(f"Erro ao testar conexão com {host}:{port} - {str(e)}")
        return False

@app.websocket("/ws/replicar")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket aceito, aguardando mensagens...")

    try:
        while True:
            msg = await websocket.receive_text()
            logger.info(f"Mensagem recebida: {msg}")

            try:
                log_dict = json.loads(msg)
                log = LogAlteracao(**log_dict)
                logger.info(f"JSON parseado com sucesso: {log}")
            except Exception as e:
                logger.error(f"Erro ao parsear JSON: {str(e)}")
                await websocket.send_json({"status": "error", "detail": f"JSON inválido: {str(e)}"})
                continue

            host = "db-junior-repl-3.sp1.br.saveincloud.net.br"
            port = 16475
            database = "/opt/firebird/data/dados-junior-remoto.fdb"
            user_remoto = "SYSDBA"
            pass_remoto = "zyAhhI2tUSdIaG9d0Pa0"

            if not testar_conexao_firebird(host, port):
                await websocket.send_json({"status": "error", "detail": "Falha na conexão com o banco remoto"})
                continue

            tabela = log.tabela.upper()

            try:
                conn = fdb.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user_remoto,
                    password=pass_remoto,
                )
                cur = conn.cursor()
                logger.info("Conectado ao banco de dados remoto com sucesso.")

                dados_dict = log.dados
                operacao = log.operacao.upper()
                logger.info(f"Iniciando operação: {operacao} na tabela {tabela}")

                if operacao == 'INSERT':
                    campos = ', '.join(dados_dict.keys())
                    placeholders = ', '.join(['?'] * len(dados_dict))
                    sql = f"INSERT INTO {tabela} ({campos}) VALUES ({placeholders})"
                    logger.info(f"SQL INSERT: {sql}")
                    logger.info(f"Valores: {tuple(dados_dict.values())}")
                    cur.execute(sql, tuple(dados_dict.values()))

                elif operacao == 'UPDATE':
                    if not log.id_registro:
                        logger.warning("Campo id_registro ausente para UPDATE.")
                        await websocket.send_json({"status": "error", "detail": "Campo 'id_registro' obrigatório para UPDATE"})
                        continue
                    set_clause = ', '.join([f"{k} = ?" for k in dados_dict])
                    sql = f"UPDATE {tabela} SET {set_clause} WHERE CAST(ID_REGISTRO AS VARCHAR(50)) = ?"
                    valores = list(dados_dict.values()) + [str(log.id_registro)]
                    logger.info(f"SQL UPDATE: {sql}")
                    logger.info(f"Valores: {valores}")
                    cur.execute(sql, tuple(valores))

                elif operacao == 'DELETE':
                    if not log.id_registro:
                        logger.warning("Campo id_registro ausente para DELETE.")
                        await websocket.send_json({"status": "error", "detail": "Campo 'id_registro' obrigatório para DELETE"})
                        continue
                    sql = f"DELETE FROM {tabela} WHERE CAST(ID_REGISTRO AS VARCHAR(50)) = ?"
                    logger.info(f"SQL DELETE: {sql}")
                    logger.info(f"Valor: {log.id_registro}")
                    cur.execute(sql, (str(log.id_registro),))

                else:
                    logger.warning("Operação não suportada.")
                    await websocket.send_json({"status": "error", "detail": "Operação não suportada"})
                    continue

                conn.commit()
                logger.info("Commit da operação principal realizado com sucesso.")

                # Deletar log com base apenas em TABELA, OPERACAO, ID_REGISTRO
                try:
                    logger.info(f"Deletando LOG_ALTERACOES com TABELA = {log.tabela}, OPERACAO = {log.operacao}, ID_REGISTRO = {log.id_registro}")
                    sql_delete_log = """
                        DELETE FROM LOG_ALTERACOES
                        WHERE UPPER(TABELA) = ?
                          AND UPPER(OPERACAO) = ?
                          AND CAST(ID_REGISTRO AS VARCHAR(50)) = ?
                    """
                    cur.execute(sql_delete_log, (
                        log.tabela.upper(),
                        log.operacao.upper(),
                        str(log.id_registro),
                    ))
                    logger.info("DELETE em LOG_ALTERACOES executado.")
                    conn.commit()
                    logger.info("Commit após DELETE em LOG_ALTERACOES realizado.")
                except Exception as e:
                    logger.error(f"Erro ao apagar de LOG_ALTERACOES: {str(e)}")
                    await websocket.send_json({
                        "status": "warning",
                        "detail": f"Operação replicada, mas erro ao apagar de LOG_ALTERACOES: {str(e)}"
                    })

                await websocket.send_json({"status": "success", "msg": f"{operacao} na tabela {tabela}"})

            except fdb.fbcore.DatabaseError as e:
                logger.error(f"Erro de banco de dados: {str(e)}")
                await websocket.send_json({"status": "error", "detail": f"Erro de banco de dados: {str(e)}"})

            except Exception as e:
                logger.error(f"Erro inesperado: {str(e)}")
                await websocket.send_json({"status": "error", "detail": f"Erro inesperado: {str(e)}"})

            finally:
                if 'cur' in locals():
                    cur.close()
                    logger.info("Cursor fechado.")
                if 'conn' in locals():
                    conn.close()
                    logger.info("Conexão com banco encerrada.")

    except WebSocketDisconnect:
        logger.warning("Cliente desconectado do WebSocket")

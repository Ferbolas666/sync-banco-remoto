from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import fdb
import socket
import time
from typing import Union
import logging

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
    """Testa conectividade básica com o servidor Firebird"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.error(f"Falha na conexão com {host}:{port} - {str(e)}")
        return False

@app.post("/replicar")
def replicar_log(log: LogAlteracao):
    host = "db-junior-repl-3.sp1.br.saveincloud.net.br"
    port = 16475
    database = "/opt/firebird/data/dados-junior-remoto.fdb"
    user_remoto = "SYSDBA"
    pass_remoto = "zyAhhI2tUSdIaG9d0Pa0"

    # Teste básico de conectividade
    if not testar_conexao_firebird(host, port):
        error_msg = f"Não foi possível conectar ao servidor Firebird em {host}:{port}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

    tabela = log.tabela.upper()
    if tabela not in chave_primaria_por_tabela:
        raise HTTPException(status_code=400, detail=f"Tabela inválida: {tabela}")

    campo_id = chave_primaria_por_tabela[tabela]

    try:
        logger.info(f"Tentando conectar ao Firebird: {host}:{port}{database}")
        
        # Conecta ao banco remoto com parâmetros separados
        conn = fdb.connect(
            host=host,
            port=port,
            database=database,
            user=user_remoto,
            password=pass_remoto,
        )
            
        cur = conn.cursor()
        logger.info(f"Conexão estabelecida com sucesso! Versão: {conn.firebird_version}")

        dados_dict = log.dados
        logger.info(f"Operação: {log.operacao} | Tabela: {tabela} | ID_REGISTRO: {log.id_registro}")

        # INSERT
        if log.operacao.upper() == 'INSERT':
            campos = ', '.join(dados_dict.keys())
            placeholders = ', '.join(['?'] * len(dados_dict))
            sql = f"INSERT INTO {tabela} ({campos}) VALUES ({placeholders})"
            
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Valores: {tuple(dados_dict.values())}")
            
            cur.execute(sql, tuple(dados_dict.values()))
            conn.commit()
            return {"status": "success", "msg": f"Insert na tabela {tabela}"}

        # UPDATE
        elif log.operacao.upper() == 'UPDATE':
            if not log.id_registro:
                raise HTTPException(status_code=400, detail="Campo 'id_registro' é obrigatório para UPDATE")

            set_clause = ', '.join([f"{k} = ?" for k in dados_dict])
            sql = f"UPDATE {tabela} SET {set_clause} WHERE {campo_id} = ?"
            valores = list(dados_dict.values()) + [log.id_registro]
            
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Valores: {tuple(valores)}")
            
            cur.execute(sql, tuple(valores))
            conn.commit()
            return {"status": "success", "msg": f"Update na tabela {tabela}"}

        # DELETE
        elif log.operacao.upper() == 'DELETE':
            if not log.id_registro:
                raise HTTPException(status_code=400, detail="Campo 'id_registro' é obrigatório para DELETE")

            sql = f"DELETE FROM {tabela} WHERE {campo_id} = ?"
            
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Valor: {log.id_registro}")
            
            cur.execute(sql, (log.id_registro,))
            conn.commit()
            return {"status": "success", "msg": f"Delete na tabela {tabela}"}

        else:
            raise HTTPException(status_code=400, detail="Operação não suportada")

    except fdb.fbcore.DatabaseError as e:
        error_msg = f"Erro de banco de dados: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    
    except Exception as e:
        error_msg = f"Erro inesperado: {str(e)}"
        logger.exception(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import fdb
import os
from dotenv import load_dotenv
from typing import Union

load_dotenv()

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
    id_registro: Union[int, str]  # aceita int ou str
    data_alteracao: str

@app.post("/replicar")
def replicar_log(log: LogAlteracao):
    # Carrega as variáveis de conexão
    dsn_remoto = os.getenv("FIREBIRD_DSN")
    user_remoto = os.getenv("FIREBIRD_USER")
    pass_remoto = os.getenv("FIREBIRD_PASS")

    if not all([dsn_remoto, user_remoto, pass_remoto]):
        raise HTTPException(status_code=500, detail="Dados de conexão ausentes ou inválidos")

    tabela = log.tabela.upper()
    if tabela not in chave_primaria_por_tabela:
        raise HTTPException(status_code=400, detail=f"Tabela inválida: {tabela}")

    campo_id = chave_primaria_por_tabela[tabela]

    try:
        # Conecta ao banco remoto
        conn = fdb.connect(dsn=dsn_remoto, user=user_remoto, password=pass_remoto)
        cur = conn.cursor()

        dados_dict = log.dados
        print(f"[LOG] Operação: {log.operacao} | Tabela: {tabela} | Dados: {dados_dict} | ID_REGISTRO: {log.id_registro}")

        # INSERT
        if log.operacao.upper() == 'INSERT':
            campos = ', '.join(dados_dict.keys())
            placeholders = ', '.join(['?'] * len(dados_dict))
            sql = f"INSERT INTO {tabela} ({campos}) VALUES ({placeholders})"
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
            cur.execute(sql, tuple(valores))
            conn.commit()
            return {"status": "success", "msg": f"Update na tabela {tabela}"}

        # DELETE
        elif log.operacao.upper() == 'DELETE':
            if not log.id_registro:
                raise HTTPException(status_code=400, detail="Campo 'id_registro' é obrigatório para DELETE")

            sql = f"DELETE FROM {tabela} WHERE {campo_id} = ?"
            cur.execute(sql, (log.id_registro,))
            conn.commit()
            return {"status": "success", "msg": f"Delete na tabela {tabela}"}

        else:
            raise HTTPException(status_code=400, detail="Operação não suportada")

    except Exception as e:
        print(f"[ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

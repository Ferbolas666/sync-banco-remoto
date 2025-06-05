from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import fdb
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

class LogAlteracao(BaseModel):
    id_log: int
    tabela: str
    operacao: str
    dados: str
    data_alteracao: str

def parse_dados(dados_str):
    pares = dados_str.split(',')
    resultado = {}
    for par in pares:
        if ':' in par:
            chave, valor = par.split(':', 1)
            chave = chave.strip()
            valor = valor.strip()
            resultado[chave] = None if valor == "" else valor
    return resultado

@app.post("/replicar")
def replicar_log(log: LogAlteracao):
    dsn_remoto = os.getenv("FIREBIRD_DSN")
    user_remoto = os.getenv("FIREBIRD_USER")
    pass_remoto = os.getenv("FIREBIRD_PASS")

    if not all([dsn_remoto, user_remoto, pass_remoto]):
        raise HTTPException(status_code=500, detail="Dados de conexão ausentes ou inválidos")

    tabelas_validas = ['VENDAS', 'CLIENTES', 'PRODUTOS']
    tabela = log.tabela.upper()

    if tabela not in tabelas_validas:
        raise HTTPException(status_code=400, detail="Tabela inválida")

    try:
        conn = fdb.connect(dsn=dsn_remoto, user=user_remoto, password=pass_remoto)
        cur = conn.cursor()

        dados_dict = parse_dados(log.dados)
        print(f"[LOG] Operação: {log.operacao} | Tabela: {tabela} | Dados: {dados_dict}")

        if log.operacao.upper() == 'INSERT':
            campos = ', '.join(dados_dict.keys())
            placeholders = ', '.join(['?'] * len(dados_dict))
            sql = f"INSERT INTO {tabela} ({campos}) VALUES ({placeholders})"
            cur.execute(sql, tuple(dados_dict.values()))
            conn.commit()
            return {"status": "success", "msg": f"Insert na tabela {tabela}"}

        elif log.operacao.upper() == 'UPDATE':
            if 'ID' not in dados_dict:
                raise HTTPException(status_code=400, detail="Campo 'ID' obrigatório para UPDATE")
            set_clause = ', '.join([f"{k} = ?" for k in dados_dict if k.upper() != 'ID'])
            sql = f"UPDATE {tabela} SET {set_clause} WHERE ID = ?"
            valores = [v for k, v in dados_dict.items() if k.upper() != 'ID']
            valores.append(dados_dict['ID'])
            cur.execute(sql, tuple(valores))
            conn.commit()
            return {"status": "success", "msg": f"Update na tabela {tabela}"}

        elif log.operacao.upper() == 'DELETE':
            if 'ID' not in dados_dict:
                raise HTTPException(status_code=400, detail="Campo 'ID' obrigatório para DELETE")
            sql = f"DELETE FROM {tabela} WHERE ID = ?"
            cur.execute(sql, (dados_dict['ID'],))
            conn.commit()
            return {"status": "success", "msg": f"Delete na tabela {tabela}"}

        else:
            raise HTTPException(status_code=400, detail="Operação não suportada")

    except Exception as e:
        print(f"[ERRO] {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

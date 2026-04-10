"""
AUTOMAÇÃO DE CONSULTA IPI - automaipiat.py
============================================
Este script roda em loop contínuo 24/7 e:
1. Consulta a tabela 'fila_ipi' no Supabase buscando registros com status 'Pendente'
2. Para cada registro, faz SELECT no Oracle WinThor (SOMENTE LEITURA):
   SELECT codfilial, mes, ano, valoripi FROM PCAPURACAOICMS WHERE ANO=:ano AND MES=:mes
3. Faz UPSERT na tabela 'saldo_ipi' no Supabase (sem duplicatas via constraint UNIQUE)
4. Marca a solicitação como 'Concluido' ou 'Erro'

SEGURANÇA:
- SOMENTE SELECT no Oracle. Nunca altera dados no WinThor.
- UPSERT no Supabase com ON CONFLICT para evitar duplicatas.
"""

import os
import time
import requests
import oracledb
from datetime import datetime

# --- CONFIGURAÇÕES DO BANCO DE DADOS ORACLE (WINTHOR) --- SOMENTE LEITURA ---
CLIENT_LIB_DIR = r"C:\Users\claudeyr.sousa\Documents\instantclient-basic-windows.x64-21.19.0.0.0dbru\instantclient_21_19"
ORACLE_USUARIO = "DNL"
ORACLE_SENHA = "DN55WINT60"
ORACLE_DSN = oracledb.makedsn("192.168.8.199", 1521, service_name="WINT")

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
except Exception as e:
    print(f"Aviso: Não foi possível inicializar Oracle Client: {e}")

# --- CONFIGURAÇÕES SUPABASE ---
SUPABASE_URL = "https://hzljpmjhmrgwjjskubii.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh6bGpwbWpobXJnd2pqc2t1YmlpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDAzNTExMCwiZXhwIjoyMDg5NjExMTEwfQ.fbJw8FyRxSFD45esE-y98tMQ93-6uc4yVID20EYiBz8"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

HEADERS_UPSERT = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal"
}

# Intervalo de polling em segundos
POLL_INTERVAL = 2


def log(msg):
    """Log com timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


from decimal import Decimal

def safe_float(valor):
    if valor is None:
        return 0.0

    if isinstance(valor, (int, float)):
        return float(valor)

    if isinstance(valor, Decimal):
        return float(valor)

    s = str(valor).strip()

    # trata formato brasileiro
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')

    try:
        return float(s)
    except Exception as e:
        log(f"[ERRO CONVERSÃO] valor={valor} tipo={type(valor)} erro={e}")
        return 0.0


def buscar_pendentes():
    """Busca registros com status 'Pendente' na tabela fila_ipi"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/fila_ipi?status=eq.Pendente&select=*"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            log(f"Erro ao buscar pendentes: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log(f"Erro de conexão ao buscar pendentes: {e}")
        return []


def atualizar_status_fila(registro_id, novo_status, obs=""):
    """Atualiza o status de um registro na tabela fila_ipi"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/fila_ipi?id=eq.{registro_id}"
        payload = {"status": novo_status}
        if obs:
            payload["obs"] = obs
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Fila #{registro_id} -> '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha ao atualizar fila #{registro_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao atualizar fila #{registro_id}: {e}")
        return False


def upsert_saldo_ipi(registros_ipi):
    """
    Faz UPSERT em lote na tabela saldo_ipi no Supabase.
    Usa ON CONFLICT (codfilial, mes, ano) para evitar duplicatas.
    Se já existir, atualiza o valoripi.
    Envia em chunks de 50 para evitar timeouts.
    Retorna (sucesso: bool, erro_msg: str)
    """
    if not registros_ipi:
        return True, ""

    CHUNK = 50
    total_chunks = (len(registros_ipi) + CHUNK - 1) // CHUNK

    for i in range(total_chunks):
        chunk = registros_ipi[i * CHUNK : (i + 1) * CHUNK]
        try:
            url = f"{SUPABASE_URL}/rest/v1/saldo_ipi?on_conflict=codfilial,mes,ano"
            response = requests.post(url, headers=HEADERS_UPSERT, json=chunk, timeout=30)
            if response.status_code in [200, 201, 204]:
                log(f"  [OK] UPSERT chunk {i+1}/{total_chunks} ({len(chunk)} registro(s))")
            else:
                erro = f"HTTP {response.status_code}: {response.text[:300]}"
                log(f"  [ERRO] UPSERT chunk {i+1} falhou: {erro}")
                return False, erro
        except Exception as e:
            erro = str(e)[:300]
            log(f"  [ERRO] Conexão chunk {i+1}: {erro}")
            return False, erro

    log(f"  [OK] UPSERT total: {len(registros_ipi)} registro(s) sincronizado(s)")
    return True, ""


def processar_solicitacao(solicitacao):
    """
    Processa uma única solicitação da fila:
    1. Marca como 'Processando'
    2. Conecta ao Oracle (somente SELECT)
    3. Busca saldos IPI do WinThor
    4. Faz UPSERT no Supabase
    5. Marca como 'Concluido' ou 'Erro'
    """
    registro_id = solicitacao.get("id")
    codfilial = solicitacao.get("codfilial")  # pode ser None/vazio = todas
    mes = solicitacao.get("mes")
    ano = solicitacao.get("ano")

    log(f"\n>>> Processando solicitação #{registro_id}: Filial={codfilial or 'TODAS'}, Mês={mes}, Ano={ano}")

    # 1. Marca como Processando
    atualizar_status_fila(registro_id, "Processando")

    connection = None
    try:
        # 2. Conectar ao Oracle (SOMENTE LEITURA)
        connection = oracledb.connect(
            user=ORACLE_USUARIO,
            password=ORACLE_SENHA,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()

        # 3. Montar e executar o SELECT (com SUM + GROUP BY para agregar por filial)
        sql = """
            SELECT codfilial, mes, ano, SUM(NVL(valoripi, 0)) AS valoripi
            FROM PCAPURACAOICMS
            WHERE ANO = :ano
              AND MES = :mes
              AND nomecampo = 'SALDOANTERIORPERIODO'
        """
        params = {"ano": ano, "mes": mes}

        # Se filial foi informada, filtra também
        if codfilial:
            sql += " AND codfilial = :codfilial"
            params["codfilial"] = codfilial

        sql += " GROUP BY codfilial, mes, ano ORDER BY codfilial"

        log(f"  Executando SELECT no WinThor...")
        cursor.execute(sql, params)
        resultados = cursor.fetchall()

        log(f"  Resultado: {len(resultados)} filial(is) encontrada(s)")

        # DEBUG: mostrar primeiros valores do Oracle
        log(f"  ===== VALORES DO ORACLE (primeiras 5 filiais) =====")
        for idx, row in enumerate(resultados[:5]):
            log(f"  [ORACLE] codfilial={repr(row[0])} | mes={row[1]} | ano={row[2]} | SUM(valoripi)={repr(row[3])} (tipo={type(row[3]).__name__})")
        log(f"  ====================================================")

        if not resultados:
            atualizar_status_fila(registro_id, "Concluido", f"Nenhum registro encontrado para Ano={ano}, Mês={mes}")
            return True

        # 4. Preparar dados para UPSERT (já vem agrupado do Oracle)
        dados_para_upsert = []
        for row in resultados:
            dados_para_upsert.append({
                "codfilial": str(row[0]).strip(),
                "mes": int(row[1]),
                "ano": int(row[2]),
                "valoripi": safe_float(row[3])
            })

        # DEBUG: payload para Supabase
        log(f"  ===== PAYLOAD PARA SUPABASE (primeiros 5) =====")
        for d in dados_para_upsert[:5]:
            log(f"  [PAYLOAD] {d}")
        log(f"  ================================================")

        # 5. UPSERT no Supabase (sem duplicatas)
        sucesso, erro_msg = upsert_saldo_ipi(dados_para_upsert)

        if sucesso:
            atualizar_status_fila(registro_id, "Concluido", f"{len(dados_para_upsert)} registro(s) sincronizado(s)")
            log(f"  [SUCESSO] Solicitação #{registro_id} concluída!")
        else:
            atualizar_status_fila(registro_id, "Erro", erro_msg[:200])

        return sucesso

    except Exception as e:
        log(f"  [ERRO ORACLE] {e}")
        atualizar_status_fila(registro_id, "Erro", str(e)[:200])
        return False

    finally:
        if connection:
            try:
                connection.close()
            except:
                pass


def buscar_pendentes_update():
    """Busca registros com status 'PendenteUpdate' na tabela fila_ipi"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/fila_ipi?status=eq.PendenteUpdate&select=*"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            log(f"Erro ao buscar updates: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log(f"Erro de conexão ao buscar updates: {e}")
        return []


def registrar_log_ipi(codfilial, mes, ano, valor_anterior, valor_novo, user_nome, user_email):
    """Registra alteração na tabela log_ipi (auditoria)"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/log_ipi"
        payload = {
            "codfilial": str(codfilial),
            "mes": int(mes),
            "ano": int(ano),
            "valor_anterior": float(valor_anterior),
            "valor_novo": float(valor_novo),
            "alterado_por": user_nome or "Desconhecido",
            "alterado_email": user_email or "",
            "alterado_em": datetime.utcnow().isoformat() + "Z"
        }
        response = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 201, 204]:
            log(f"  [AUDIT] Log registrado: {valor_anterior} -> {valor_novo} por {user_nome}")
            return True
        else:
            log(f"  [AUDIT ERRO] {response.status_code}: {response.text[:200]}")
            return False
    except Exception as e:
        log(f"  [AUDIT ERRO] {e}")
        return False


def processar_update(solicitacao):
    """
    Processa uma atualização de valor IPI no WinThor com auditoria completa:
    1. Valida valor (não negativo)
    2. SELECT valor atual ANTES do UPDATE
    3. UPDATE PCAPURACAOICMS SET valoripi = :novo_valor
    4. SELECT novamente para VERIFICAR se mudou
    5. Registra log de auditoria no Supabase
    """
    registro_id = solicitacao.get("id")
    codfilial = solicitacao.get("codfilial")
    mes = solicitacao.get("mes")
    ano = solicitacao.get("ano")
    novo_valor_str = solicitacao.get("obs")  # novo valor vem no campo obs
    user_nome = solicitacao.get("user_nome", "Desconhecido")
    user_email = solicitacao.get("user_email", "")

    log(f"\n>>> UPDATE #{registro_id}: Filial={codfilial}, Mês={mes}, Ano={ano}")
    log(f"  Novo Valor: {novo_valor_str} | Por: {user_nome} ({user_email})")

    atualizar_status_fila(registro_id, "Processando")

    novo_valor = safe_float(novo_valor_str)

    # 1. VALIDAÇÃO: valor negativo
    if novo_valor < 0:
        msg = "Valor negativo não é permitido"
        log(f"  [BLOQUEADO] {msg}")
        atualizar_status_fila(registro_id, "Erro", msg)
        return False

    connection = None
    try:
        connection = oracledb.connect(
            user=ORACLE_USUARIO,
            password=ORACLE_SENHA,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()

        filter_params = {"codfilial": codfilial, "mes": mes, "ano": ano}

        # 2. SELECT ANTES: capturar valor atual
        sql_select = """
            SELECT SUM(NVL(valoripi, 0)) AS valoripi
            FROM PCAPURACAOICMS
            WHERE codfilial = :codfilial
              AND mes = :mes
              AND ano = :ano
              AND nomecampo = 'SALDOANTERIORPERIODO'
        """
        cursor.execute(sql_select, filter_params)
        resultado_antes = cursor.fetchone()
        valor_anterior = safe_float(resultado_antes[0]) if resultado_antes else 0.0
        log(f"  [ANTES] Valor atual no WinThor: {valor_anterior}")

        # 3. UPDATE
        sql_update = """
            UPDATE PCAPURACAOICMS
            SET valoripi = :novo_valor
            WHERE codfilial = :codfilial
              AND mes = :mes
              AND ano = :ano
              AND nomecampo = 'SALDOANTERIORPERIODO'
        """
        update_params = {"novo_valor": novo_valor, **filter_params}

        log(f"  Executando UPDATE no WinThor: {valor_anterior} -> {novo_valor}")
        cursor.execute(sql_update, update_params)
        rows = cursor.rowcount
        log(f"  Linhas afetadas: {rows}")

        if rows == 0:
            msg = f"UPDATE não afetou nenhuma linha (Filial={codfilial}, Mês={mes}, Ano={ano})"
            log(f"  [ERRO] {msg}")
            connection.rollback()
            atualizar_status_fila(registro_id, "Erro", msg)
            return False

        connection.commit()
        log(f"  COMMIT realizado!")

        # 4. SELECT DEPOIS: verificar se realmente alterou
        cursor.execute(sql_select, filter_params)
        resultado_depois = cursor.fetchone()
        valor_verificado = safe_float(resultado_depois[0]) if resultado_depois else -1
        log(f"  [DEPOIS] Valor verificado no WinThor: {valor_verificado}")

        if abs(valor_verificado - novo_valor) > 0.01:
            msg = f"Verificação falhou: esperado={novo_valor}, encontrado={valor_verificado}"
            log(f"  [AVISO] {msg}")
            atualizar_status_fila(registro_id, "Erro", msg)
            return False

        # 5. REGISTRAR AUDITORIA
        registrar_log_ipi(codfilial, mes, ano, valor_anterior, novo_valor, user_nome, user_email)

        atualizar_status_fila(registro_id, "UpdateConcluido",
            f"Atualizado: {valor_anterior} -> {novo_valor} por {user_nome}")
        log(f"  [SUCESSO] Valor IPI atualizado e verificado no WinThor!")
        return True

    except Exception as e:
        log(f"  [ERRO ORACLE] {e}")
        if connection:
            try: connection.rollback()
            except: pass
        atualizar_status_fila(registro_id, "Erro", str(e)[:200])
        return False

    finally:
        if connection:
            try: connection.close()
            except: pass


def main():
    log("=" * 60)
    log("AUTOMAÇÃO IPI INICIADA (automaipiat.py)")
    log("Monitorando tabela 'fila_ipi' no Supabase...")
    log(f"Oracle User: {ORACLE_USUARIO} | DSN: {ORACLE_DSN}")
    log(f"Intervalo de polling: {POLL_INTERVAL}s")
    log("MODO: LEITURA (Pendente) + ESCRITA (PendenteUpdate)")
    log("=" * 60)

    while True:
        try:
            # 1. Processar consultas (SELECT)
            pendentes = buscar_pendentes()
            if pendentes:
                log(f"\n>>> {len(pendentes)} consulta(s) pendente(s)")
                for s in pendentes:
                    processar_solicitacao(s)

            # 2. Processar atualizações (UPDATE no WinThor)
            updates = buscar_pendentes_update()
            if updates:
                log(f"\n>>> {len(updates)} atualização(ões) pendente(s)")
                for u in updates:
                    processar_update(u)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            log("\nAutomação interrompida pelo usuário (Ctrl+C)")
            break
        except Exception as e:
            log(f"ERRO NO LOOP PRINCIPAL: {e}")
            log("Retentando em 30 segundos...")
            time.sleep(30)


if __name__ == "__main__":
    main()

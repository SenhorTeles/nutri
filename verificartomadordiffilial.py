"""
AUTOMAÇÃO DE VERIFICAÇÃO - TOMADOR DIF FILIAL
================================================
Este script roda em loop contínuo e:
1. Consulta a tabela 'verificando_tomador' no Supabase buscando registros com status 'Verificando'
2. Para cada registro, verifica no Oracle WinThor:
   a) Se o CNPJ Tomador XML começa com "36899" (é da empresa):
      - Significa que foi importado na filial errada
      - Verifica se a nota foi TRANSFERIDA para a filial correta
        (busca pelo numtransent/numnota em TODAS as filiais e verifica 
         se agora está na filial cujo CNPJ bate com o cnpj_tomador do XML)
      - Se encontrou na filial correta → OK
      - Se ainda está errado → Divergente
   b) Se o CNPJ Tomador XML NÃO começa com "36899" (não é da empresa):
      - Deveria ter sido excluído do WinThor
      - Verifica se o registro AINDA EXISTE no Oracle
      - Se não existe mais (cancelado/excluído) → remove da nossa base
      - Se ainda existe → mantém na base como divergente
3. Atualiza o Supabase com o resultado

SEGURANÇA:
- O SELECT usa bind variables para evitar SQL Injection
- Logs detalhados de cada passo
"""

import os
import time
import requests
import oracledb
from datetime import datetime

# --- CONFIGURAÇÕES DO BANCO DE DADOS ORACLE (WINTHOR) --- CREDENCIAL DE LEITURA ---
CLIENT_LIB_DIR = r"C:\Users\claudeyr.sousa\Documents\instantclient-basic-windows.x64-21.19.0.0.0dbru\instantclient_21_19"
ORACLE_USUARIO = "CONSULTA"
ORACLE_SENHA = "CONPHPCMV"
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

# Mapa CNPJ → Código Filial (todas as filiais da empresa Soollar, CNPJ raiz 36899766)
FILIAIS_MAP = {
    "36899766000105": "8",
    "36899766000369": "18",
    "36899766000288": "10",
    "36899766000440": "22",
    "36899766000520": "23",
    "36899766000601": "24",
    "36899766000792": "25",
    "36899766001179": "38",
    "36899766001411": "41",
    "36899766001250": "39",
    "36899766001330": "40",
    "36899766001500": "42",
    "36899766000873": "35",
    "36899766000954": "36",
    "36899766001098": "37",
    "36899766001683": "43",
    "36899766001764": "44",
    "36899766002221": "51",
    "36899766002060": "48",
    "36899766001845": "46",
    "36899766001926": "47",
    "36899766002140": "49",
}

# Inverso: Código Filial → CNPJ
CODFILIAL_TO_CNPJ = {v: k for k, v in FILIAIS_MAP.items()}

CNPJ_PREFIX = "36899"

# Intervalo de polling em segundos
POLL_INTERVAL = 2


def log(msg):
    """Log com timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def buscar_pendentes():
    """Busca registros com status 'Verificando' na tabela verificando_tomador"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificando_tomador?status=eq.Verificando&select=*"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            log(f"Erro ao buscar pendentes: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log(f"Erro de conexão ao buscar pendentes: {e}")
        return []


def atualizar_status_verificando_tomador(registro_id, novo_status, obs_extra=""):
    """Atualiza o status de um registro na tabela verificando_tomador"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificando_tomador?id=eq.{registro_id}"
        payload = {"status": novo_status}
        if obs_extra:
            payload["obs"] = obs_extra
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Status verificando_tomador #{registro_id} → '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha atualizar verificando_tomador #{registro_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão atualizar verificando_tomador #{registro_id}: {e}")
        return False


def atualizar_confronto_status(confronto_id, novo_status, obs=None, codfilial_novo=None, cnpj_filial_novo=None, user_nome=None, user_email=None):
    """Atualiza o status do confrontofiscal no Supabase"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscal?id=eq.{confronto_id}"
        payload = {"status": novo_status}
        
        if obs is not None:
            payload["obs"] = obs
        if codfilial_novo is not None:
            payload["codfilial"] = codfilial_novo
        if cnpj_filial_novo is not None:
            payload["cnpj_filial"] = cnpj_filial_novo
        
        if novo_status == "OK" and user_email:
            payload["validado_por"] = user_nome
            payload["validado_email"] = user_email
            payload["validado_em"] = datetime.utcnow().isoformat() + "Z"
        
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] confrontofiscal #{confronto_id} → '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha atualizar confrontofiscal #{confronto_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão atualizar confrontofiscal #{confronto_id}: {e}")
        return False


def deletar_confronto(confronto_id):
    """Remove o registro do confrontofiscal no Supabase (para notas excluídas do WinThor)"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscal?id=eq.{confronto_id}"
        response = requests.delete(url, headers=HEADERS, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] confrontofiscal #{confronto_id} REMOVIDO da base")
            return True
        else:
            log(f"  [ERRO] Falha ao remover confrontofiscal #{confronto_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao remover confrontofiscal #{confronto_id}: {e}")
        return False


def remover_da_verificando_tomador(registro_id):
    """Remove o registro da tabela verificando_tomador após conclusão"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificando_tomador?id=eq.{registro_id}"
        response = requests.delete(url, headers=HEADERS, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] verificando_tomador #{registro_id} removido")
            return True
        else:
            log(f"  [ERRO] Falha remover verificando_tomador #{registro_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão remover verificando_tomador #{registro_id}: {e}")
        return False


def processar_registro(registro):
    """
    Processa um único registro de Tomador Divergente:
    
    CASO 1: CNPJ Tomador XML começa com "36899" (é da empresa)
      → Foi importado na filial errada
      → Verificar se foi transferido para a filial correta no WinThor
      → A filial correta é aquela cujo CNPJ = cnpj_tomador do XML
    
    CASO 2: CNPJ Tomador XML NÃO começa com "36899" (não é da empresa)
      → Deveria ter sido excluído do WinThor
      → Verificar se o registro ainda existe no Oracle
      → Se sumiu → remover da nossa base
      → Se ainda existe → manter como divergente
    """
    registro_id = registro.get("id")
    confronto_id = registro.get("confronto_id")
    numtransent = registro.get("numtransent")
    numnota = registro.get("numnota")
    codfilial_atual = str(registro.get("codfilial") or "")
    cnpj_tomador_xml = registro.get("cnpj_tomador") or ""
    cnpj_filial_db = registro.get("cnpj_filial") or ""
    dtent = registro.get("dtent")  # yyyy-mm-dd
    user_nome = registro.get("user_nome")
    user_email = registro.get("user_email")

    if not numtransent or not confronto_id:
        log(f"  [SKIP] Registro #{registro_id} sem numtransent ou confronto_id")
        atualizar_status_verificando_tomador(registro_id, "Erro", "Dados insuficientes")
        return False

    log(f"  Processando: Nota {numnota} | Filial {codfilial_atual} | NT {numtransent} | CNPJ Tomador: {cnpj_tomador_xml}")

    is_empresa = cnpj_tomador_xml.startswith(CNPJ_PREFIX)

    connection = None
    try:
        connection = oracledb.connect(
            user=ORACLE_USUARIO,
            password=ORACLE_SENHA,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()

        if is_empresa:
            # ═══════════════════════════════════════════════════════════════
            # CASO 1: CNPJ é da empresa (36899...) 
            # Foi importado na filial errada, verificar se transferiram
            # ═══════════════════════════════════════════════════════════════
            log(f"  [CASO 1] CNPJ Tomador é da empresa (começa com {CNPJ_PREFIX})")
            
            # Descobrir qual é a filial CORRETA baseada no CNPJ do tomador
            filial_correta = FILIAIS_MAP.get(cnpj_tomador_xml)
            if not filial_correta:
                msg = f"CNPJ Tomador {cnpj_tomador_xml} não mapeado a nenhuma filial conhecida"
                log(f"  [ERRO] {msg}")
                atualizar_status_verificando_tomador(registro_id, "Erro", msg)
                return False
            
            log(f"  Filial correta para o CNPJ {cnpj_tomador_xml}: filial {filial_correta}")
            log(f"  Filial atual (onde foi importado errado): filial {codfilial_atual}")
            
            # Buscar no WinThor se a nota existe na filial CORRETA
            # (busca pelo numnota na filial correta, com mesmo espécie CT)
            sql_check_transferido = """
                SELECT n.numtransent, n.vltotal, 
                       NVL(b.vlicms, 0) AS vlicms,
                       n.codfilial, n.conferido
                FROM   pcnfent n, pcnfbaseent b
                WHERE  b.numtransent = n.numtransent
                  AND  n.numnota = :numnota
                  AND  n.codfilial = :codfilial_correto
                  AND  n.especie = 'CT'
                  AND  b.especie = 'CT'
                  AND  n.dtcancel is null
            """
            
            params_check = {
                "numnota": numnota,
                "codfilial_correto": filial_correta
            }
            
            cursor.execute(sql_check_transferido, params_check)
            resultado = cursor.fetchone()
            
            if resultado:
                # Nota foi encontrada na filial correta! 
                wt_numtransent = resultado[0]
                wt_vltotal = float(resultado[1] or 0)
                wt_vlicms = float(resultado[2] or 0)
                wt_codfilial = str(resultado[3])
                wt_conferido = resultado[4]
                
                log(f"  [ENCONTRADO] Nota {numnota} encontrada na filial {filial_correta} (NT: {wt_numtransent})")
                
                # Verificar valores XML vs WinThor (filial correta)
                vltotal_xml = float(registro.get("vltotal_xml") or 0)
                vlicms_xml = float(registro.get("vlicms_xml") or 0)
                
                motivos_diverge = []
                if abs(wt_vltotal - vltotal_xml) > 0.01:
                    motivos_diverge.append(f"TOTAL (WT:{wt_vltotal:.2f} != XML:{vltotal_xml:.2f})")
                if abs(wt_vlicms - vlicms_xml) > 0.01:
                    motivos_diverge.append(f"ICMS (WT:{wt_vlicms:.2f} != XML:{vlicms_xml:.2f})")
                
                if motivos_diverge:
                    # Transferiu mas valores divergem
                    msg = f"Transferido p/ filial {filial_correta}, mas valores divergem: {', '.join(motivos_diverge)}"
                    log(f"  [DIVERGE] {msg}")
                    atualizar_confronto_status(
                        confronto_id, "Divergente", 
                        obs=f"Transferido p/ filial correta ({filial_correta}), mas {', '.join(motivos_diverge)}",
                        codfilial_novo=int(filial_correta),
                        cnpj_filial_novo=cnpj_tomador_xml
                    )
                    remover_da_verificando_tomador(registro_id)
                    return True
                else:
                    # Tudo OK! Transferido e valores batem
                    log(f"  [OK] Nota transferida com sucesso para filial {filial_correta} e valores conferem!")
                    atualizar_confronto_status(
                        confronto_id, "OK",
                        obs=f"Transferido de filial {codfilial_atual} → {filial_correta}. Tomador OK.",
                        codfilial_novo=int(filial_correta),
                        cnpj_filial_novo=cnpj_tomador_xml,
                        user_nome=user_nome,
                        user_email=user_email
                    )
                    remover_da_verificando_tomador(registro_id)
                    return True
            else:
                # Nota NÃO foi encontrada na filial correta → ainda não foi transferida
                log(f"  [PENDENTE] Nota {numnota} AINDA NÃO foi transferida para filial {filial_correta}")
                
                # Verificar se a nota original ainda existe na filial errada
                sql_check_original = """
                    SELECT n.numtransent, n.codfilial
                    FROM   pcnfent n
                    WHERE  n.numtransent = :numtransent
                      AND  n.codfilial = :codfilial
                      AND  n.especie = 'CT'
                      AND  n.dtcancel is null
                """
                cursor.execute(sql_check_original, {
                    "numtransent": numtransent,
                    "codfilial": codfilial_atual
                })
                original = cursor.fetchone()
                
                if original:
                    msg = f"Nota ainda na filial errada ({codfilial_atual}). Aguardando transferência para filial {filial_correta}."
                    log(f"  [AGUARDANDO] {msg}")
                    atualizar_status_verificando_tomador(registro_id, "Erro", msg)
                else:
                    msg = f"Nota removida da filial {codfilial_atual} mas não encontrada na filial {filial_correta}."
                    log(f"  [ALERTA] {msg}")
                    atualizar_status_verificando_tomador(registro_id, "Erro", msg)
                
                return False
        
        else:
            # ═══════════════════════════════════════════════════════════════
            # CASO 2: CNPJ NÃO é da empresa
            # Deveria ter sido excluído do WinThor
            # ═══════════════════════════════════════════════════════════════
            log(f"  [CASO 2] CNPJ Tomador NÃO é da empresa (não começa com {CNPJ_PREFIX})")
            log(f"  Verificando se a nota foi excluída do WinThor...")
            
            # Verificar se o registro AINDA EXISTE no Oracle
            sql_check_existe = """
                SELECT n.numtransent, n.codfilial, n.numnota, n.dtcancel
                FROM   pcnfent n
                WHERE  n.numtransent = :numtransent
                  AND  n.codfilial = :codfilial
                  AND  n.especie = 'CT'
                  AND  n.dtcancel is null
            """
            
            params_existe = {
                "numtransent": numtransent,
                "codfilial": codfilial_atual
            }
            
            cursor.execute(sql_check_existe, params_existe)
            resultado = cursor.fetchone()
            
            if resultado is None:
                # Registro NÃO existe mais no Oracle → foi excluído! 
                log(f"  [EXCLUÍDO] Nota {numnota} (NT:{numtransent}) não existe mais no WinThor → REMOVENDO da base")
                deletar_confronto(confronto_id)
                remover_da_verificando_tomador(registro_id)
                return True
            else:
                # Registro AINDA existe no Oracle → NÃO foi excluído ainda
                msg = f"Nota {numnota} AINDA existe no WinThor (filial {codfilial_atual}). Aguardando exclusão manual."
                log(f"  [PENDENTE] {msg}")
                atualizar_status_verificando_tomador(registro_id, "Erro", msg)
                return False

    except Exception as e:
        log(f"  [ERRO ORACLE] {e}")
        atualizar_status_verificando_tomador(registro_id, "Erro", str(e)[:200])
        return False

    finally:
        if connection:
            try:
                connection.close()
            except:
                pass


def processar_lote(registros):
    """Processa todos os registros pendentes"""
    if not registros:
        return

    total = len(registros)
    log(f"\n{'='*60}")
    log(f"  PROCESSAMENTO TOMADOR DIF FILIAL: {total} registro(s)")
    log(f"{'='*60}")

    # Marcar como 'Processando'
    for reg in registros:
        atualizar_status_verificando_tomador(reg.get("id"), "Processando")

    sucesso = 0
    erros = 0

    for reg in registros:
        try:
            ok = processar_registro(reg)
            if ok:
                sucesso += 1
            else:
                erros += 1
        except Exception as e:
            log(f"  [ERRO] #{reg.get('id')}: {e}")
            atualizar_status_verificando_tomador(reg.get("id"), "Erro", str(e)[:200])
            erros += 1

    log(f"\n  {'='*60}")
    log(f"  LOTE FINALIZADO: {sucesso} sucesso(s) | {erros} erro(s)")
    log(f"  {'='*60}")


def main():
    log("=" * 60)
    log("AUTOMAÇÃO - VERIFICAR TOMADOR DIF FILIAL")
    log("Monitorando tabela 'verificando_tomador' no Supabase...")
    log(f"Oracle User: {ORACLE_USUARIO} | DSN: {ORACLE_DSN}")
    log(f"Intervalo de polling: {POLL_INTERVAL}s")
    log("=" * 60)

    while True:
        try:
            pendentes = buscar_pendentes()

            if pendentes:
                log(f"\n>>> {len(pendentes)} registro(s) pendente(s) encontrado(s)")
                processar_lote(pendentes)

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

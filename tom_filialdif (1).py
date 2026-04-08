"""
AUTOMAÇÃO DE VERIFICAÇÃO - TOMADOR DIF FILIAL
================================================
Este script roda em loop contínuo e:
1. Consulta a tabela 'verificandonfconsumo_tomador' no Supabase buscando registros com status 'Verificando'
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
CLIENT_LIB_DIR = r"C:\Users\informatica.ti\Documents\appdiscooveryzynapse\cmdintanci\instantclient_21_19"
ORACLE_USUARIO = "MIGRACAO"
ORACLE_SENHA = "fzabu69128XPKGY@!"
ORACLE_DSN = oracledb.makedsn("201.157.211.96", 1521, service_name="CS8NZK_190797_W_high.paas.oracle.com")

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
except Exception as e:
    print(f"Aviso: Não foi possível inicializar Oracle Client: {e}")

# --- CONFIGURAÇÕES SUPABASE ---
SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# Mapa CNPJ → Código Filial (todas as filiais da empresa Nutriport, CNPJ raiz 36123120)
FILIAIS_MAP = {
    "3612312000144": "1",
    "3612312000306": "2",
    "3612312000497": "3",
    "3612312000225": "4",
}

# Inverso: Código Filial → CNPJ
CODFILIAL_TO_CNPJ = {v: k for k, v in FILIAIS_MAP.items()}
# Adicionar as filiais duplicadas
CODFILIAL_TO_CNPJ["11"] = "3612312000144"
CODFILIAL_TO_CNPJ["22"] = "3612312000306"
CODFILIAL_TO_CNPJ["33"] = "3612312000497"

CNPJ_PREFIX = "36123"

# Intervalo de polling em segundos
POLL_INTERVAL = 2


def log(msg):
    """Log com timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def buscar_pendentes():
    """Busca registros com status 'Verificando' na tabela verificandonfconsumo_tomador"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo_tomador?status=eq.Verificando&select=*"
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
    """Atualiza o status de um registro na tabela verificandonfconsumo_tomador"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo_tomador?id=eq.{registro_id}"
        payload = {"status": novo_status}
        if obs_extra:
            payload["obs"] = obs_extra
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Status verificandonfconsumo_tomador #{registro_id} → '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha atualizar verificandonfconsumo_tomador #{registro_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão atualizar verificandonfconsumo_tomador #{registro_id}: {e}")
        return False


def atualizar_confronto_status(confronto_id, novo_status, obs=None, codfilial_novo=None, cnpj_filial_novo=None, codfilialnf_novo=None, user_nome=None, user_email=None):
    """Atualiza o status do confrontofiscalnfconsumo no Supabase"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}"
        payload = {"status": novo_status}
        
        if obs is not None:
            payload["obs"] = obs
        if codfilial_novo is not None:
            payload["codfilial"] = codfilial_novo
        if cnpj_filial_novo is not None:
            payload["cnpj_filial"] = cnpj_filial_novo
        if codfilialnf_novo is not None:
            payload["codfilialnf"] = codfilialnf_novo
        
        if novo_status == "OK" and user_email:
            payload["validado_por"] = user_nome
            payload["validado_email"] = user_email
            payload["validado_em"] = datetime.utcnow().isoformat() + "Z"
        
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] confrontofiscalnfconsumo #{confronto_id} → '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha atualizar confrontofiscalnfconsumo #{confronto_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão atualizar confrontofiscalnfconsumo #{confronto_id}: {e}")
        return False


def deletar_confronto(confronto_id):
    """Remove o registro do confrontofiscalnfconsumo no Supabase (para notas excluídas do WinThor)"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}"
        response = requests.delete(url, headers=HEADERS, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] confrontofiscalnfconsumo #{confronto_id} REMOVIDO da base")
            return True
        else:
            log(f"  [ERRO] Falha ao remover confrontofiscalnfconsumo #{confronto_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao remover confrontofiscalnfconsumo #{confronto_id}: {e}")
        return False


def remover_da_verificando_tomador(registro_id):
    """Remove o registro da tabela verificandonfconsumo_tomador após conclusão"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo_tomador?id=eq.{registro_id}"
        response = requests.delete(url, headers=HEADERS, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] verificandonfconsumo_tomador #{registro_id} removido")
            return True
        else:
            log(f"  [ERRO] Falha remover verificandonfconsumo_tomador #{registro_id}: {response.status_code}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão remover verificandonfconsumo_tomador #{registro_id}: {e}")
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
            
            # Buscar no WinThor se a nota existe com a FilialNF CORRETA
            # (FilialNF é o campo que precisa bater com o tomador)
            sql_check_transferido = """
                SELECT n.numtransent, n.vltotal, 
                       NVL(b.vlicms, 0) AS vlicms,
                       n.codfilial, n.codfilialnf, n.conferido
                FROM   pcnfent n, pcnfbaseent b
                WHERE  b.numtransent = n.numtransent
                  AND  n.numnota = :numnota
                  AND  n.codfilialnf = :codfilialnf_correto
                  AND  n.especie = 'NF'
                  AND  b.especie = 'NF'
                  AND  n.dtcancel is null
            """
            
            params_check = {
                "numnota": numnota,
                "codfilialnf_correto": filial_correta
            }
            
            cursor.execute(sql_check_transferido, params_check)
            resultado = cursor.fetchone()
            
            if resultado:
                # Nota foi encontrada com a FilialNF correta! 
                wt_numtransent = resultado[0]
                wt_vltotal = float(resultado[1] or 0)
                wt_vlicms = float(resultado[2] or 0)
                wt_codfilial = str(resultado[3])
                wt_codfilialnf = str(resultado[4])
                wt_conferido = resultado[5]
                
                log(f"  [ENCONTRADO] Nota {numnota} com FilialNF={wt_codfilialnf} (Filial={wt_codfilial}, NT: {wt_numtransent})")
                
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
                    msg = f"Transferido p/ FilialNF {filial_correta}, mas valores divergem: {', '.join(motivos_diverge)}"
                    log(f"  [DIVERGE] {msg}")
                    atualizar_confronto_status(
                        confronto_id, "Divergente", 
                        obs=f"FilialNF corrigida ({filial_correta}), mas {', '.join(motivos_diverge)}",
                        codfilial_novo=int(wt_codfilial),
                        cnpj_filial_novo=cnpj_tomador_xml,
                        codfilialnf_novo=int(filial_correta)
                    )
                    remover_da_verificando_tomador(registro_id)
                    return True
                else:
                    # Tudo OK! FilialNF correta e valores batem
                    log(f"  [OK] Nota com FilialNF correta ({filial_correta}) e valores conferem!")
                    atualizar_confronto_status(
                        confronto_id, "OK",
                        obs=f"FilialNF corrigida de {codfilial_atual} → {filial_correta}. Tomador OK.",
                        codfilial_novo=int(wt_codfilial),
                        cnpj_filial_novo=cnpj_tomador_xml,
                        codfilialnf_novo=int(filial_correta),
                        user_nome=user_nome,
                        user_email=user_email
                    )
                    remover_da_verificando_tomador(registro_id)
                    return True
            else:
                # Nota NÃO foi encontrada com FilialNF correta → ainda não corrigiram
                log(f"  [PENDENTE] Nota {numnota} AINDA NÃO tem FilialNF={filial_correta}")
                
                # Verificar se a nota original ainda existe na filial errada
                sql_check_original = """
                    SELECT n.numtransent, n.codfilial
                    FROM   pcnfent n
                    WHERE  n.numtransent = :numtransent
                      AND  n.codfilial = :codfilial
                      AND  n.especie = 'NF'
                      AND  n.dtcancel is null
                """
                cursor.execute(sql_check_original, {
                    "numtransent": numtransent,
                    "codfilial": codfilial_atual
                })
                original = cursor.fetchone()
                
                if original:
                    msg = f"Nota ainda com FilialNF errada ({codfilial_atual}). Aguardando correção para FilialNF={filial_correta}."
                    log(f"  [AGUARDANDO] {msg}")
                    atualizar_status_verificando_tomador(registro_id, "Erro", msg)
                else:
                    msg = f"Nota não encontrada na filial {codfilial_atual} nem com FilialNF={filial_correta}."
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
                  AND  n.especie = 'NF'
                  AND  n.dtcancel is null
            """
            
            params_existe = {
                "numtransent": numtransent,
                "codfilial": codfilial_atual
            }
            
            cursor.execute(sql_check_existe, params_existe)
            resultado = cursor.fetchone()
            
            if resultado is None:
                # Registro NÃO existe mais no Oracle → foi excluído com sucesso!
                # Manter o registro na base mas marcar como Resolvido
                log(f"  [EXCLUÍDO] Nota {numnota} (NT:{numtransent}) não existe mais no WinThor → marcando como Resolvido")
                atualizar_confronto_status(
                    confronto_id, "Resolvido",
                    obs=f"Nota excluída do WinThor (CNPJ tomador {cnpj_tomador_xml} não é da empresa). Removida com sucesso.",
                    user_nome=user_nome,
                    user_email=user_email
                )
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
    log("Monitorando tabela 'verificandonfconsumo_tomador' no Supabase...")
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

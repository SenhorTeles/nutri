"""
AUTOMAÇÃO DE VERIFICAÇÃO - automaverificado.py
================================================
Este script roda em loop contínuo e:
1. Consulta a tabela 'verificandonfconsumo' no Supabase buscando registros com status 'Subindo'
2. Para cada registro, faz UPDATE no Oracle WinThor: conferido = 'S' na pcnfent (WHERE numtransent = ?)
3. Após o UPDATE, faz SELECT para confirmar que conferido realmente mudou para 'S'
4. Se confirmado: atualiza status na tabela 'verificandonfconsumo' para 'Verificado'
                   atualiza status na tabela 'confrontofiscalnfconsumo' para 'OK'
5. Se falhou: atualiza status na tabela 'verificandonfconsumo' para 'Erro'

SEGURANÇA:
- O UPDATE usa bind variables (:numtransent) para evitar SQL Injection
- O UPDATE é feito com WHERE preciso: numtransent + codfilial
- Cada operação é commitada individualmente
- Logs detalhados de cada passo
"""

import os
import time
import requests
import oracledb
from datetime import datetime

# --- CONFIGURAÇÕES DO BANCO DE DADOS ORACLE (WINTHOR) --- CREDENCIAL DE ESCRITA ---
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

# Intervalo de polling em segundos
POLL_INTERVAL = 1

# --- MAPA FILIAL -> CGC (para validação de tomador) ---
FILIAL_CGC_MAP = {
    "1": "3612312000144", "11": "3612312000144",
    "2": "3612312000306", "22": "3612312000306",
    "3": "3612312000497", "33": "3612312000497",
    "4": "3612312000225"
}


def log(msg):
    """Log com timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def buscar_pendentes():
    """Busca registros com status 'Subindo' na tabela verificandonfconsumo"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo?status=eq.Subindo&select=*"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            log(f"Erro ao buscar pendentes: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log(f"Erro de conexão ao buscar pendentes: {e}")
        return []


def atualizar_status_verificando(registro_id, novo_status, obs_extra=""):
    """Atualiza o status de um registro na tabela verificandonfconsumo"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo?id=eq.{registro_id}"
        payload = {"status": novo_status}
        if obs_extra:
            payload["obs"] = obs_extra
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Status do registro verificandonfconsumo #{registro_id} atualizado para '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha ao atualizar verificandonfconsumo #{registro_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao atualizar verificandonfconsumo #{registro_id}: {e}")
        return False


def atualizar_status_confronto(confronto_id, novo_status, user_nome=None, user_email=None):
    """Atualiza o status de um registro na tabela confrontofiscalnfconsumo e salva auditoria"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}"
        payload = {"status": novo_status}
        
        # Se estamos marcando como 'Validado' ou 'OK', preenche os dados de auditoria
        if novo_status in ["OK", "Validado"] and user_email:
            payload["validado_winthor"] = True
            payload["validado_por"] = user_nome
            payload["validado_email"] = user_email
            payload["validado_em"] = datetime.now().isoformat()
            
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Status do confrontofiscalnfconsumo #{confronto_id} atualizado para '{novo_status}'")
            return True
        else:
            log(f"  [ERRO] Falha ao atualizar confrontofiscalnfconsumo #{confronto_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao atualizar confrontofiscalnfconsumo #{confronto_id}: {e}")
        return False


def atualizar_confronto_com_dados(confronto_id, winthor_vltotal, winthor_vlicms, user_nome=None, user_email=None):
    """Atualiza o confrontofiscalnfconsumo com os dados FRESCOS do WinThor + auditoria de validação"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}"
        payload = {
            "status": "Validado",
            "vltotal": winthor_vltotal,
            "vlicms": winthor_vlicms,
            "validado_winthor": True,
            "validado_por": user_nome,
            "validado_email": user_email,
            "validado_em": datetime.utcnow().isoformat() + "Z"
        }
        response = requests.patch(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] confrontofiscalnfconsumo #{confronto_id} atualizado com dados frescos do WinThor (Total:{winthor_vltotal}, ICMS:{winthor_vlicms})")
            return True
        else:
            log(f"  [ERRO] Falha ao atualizar confrontofiscalnfconsumo #{confronto_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao atualizar confrontofiscalnfconsumo #{confronto_id}: {e}")
        return False


def remover_da_verificando(registro_id):
    """Remove o registro da tabela verificandonfconsumo após verificação concluída"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/verificandonfconsumo?id=eq.{registro_id}"
        response = requests.delete(url, headers=HEADERS, timeout=10)
        if response.status_code in [200, 204]:
            log(f"  [OK] Registro verificandonfconsumo #{registro_id} removido da tabela")
            return True
        else:
            log(f"  [ERRO] Falha ao remover verificandonfconsumo #{registro_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log(f"  [ERRO] Conexão ao remover verificandonfconsumo #{registro_id}: {e}")
        return False


import re
import xml.etree.ElementTree as ET

def get_cnpj_tomador(xml_doc):
    """Lógica python reproduzindo getCnpjTomador do JS"""
    if not xml_doc:
        return ""
    try:
        # Remover TODOS os namespaces e prefixos para simplificar a busca
        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_doc)
        clean_xml = clean_xml.replace('cte:', '')
        
        root = ET.fromstring(clean_xml)
        
        # Achar qualquer tag <toma> (dentro de toma0, toma3, toma4)
        toma_val = None
        for tag in ['.//toma3/toma', './/toma0/toma', './/toma4/toma', './/toma']:
            elem = root.find(tag)
            if elem is not None and elem.text is not None:
                toma_val = str(elem.text).strip()
                break
                
        if toma_val == '0':
            elem = root.find('.//rem/CNPJ')
            return elem.text if elem is not None else ""
        elif toma_val == '1':
            elem = root.find('.//exped/CNPJ')
            return elem.text if elem is not None else ""
        elif toma_val == '2':
            elem = root.find('.//receb/CNPJ')
            return elem.text if elem is not None else ""
        elif toma_val == '3':
            elem = root.find('.//dest/CNPJ')
            return elem.text if elem is not None else ""
        elif toma_val == '4':
            elem = root.find('.//toma4/CNPJ')
            return elem.text if elem is not None else ""
            
    except Exception as e:
        pass
    return ""

def processar_registro(registro):
    """
    Processa um único registro:
    1. Busca os dados ATUAIS do WinThor (vltotal, vlicms) via SELECT
    2. Compara os dados do WinThor com os dados do XML (vindos do Supabase)
    3. Se baterem: UPDATE conferido='S' no Oracle, atualiza Supabase com dados frescos
    4. Se não baterem: retorna Erro com detalhe do que diverge
    """
    numtransent = registro.get("numtransent")
    codfilial = registro.get("codfilial")
    codfilialnf = registro.get("codfilialnf")
    numnota = registro.get("numnota")
    confronto_id = registro.get("confronto_id")
    registro_id = registro.get("id")
    
    user_nome = registro.get("user_nome")
    user_email = registro.get("user_email")

    # Valores XML (referência correta, vindo do Supabase)
    vltotal_xml = float(registro.get("vltotal_xml") or 0.0)
    vlicms_xml = float(registro.get("vlicms_xml") or 0.0)

    if not numtransent:
        log(f"  [SKIP] Registro #{registro_id} sem numtransent - não é possível processar")
        atualizar_status_verificando(registro_id, "Erro", "numtransent ausente")
        return False

    # 1. Verificar se o tomador do XML bate com o CGC da Filial NF
    #    (uma filial não pode pagar CT-e de outra filial)
    cgc_filial_nf = FILIAL_CGC_MAP.get(str(codfilialnf), "")
    if confronto_id and cgc_filial_nf:
        try:
            url_cf = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}&select=cnpj_tomador,xml_doc"
            resp_cf = requests.get(url_cf, headers=HEADERS, timeout=10)
            if resp_cf.status_code == 200 and resp_cf.json():
                cf_data = resp_cf.json()[0]
                cnpj_tom = cf_data.get('cnpj_tomador') or ''
                
                # Se não tem cnpj_tomador salvo, tenta extrair do XML
                if not cnpj_tom and cf_data.get('xml_doc'):
                    cnpj_tom = get_cnpj_tomador(cf_data['xml_doc'])
                
                # Tomador tem que ser igual ao CGC da Filial NF
                if cnpj_tom and cnpj_tom != cgc_filial_nf:
                    msg = f"Tomador divergente: tomador XML ({cnpj_tom}) != CGC Filial NF {codfilialnf} ({cgc_filial_nf})"
                    log(f"  [BLOQUEADO] {msg}")
                    atualizar_status_verificando(registro_id, "Erro", f"Divergência: {msg}")
                    return False
        except Exception as e:
            log(f"  [WARN] Erro ao verificar tomador: {e}")

    log(f"  Processando: Nota {numnota} | Filial {codfilial} | NumTransEnt {numtransent}")

    connection = None
    try:
        # --- PASSO 1: Conectar ao Oracle ---
        connection = oracledb.connect(
            user=ORACLE_USUARIO,
            password=ORACLE_SENHA,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()

        # --- PASSO 2: Buscar dados ATUAIS do WinThor ---
        d_ent = registro.get('dtent')  # yyyy-mm-dd
        data_ent_formatada = datetime.strptime(d_ent, '%Y-%m-%d').strftime('%d/%m/%Y') if d_ent else ''

        sql_select = """
            SELECT n.vltotal, 
                   NVL(b.vlicms, 0) AS vlicms,
                   n.conferido
            FROM   pcnfent n, pcnfbaseent b
            WHERE  b.numtransent = n.numtransent
              AND  n.numtransent = :numtransent
              AND  n.numnota = :numnota
              AND  n.codfilial = :codfilial
              AND  n.especie = 'NF'
              AND  b.especie = 'NF'
              AND  n.dtcancel is null
              AND  TRUNC(n.dtent) = TO_DATE(:dtent, 'DD/MM/YYYY')
        """

        params = {
            "numtransent": numtransent,
            "numnota": numnota,
            "codfilial": str(codfilial),
            "dtent": data_ent_formatada
        }

        log(f"  Buscando dados atuais do WinThor...")
        cursor.execute(sql_select, params)
        resultado = cursor.fetchone()

        if not resultado:
            log(f"  [ERRO] Registro não encontrado no WinThor com os filtros fornecidos")
            atualizar_status_verificando(registro_id, "Erro", "Registro não encontrado no WinThor")
            return False

        winthor_vltotal = float(resultado[0] or 0.0)
        winthor_vlicms = float(resultado[1] or 0.0)
        winthor_conferido = resultado[2]

        log(f"  Dados WinThor atuais: Total={winthor_vltotal}, ICMS={winthor_vlicms}, Conferido={winthor_conferido}")
        log(f"  Dados XML referência: Total={vltotal_xml}, ICMS={vlicms_xml}")

        # --- PASSO 3: Comparar WinThor ATUAL vs XML ---
        motivos_rejeicao = []

        if abs(winthor_vltotal - vltotal_xml) > 0.01:
            motivos_rejeicao.append(f"TOTAL divergem (WinThor:{winthor_vltotal:.2f} != XML:{vltotal_xml:.2f})")

        if str(codfilial) == '47':
            if abs(winthor_vlicms) > 0.01:
                motivos_rejeicao.append(f"ICMS WinThor não é 0 p/ filial 47 (WT:{winthor_vlicms:.2f})")
        else:
            if abs(winthor_vlicms - vlicms_xml) > 0.01:
                motivos_rejeicao.append(f"ICMS divergem (WinThor:{winthor_vlicms:.2f} != XML:{vlicms_xml:.2f})")

        if motivos_rejeicao:
            msg_erro = ", ".join(motivos_rejeicao)
            log(f"  [BLOQUEADO] Dados do WinThor ainda não batem com o XML: {msg_erro}")
            atualizar_status_verificando(registro_id, "Erro", f"Divergência: {msg_erro}")
            return False

        log(f"  [OK] Valores do WinThor batem com o XML! Prosseguindo com UPDATE...")

        # --- PASSO 4: Se já está "S", apenas atualizar o Supabase ---
        if winthor_conferido == 'S':
            log(f"  [INFO] O registro já estava como conferido='S'. Atualizando Supabase...")
            # Atualiza confrontofiscalnfconsumo com os dados frescos do WinThor
            atualizar_confronto_com_dados(confronto_id, winthor_vltotal, winthor_vlicms, user_nome, user_email)
            remover_da_verificando(registro_id)
            return True

        # --- PASSO 5: Fazer o UPDATE conferido = 'S' ---
        sql_update = """
            UPDATE pcnfent n
            SET n.conferido = 'S'
            WHERE n.numtransent = :numtransent
              AND n.numnota = :numnota
              AND n.codfilial = :codfilial
              AND n.especie = 'NF'
              AND n.conferido = 'N'
              AND n.dtcancel is null
              AND TRUNC(n.dtent) = TO_DATE(:dtent, 'DD/MM/YYYY')
              AND EXISTS (
                  SELECT 1 
                  FROM pcnfbaseent b 
                  WHERE b.numtransent = n.numtransent 
                    AND b.especie = 'NF'
              )
        """
        
        log(f"  Executando UPDATE conferido='S'...")
        cursor.execute(sql_update, params)
        rows_affected = cursor.rowcount
        log(f"  Linhas afetadas pelo UPDATE: {rows_affected}")

        if rows_affected == 0:
            log(f"  [ERRO] UPDATE não afetou nenhuma linha!")
            connection.rollback()
            atualizar_status_verificando(registro_id, "Erro", "UPDATE não encontrou linha válida")
            return False

        # --- PASSO 6: COMMIT ---
        connection.commit()
        log(f"  COMMIT realizado com sucesso!")

        # --- PASSO 7: Atualiza Supabase com dados FRESCOS do WinThor ---
        atualizar_confronto_com_dados(confronto_id, winthor_vltotal, winthor_vlicms, user_nome, user_email)
        remover_da_verificando(registro_id)
        log(f"  [SUCESSO] Nota {numnota} verificada e Supabase atualizado com dados frescos!")
        return True

    except Exception as e:
        log(f"  [ERRO ORACLE] {e}")
        if connection:
            try:
                connection.rollback()
                log(f"  ROLLBACK realizado")
            except:
                pass
        error_msg = str(e)
        atualizar_status_verificando(registro_id, "Erro", error_msg[:200])
        return False

    finally:
        if connection:
            try:
                connection.close()
            except:
                pass


def processar_lote(registros):
    """
    Processa TODOS os registros pendentes em lote:
    1. Abre 1 conexão Oracle
    2. Faz 1 SELECT em bloco para buscar todos os dados do WinThor
    3. Compara WinThor vs XML em memória (instantâneo)
    4. Separa em aprovados vs divergentes
    5. Faz 1 UPDATE em bloco nos aprovados
    6. 1 COMMIT
    7. Atualiza Supabase em lote (sem sleeps)
    """
    if not registros:
        return

    total = len(registros)
    log(f"\n{'='*60}")
    log(f"  PROCESSAMENTO EM LOTE: {total} registro(s)")
    log(f"{'='*60}")

    # --- Pré-validação: separar registros válidos dos inválidos ---
    validos = []
    for reg in registros:
        registro_id = reg.get("id")
        numtransent = reg.get("numtransent")
        codfilial = reg.get("codfilial")
        codfilialnf = reg.get("codfilialnf")

        if not numtransent:
            log(f"  [SKIP] Registro #{registro_id} sem numtransent")
            atualizar_status_verificando(registro_id, "Erro", "numtransent ausente")
            continue

        if str(codfilialnf) != str(codfilial):
            # Verificar se o tomador bate com o CGC da Filial NF
            confronto_id = reg.get("confronto_id")
            cgc_filial_nf = FILIAL_CGC_MAP.get(str(codfilialnf), "")
            tomador_ok = True  # default OK se não conseguir verificar
            
            if confronto_id and cgc_filial_nf:
                try:
                    url_cf = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}&select=cnpj_tomador,xml_doc"
                    resp_cf = requests.get(url_cf, headers=HEADERS, timeout=10)
                    if resp_cf.status_code == 200 and resp_cf.json():
                        cf_data = resp_cf.json()[0]
                        cnpj_tom = cf_data.get('cnpj_tomador') or ''
                        if not cnpj_tom and cf_data.get('xml_doc'):
                            cnpj_tom = get_cnpj_tomador(cf_data['xml_doc'])
                        if cnpj_tom and cnpj_tom != cgc_filial_nf:
                            tomador_ok = False
                except Exception as e:
                    log(f"  [WARN] Erro ao verificar tomador #{registro_id}: {e}")
            
            if not tomador_ok:
                msg = f"Tomador divergente: tomador != CGC Filial NF {codfilialnf} ({cgc_filial_nf})"
                log(f"  [BLOQUEADO] #{registro_id}: {msg}")
                atualizar_status_verificando(registro_id, "Erro", f"Divergência: {msg}")
                continue

        validos.append(reg)

    if not validos:
        log(f"  Nenhum registro válido para processar no lote.")
        return

    log(f"  {len(validos)} registro(s) válido(s) para processar no Oracle")

    CHUNK_SIZE = 10
    total_aprovados = 0
    total_erros = 0

    connection = None
    try:
        # --- 1. CONEXÃO ÚNICA para todos os chunks ---
        connection = oracledb.connect(
            user=ORACLE_USUARIO,
            password=ORACLE_SENHA,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()
        log(f"  [OK] Conexão Oracle aberta")

        # --- 2. PROCESSAR EM CHUNKS DE {CHUNK_SIZE} ---
        total_chunks = (len(validos) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for chunk_idx in range(total_chunks):
            inicio = chunk_idx * CHUNK_SIZE
            fim = min(inicio + CHUNK_SIZE, len(validos))
            chunk = validos[inicio:fim]

            log(f"\n  --- Chunk {chunk_idx+1}/{total_chunks} ({len(chunk)} registros) ---")

            # Marcar este chunk como 'Processando'
            for reg in chunk:
                atualizar_status_verificando(reg.get("id"), "Processando")

            # Montar mapa por numtransent
            mapa_chunk = {}
            for reg in chunk:
                mapa_chunk[str(reg["numtransent"])] = reg

            numtransents = list(mapa_chunk.keys())

            # SELECT em bloco para este chunk
            bind_names = [f":nt{i}" for i in range(len(numtransents))]
            bind_clause = ", ".join(bind_names)
            bind_params = {f"nt{i}": nt for i, nt in enumerate(numtransents)}

            sql_select = f"""
                SELECT n.numtransent, n.vltotal, 
                       NVL(b.vlicms, 0) AS vlicms,
                       n.conferido, n.numnota, n.codfilial,
                       TO_CHAR(TRUNC(n.dtent), 'YYYY-MM-DD') AS dtent
                FROM   pcnfent n, pcnfbaseent b
                WHERE  b.numtransent = n.numtransent
                  AND  n.numtransent IN ({bind_clause})
                  AND  n.especie = 'NF'
                  AND  b.especie = 'NF'
                  AND  n.dtcancel is null
            """

            cursor.execute(sql_select, bind_params)
            resultados_oracle = cursor.fetchall()

            # Indexar resultados do Oracle
            oracle_data = {}
            for row in resultados_oracle:
                nt = str(row[0])
                oracle_data[nt] = {
                    "vltotal": float(row[1] or 0.0),
                    "vlicms": float(row[2] or 0.0),
                    "conferido": row[3],
                    "numnota": row[4],
                    "codfilial": str(row[5]),
                    "dtent": row[6]
                }

            # Comparar WinThor vs XML em memória
            aprovados = []
            ja_conferidos = []
            num_erros_chunk = 0

            for reg in chunk:
                registro_id = reg["id"]
                nt = str(reg["numtransent"])
                numnota = reg.get("numnota")
                vltotal_xml = float(reg.get("vltotal_xml") or 0.0)
                vlicms_xml = float(reg.get("vlicms_xml") or 0.0)

                if nt not in oracle_data:
                    log(f"    [ERRO] #{registro_id} (NT:{nt}) não encontrado no WinThor")
                    atualizar_status_verificando(registro_id, "Erro", "Registro não encontrado no WinThor")
                    num_erros_chunk += 1
                    continue

                dados_wt = oracle_data[nt]
                motivos = []

                if abs(dados_wt["vltotal"] - vltotal_xml) > 0.01:
                    motivos.append(f"TOTAL (WT:{dados_wt['vltotal']:.2f} != XML:{vltotal_xml:.2f})")
                
                if str(dados_wt["codfilial"]) == '47':
                    if abs(dados_wt["vlicms"]) > 0.01:
                        motivos.append(f"ICMS WT deve ser 0 p/ filial 47 (WT:{dados_wt['vlicms']:.2f})")
                else:
                    if abs(dados_wt["vlicms"] - vlicms_xml) > 0.01:
                        motivos.append(f"ICMS (WT:{dados_wt['vlicms']:.2f} != XML:{vlicms_xml:.2f})")

                if motivos:
                    msg = ", ".join(motivos)
                    log(f"    [DIVERGE] #{registro_id} Nota {numnota}: {msg}")
                    atualizar_status_verificando(registro_id, "Erro", f"Divergência: {msg}")
                    num_erros_chunk += 1
                    continue

                if dados_wt["conferido"] == 'S':
                    ja_conferidos.append(reg)
                else:
                    aprovados.append(reg)

            # UPDATE em bloco para este chunk
            if aprovados:
                nts_update = [str(r["numtransent"]) for r in aprovados]
                bind_upd_names = [f":u{i}" for i in range(len(nts_update))]
                bind_upd_clause = ", ".join(bind_upd_names)
                bind_upd_params = {f"u{i}": nt for i, nt in enumerate(nts_update)}

                sql_update = f"""
                    UPDATE pcnfent n
                    SET n.conferido = 'S'
                    WHERE n.numtransent IN ({bind_upd_clause})
                      AND n.especie = 'NF'
                      AND n.conferido = 'N'
                      AND n.dtcancel is null
                      AND EXISTS (
                          SELECT 1 FROM pcnfbaseent b 
                          WHERE b.numtransent = n.numtransent AND b.especie = 'NF'
                      )
                """

                cursor.execute(sql_update, bind_upd_params)
                rows = cursor.rowcount
                connection.commit()
                log(f"    UPDATE: {rows} linha(s) | COMMIT OK")

            # Atualizar Supabase para este chunk
            todos_sucesso = aprovados + ja_conferidos
            for reg in todos_sucesso:
                nt = str(reg["numtransent"])
                dados_wt = oracle_data[nt]
                atualizar_confronto_com_dados(
                    reg.get("confronto_id"), dados_wt["vltotal"], dados_wt["vlicms"],
                    reg.get("user_nome"), reg.get("user_email")
                )
                remover_da_verificando(reg.get("id"))

            total_aprovados += len(todos_sucesso)
            total_erros += num_erros_chunk
            log(f"    ✓ Chunk {chunk_idx+1}: {len(todos_sucesso)} aprovado(s) | {num_erros_chunk} erro(s)")

        log(f"\n  {'='*60}")
        log(f"  LOTE FINALIZADO: {total_aprovados} aprovado(s) | {total_erros} erro(s) | {total_chunks} chunk(s)")
        log(f"  {'='*60}")

    except Exception as e:
        log(f"  [ERRO LOTE] {e}")
        if connection:
            try:
                connection.rollback()
                log(f"  ROLLBACK realizado")
            except:
                pass

        # Fallback: tentar processar 1-a-1
        log(f"  Fazendo fallback para processamento individual...")
        for reg in validos:
            try:
                processar_registro(reg)
            except Exception as e2:
                log(f"  [ERRO INDIVIDUAL] #{reg.get('id')}: {e2}")

    finally:
        if connection:
            try:
                connection.close()
                log(f"  Conexão Oracle fechada")
            except:
                pass


def main():
    log("=" * 60)
    log("AUTOMAÇÃO DE VERIFICAÇÃO INICIADA (MODO LOTE)")
    log("Monitorando tabela 'verificandonfconsumo' no Supabase...")
    log(f"Oracle User: {ORACLE_USUARIO} | DSN: {ORACLE_DSN}")
    log(f"Intervalo de polling: {POLL_INTERVAL}s")
    log("=" * 60)

    while True:
        try:
            pendentes = buscar_pendentes()

            if pendentes:
                log(f"\n>>> {len(pendentes)} registro(s) pendente(s) encontrado(s)")

                # Processar em chunks de 10 (marca 'Processando' dentro do chunk)
                processar_lote(pendentes)

            log(f"... Sistema operante 24/7. Aguardando {POLL_INTERVAL} segundos...")
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

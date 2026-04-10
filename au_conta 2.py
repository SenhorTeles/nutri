import sys
import traceback

def __global_exception_handler(exctype, value, tb):
    print(f"\n[ERRO FATAL DETECTADO]")
    print(f"Tipo: {exctype.__name__}")
    print(f"Mensagem: {value}")
    traceback.print_tb(tb)
    input("\nPressione ENTER para sair do programa...")
    sys.exit(1)

sys.excepthook = __global_exception_handler

import os
import time
import requests
import oracledb
import xml.etree.ElementTree as ET
import re
import threading
import getpass
from datetime import datetime

# --- CREDENCIAIS DE ACESSO AO SUPABASE ---
SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}
HEADERS_GET = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json"
}

# Prefixo do CNPJ base da empresa de acordo com o json (36.123.120...)
CNPJ_PREFIX = "36123"

def log(msg, prefix="[MAIN]"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {prefix} {msg}")

# ==========================================================
# CONFIGURAÇÕES REMOTAS (SUPABASE)
# ==========================================================
def carregar_configuracoes_secretas():
    log("Baixando configurações e regras do servidor...", "[CONFIG]")
    url = f"{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1&select=config_json"
    resp = requests.get(url, headers=HEADERS_GET)
    if resp.status_code == 200 and resp.json():
        return resp.json()[0]['config_json']
    raise Exception("Falha ao baixar regras secretas.")

CONFIG = None
ORACLE_CONF = None
FILIAIS = []
FILIAIS_MAP = {}
FILIAL_CGC_MAP = {}
MODOS_SQL = {}
DSN = None

try:
    CONFIG = carregar_configuracoes_secretas()
    ORACLE_CONF = CONFIG['oracle_credentials']
    FILIAIS = CONFIG['filiais']
    FILIAIS_MAP = {f['cgc']: f['codigo'] for f in FILIAIS}
    FILIAL_CGC_MAP = {f['codigo']: f['cgc'] for f in FILIAIS}
    MODOS_SQL = CONFIG['sqls']

    if os.path.exists(ORACLE_CONF['client_lib_dir']):
        oracledb.init_oracle_client(lib_dir=ORACLE_CONF['client_lib_dir'])
    
    DSN = oracledb.makedsn(ORACLE_CONF['host'], ORACLE_CONF['port'], service_name=ORACLE_CONF['service_name'])

except Exception as e:
    import traceback
    print(f"ERRO CRÍTICO NA INICIALIZAÇÃO:")
    print(traceback.format_exc())
    input("\nPressione ENTER para sair...")
    import sys
    sys.exit(1)

def get_db_connection():
    return oracledb.connect(user=ORACLE_CONF['user'], password=ORACLE_CONF['password'], dsn=DSN)

# ==========================================================
# THREAD 1: AU_CONTA (Varredura de Notas e Atualização Front)
# ==========================================================
def buscar_dados_winthor(sql_query, codfilial, data_inicio, data_fim):
    sql = sql_query.replace('{codfilial}', str(codfilial))\
                   .replace('{data_inicio}', data_inicio)\
                   .replace('{data_fim}', data_fim)
    notas_dict = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [c[0].lower() for c in cursor.description]
        for row in cursor.fetchall():
            # Converter datetime para string para evitar erro de serialização JSON
            valores = []
            for v in row:
                if isinstance(v, datetime):
                    valores.append(v.strftime('%Y-%m-%d'))
                elif hasattr(v, 'isoformat'):
                    valores.append(v.isoformat())
                else:
                    valores.append(v)
            dado = dict(zip(cols, valores))
            chave = str(dado.get('chavenfe') or dado.get('chavecte') or '').strip()
            if chave and chave != 'None':
                notas_dict[chave] = dado
        conn.close()
    except Exception as e:
        log(f"Erro buscar_dados_winthor: {e}", "[AU_CONTA]")
    return notas_dict

def enviar_supabase(table_name, payload):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=chave_xml"
    headers = HEADERS.copy()
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    requests.post(url, headers=headers, json=payload)

def buscar_chaves_existentes(table_name, codfilial, data_inicio, data_fim):
    dt_inicio_f = datetime.strptime(data_inicio, '%d/%m/%Y').strftime('%Y-%m-%d')
    dt_fim_f = datetime.strptime(data_fim, '%d/%m/%Y').strftime('%Y-%m-%d')
    existentes = {}
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=chavecte,xml_doc&codfilial=eq.{codfilial}&dtent=gte.{dt_inicio_f}&dtent=lte.{dt_fim_f}"
    try:
        of, lim = 0, 1000
        while True:
            hdrs = HEADERS_GET.copy()
            hdrs["Range"] = f"{of}-{of+lim-1}"
            resp = requests.get(url, headers=hdrs)
            if resp.status_code == 200:
                data = resp.json()
                if not data: break
                for d in data:
                    if d.get("chavecte"):
                        existentes[d["chavecte"]] = True
                if len(data) < lim: break
                of += lim
            else: break
    except: pass
    return existentes

def avaliar_xml(table_name, reg):
    xml_doc = reg.get("xml_doc", "")
    if not xml_doc: return
    try:
        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_doc).replace('cte:', '').replace('nfe:', '')
        root = ET.fromstring(clean_xml)
        
        vltotal_xml, vlicms_xml, vlpis_xml, vlcofins_xml = 0.0, 0.0, 0.0, 0.0
        
        vt_el = root.find('.//vTPrest') or root.find('.//vNF')
        if vt_el is not None: vltotal_xml = float(vt_el.text)
            
        icms_tot = root.find('.//ICMSTot')
        if icms_tot is not None:
            v_icms = float(icms_tot.find('vICMS').text) if icms_tot.find('vICMS') is not None and icms_tot.find('vICMS').text else 0.0
            v_mono = float(icms_tot.find('vICMSMonoRet').text) if icms_tot.find('vICMSMonoRet') is not None and icms_tot.find('vICMSMonoRet').text else 0.0
            vlicms_xml = max(v_icms, v_mono)
        else:
            for t in ['ICMS00','ICMSOutraUF','ICMS20','ICMS45','ICMS60','ICMS90','ICMSSN','ICMS61']:
                n = root.find(f'.//{t}')
                if n is not None:
                    vicms = n.find('vICMS') or n.find('vICMSOutraUF') or n.find('vICMSMonoRet')
                    if vicms is not None:
                        vlicms_xml = float(vicms.text)
                        break
                    
        p_el, c_el = root.find('.//vPIS'), root.find('.//vCOFINS')
        if p_el is not None and p_el.text: vlpis_xml = float(p_el.text)
        if c_el is not None and c_el.text: vlcofins_xml = float(c_el.text)

        toma_val, cnpj_tomador = None, ""
        for tg in ['.//toma3/toma', './/toma0/toma', './/toma4/toma', './/toma']:
            e = root.find(tg)
            if e is not None and e.text:
                toma_val = e.text.strip()
                break
        
        t_map = {'0':'.//rem/CNPJ','1':'.//exped/CNPJ','2':'.//receb/CNPJ','3':'.//dest/CNPJ','4':'.//toma4/CNPJ'}
        if toma_val in t_map:
            e = root.find(t_map[toma_val])
            if e is not None: cnpj_tomador = e.text
            
        if not cnpj_tomador:
            d_cnpj = root.find('.//dest/CNPJ')
            if d_cnpj is not None: cnpj_tomador = d_cnpj.text
            
        cnpj_remetente = ""
        r_cnpj = root.find('.//rem/CNPJ') or root.find('.//emit/CNPJ')
        if r_cnpj is not None: cnpj_remetente = r_cnpj.text

        vltotal_db = float(reg.get("vltotal") or 0)
        vlicms_db = float(reg.get("vlicms") or 0)
        cnpj_filial = FILIAL_CGC_MAP.get(str(reg.get("codfilialnf")), reg.get("cnpj_filial", ""))
        
        diff_tomador = bool(cnpj_tomador and cnpj_filial and cnpj_tomador.lstrip('0') != cnpj_filial.lstrip('0'))
        diff_tot = abs(vltotal_db - vltotal_xml) > 0.01
        diff_icms = abs(vlicms_db - vlicms_xml) > 0.01

        status = "Tomador Divergente" if diff_tomador else ("DIVERGENTE" if diff_tot or diff_icms else "OK")
        
        obs = []
        if diff_tot: obs.append(f"TOTAL (DB: {vltotal_db} | XML: {vltotal_xml})")
        if diff_icms: obs.append(f"ICMS (DB: {vlicms_db} | XML: {vlicms_xml})")
        if diff_tomador: obs.append(f"Tomador divergente")
        
        payload = {
            "status": status,
            "vltotal_xml": vltotal_xml, "vlicms_xml": vlicms_xml,
            "vlpis_xml": vlpis_xml, "vlcofins_xml": vlcofins_xml,
            "cnpj_tomador": cnpj_tomador, "cnpj_remetente": cnpj_remetente,
            "obs": " | ".join(obs)
        }
        requests.patch(f"{SUPABASE_URL}/rest/v1/{table_name}?id=eq.{reg['id']}", headers=HEADERS, json=payload)
    except: pass

def _gerar_periodos_mensais(ano, mes_atual):
    """Gera lista de tuplas (data_inicio, data_fim) mês a mês de Janeiro até o mês atual."""
    from calendar import monthrange
    periodos = []
    for m in range(1, mes_atual + 1):
        di = f"01/{m:02d}/{ano}"
        ultimo_dia = monthrange(ano, m)[1]
        if m == mes_atual:
            ultimo_dia = min(ultimo_dia, int(datetime.now().strftime('%d')))
        df = f"{ultimo_dia:02d}/{m:02d}/{ano}"
        periodos.append((di, df))
    return periodos

BATCH_SIZE = 50  # Envia no máximo 50 notas por lote
BATCH_PAUSE = 2  # Pausa de 2 segundos entre lotes

def run_au_conta():
    log("Iniciando rotina.", "[AU_CONTA]")
    is_initial_run = True
    while True:
        try:
            # Fase 0: Avaliar XMLs pendentes (registros que já têm XML mas status "Sem XML")
            for nome, config in MODOS_SQL.items():
                resp = requests.get(f"{SUPABASE_URL}/rest/v1/{config['tbl_confronto']}?status=eq.Sem XML&xml_doc=neq.&select=*", headers=HEADERS_GET)
                if resp.status_code == 200:
                    registros = resp.json()
                    if isinstance(registros, list):
                        for r in registros:
                            avaliar_xml(config['tbl_confronto'], r)

            agora = datetime.now()

            if is_initial_run:
                periodos = _gerar_periodos_mensais(agora.year, agora.month)
                log(f"Executando ciclo inicial: {len(periodos)} meses para processar (Jan-{agora.strftime('%b')}/{agora.year}).", "[AU_CONTA]")
            else:
                primeiro_dia = f"01/{agora.month:02d}/{agora.year}"
                hoje = agora.strftime("%d/%m/%Y")
                periodos = [(primeiro_dia, hoje)]
                log(f"Executando ciclo regular: apenas mês atual ({primeiro_dia} a {hoje}).", "[AU_CONTA]")

            for idx_p, (dt_ini, dt_fim) in enumerate(periodos):
                log(f"--- Período {idx_p+1}/{len(periodos)}: {dt_ini} a {dt_fim} ---", "[AU_CONTA]")
                
                for f in FILIAIS:
                    log(f"  Filial {f['codigo']} ({f['uf']})...", "[AU_CONTA]")
                    for nome, config in MODOS_SQL.items():
                        log(f"    -> Módulo {nome}: Query WinThor...", "[AU_CONTA]")
                        notas = buscar_dados_winthor(config['varredura'], f["codigo"], dt_ini, dt_fim)
                        if not notas: 
                            log(f"        Sem resultados.", "[AU_CONTA]")
                            continue
                        
                        log(f"        {len(notas)} notas. Conferindo no Supabase...", "[AU_CONTA]")
                        exis = buscar_chaves_existentes(config['tbl_confronto'], f["codigo"], dt_ini, dt_fim)
                        novas = [c for c in notas.keys() if c not in exis]
                        
                        if not novas:
                            log(f"        Todas já existem.", "[AU_CONTA]")
                            continue
                            
                        log(f"        Enviando {len(novas)} notas NOVAS em lotes de {BATCH_SIZE}...", "[AU_CONTA]")
                        
                        enviadas = 0
                        for i in range(0, len(novas), BATCH_SIZE):
                            lote = novas[i:i+BATCH_SIZE]
                            for c in lote:
                                d = notas[c]
                                payload = d.copy() 
                                payload.update({
                                    "vltotal": float(d.get('vltotal') or 0), "vlicms": float(d.get('vlicms') or 0),
                                    "vlpis": float(d.get('vlpis') or 0), "vlcofins": float(d.get('vlcofins') or 0),
                                    "vltotal_xml": 0.0, "vlicms_xml": 0.0, "vlpis_xml": 0.0, "vlcofins_xml": 0.0,
                                    "chavecte": c, "chave_xml": c, "chave_winthor": c,
                                    "cnpj_filial": f['cgc'], 
                                    "dtemissao": str(d.get('dtemissao'))[:10] if d.get('dtemissao') else None,
                                    "dtent": str(d.get('dtent'))[:10] if d.get('dtent') else None, 
                                    "status": "Sem XML",
                                    "obs": "Aguardando xml"
                                })
                                enviar_supabase(config['tbl_confronto'], payload)
                            enviadas += len(lote)
                            log(f"          Lote enviado: {enviadas}/{len(novas)}", "[AU_CONTA]")
                            time.sleep(BATCH_PAUSE)
                            
                        log(f"        Upload de {nome} concluído!", "[AU_CONTA]")
                    
                    # Pausa entre filiais para não sobrecarregar
                    time.sleep(3)
                    
                log(f"--- Período {dt_ini} a {dt_fim} finalizado. ---", "[AU_CONTA]")
                time.sleep(5)  # Pausa entre meses
                    
            is_initial_run = False
            
        except Exception as e:
            log(f"Erro: {e}\n{traceback.format_exc()}", "[AU_CONTA]")
        
        log(f"Ciclo concluído. Dormindo por 2 horas...", "[AU_CONTA]")
        time.sleep(7200)

# ==========================================================
# THREAD 2: BUSCA ISOLADA (com comparação imediata)
# ==========================================================
def run_busca_isolada():
    log("Iniciando rotina.", "[BUSCA]")
    while True:
        try:
            resp = requests.get(f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?status=eq.Pendente", headers=HEADERS_GET)
            if resp.status_code != 200:
                time.sleep(5)
                continue
                
            pendentes = resp.json()
            if isinstance(pendentes, list) and pendentes:
                for p in pendentes:
                    chave = p.get('chavecte')
                    id_ = p.get('id')
                    requests.patch(f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{id_}", headers=HEADERS, json={"status": "Processando"})
                    
                    achou = False
                    tbl_achou = None
                    modulos_tentados = []
                    for nome, cfg in MODOS_SQL.items():
                        c_sql = f"'{chave}'"
                        sql = cfg['busca_isolada'].replace('{chaves_sql}', c_sql)
                        res = buscar_dados_winthor(sql, "", "", "")
                        if chave in res:
                            d = res[chave]
                            f_cgc = FILIAL_CGC_MAP.get(str(d.get('codfilialnf')), "")
                            
                            payload = d.copy()
                            payload.update({
                                "vltotal": float(d.get('vltotal') or 0), "vlicms": float(d.get('vlicms') or 0),
                                "chavecte": chave, "chave_xml": chave, "cnpj_filial": f_cgc, 
                                "status": "Sem XML",
                                "obs": f"Chave {chave} encontrada no WinThor (módulo {nome}). Filial: {d.get('codfilialnf')}."
                            })
                            enviar_supabase(cfg['tbl_confronto'], payload)
                            achou = True
                            tbl_achou = cfg['tbl_confronto']
                            modulos_tentados.append(f"{nome}: ENCONTRADA")
                            
                            # --- COMPARAÇÃO IMEDIATA ---
                            time.sleep(0.5)
                            resp_reg = requests.get(
                                f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?chavecte=eq.{chave}&select=*",
                                headers=HEADERS_GET
                            )
                            if resp_reg.status_code == 200:
                                regs = resp_reg.json()
                                if isinstance(regs, list) and regs:
                                    reg = regs[0]
                                    if reg.get('xml_doc'):
                                        log(f"  XML já presente! Rodando comparação imediata...", "[BUSCA]")
                                        avaliar_xml(cfg['tbl_confronto'], reg)
                                        log(f"  Comparação concluída para {chave[:20]}...", "[BUSCA]")
                            break
                        else:
                            modulos_tentados.append(f"{nome}: não encontrada")
                            
                    if not achou:
                        obs_detalhada = f"Chave {chave} buscada em: {', '.join(modulos_tentados)}. Resultado: NÃO ENCONTRADA no WinThor."
                        for nome, cfg in MODOS_SQL.items():
                            requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?chave_xml=eq.{chave}", headers=HEADERS, json={"status": "Não Importada", "obs": obs_detalhada})

                    requests.delete(f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{id_}", headers=HEADERS)
                    log(f"Chave {chave[:20]}... processada. Achou={achou}", "[BUSCA]")
        except Exception as e:
            log(f"Erro: {e}", "[BUSCA]")
        time.sleep(5)

# ==========================================================
# THREAD 3: VE_RI (Validar Todos OK - marca 'S' no WinThor)
# ==========================================================
def run_ve_ri():
    log("Iniciando rotina.", "[VE_RI]")
    while True:
        try:
            for nome, cfg in MODOS_SQL.items():
                resp = requests.get(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri']}?status=eq.Subindo", headers=HEADERS_GET)
                if resp.status_code != 200: continue
                filas = resp.json()
                if not isinstance(filas, list) or not filas: continue
                
                log(f"({nome}) {len(filas)} registro(s) na fila de verificação.", "[VE_RI]")
                
                binds = ",".join(str(r['numtransent']) for r in filas if r.get('numtransent'))
                if not binds: continue
                
                sql_check = cfg['veri_check'].replace('{binds}', binds)
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(sql_check)
                cols = [c[0].lower() for c in cursor.description]
                wt_data = {str(d['numtransent']): d for d in (dict(zip(cols, row)) for row in cursor.fetchall())}
                
                aprovados = []
                for reg in filas:
                    nt = str(reg.get('numtransent'))
                    v_tot = float(reg.get('vltotal_xml') or 0)
                    v_icms = float(reg.get('vlicms_xml') or 0)
                    if nt not in wt_data:
                        requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri']}?id=eq.{reg['id']}", headers=HEADERS, json={"status": "Erro: Nota removida DB"})
                        continue
                        
                    db = wt_data[nt]
                    if abs(db['vltotal'] - v_tot) > 0.01 or abs(db['vlicms'] - v_icms) > 0.01:
                        requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri']}?id=eq.{reg['id']}", headers=HEADERS, json={"status": "Erro: Valores divergentes"})
                        continue
                        
                    aprovados.append(nt)
                
                if aprovados:
                    binds_upd = ",".join(aprovados)
                    sql_upd = cfg['veri_update'].replace('{binds}', binds_upd)
                    cursor.execute(sql_upd)
                    conn.commit()
                    log(f"({nome}) UPDATE de {len(aprovados)} NFs para Conferido='S'.", "[VE_RI]")
                    
                    for reg in filas:
                        if str(reg.get('numtransent')) in aprovados:
                            payload = {"status": "Totalmente Validada", "validado_por": reg.get("user_nome"), "validado_em": datetime.utcnow().isoformat()+"Z"}
                            requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{reg['confronto_id']}", headers=HEADERS, json=payload)
                            requests.delete(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri']}?id=eq.{reg['id']}", headers=HEADERS)
                            
                conn.close()
        except Exception as e:
            log(f"Erro: {e}", "[VE_RI]")
        time.sleep(5)

# ==========================================================
# THREAD 4: TOMADOR DIFERENTE
# ==========================================================
def run_toma_dif():
    log("Iniciando rotina.", "[TOMA]")
    while True:
        try:
            for nome, cfg in MODOS_SQL.items():
                resp = requests.get(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri_tomador']}?status=eq.Subindo", headers=HEADERS_GET)
                if resp.status_code != 200: continue
                filas = resp.json()
                if not isinstance(filas, list) or not filas: continue
                
                log(f"({nome}) {len(filas)} registro(s) na fila de tomador.", "[TOMA]")
                
                conn = get_db_connection()
                cursor = conn.cursor()
                for reg in filas:
                    cnpj_t = (reg.get('cnpj_tomador') or "").lstrip('0')
                    if cnpj_t.startswith(CNPJ_PREFIX):
                        filial_correta = FILIAIS_MAP.get(cnpj_t)
                        if not filial_correta: continue
                        
                        sql = cfg['toma_check'].replace(':numnota', str(reg['numnota'])).replace(':codfilialnf_correto', str(filial_correta))
                        cursor.execute(sql)
                        row = cursor.fetchone()
                        
                        if row:
                            st = "Totalmente Validada" if row[-1] == 'S' else "OK"
                            payload = {"status": st, "codfilialnf": filial_correta, "codfilial": row[3]}
                            requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{reg['confronto_id']}", headers=HEADERS, json=payload)
                            requests.delete(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri_tomador']}?id=eq.{reg['id']}", headers=HEADERS)
                            log(f"({nome}) Tomador corrigido para filial {filial_correta}.", "[TOMA]")
                    else:
                        pass # Outros casos de exclusao manter "Divergente" e remover da fila
                        requests.delete(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_veri_tomador']}?id=eq.{reg['id']}", headers=HEADERS)
                conn.close()
        except Exception as e:
            log(f"Erro: {e}", "[TOMA]")
        time.sleep(5)

# ==========================================================
# THREAD 5: IMPORTAÇÃO NF CONSUMO
# ==========================================================
def run_import_consumo():
    log("Iniciando rotina de importação NF Consumo.", "[IMPORT_CONSUMO]")
    while True:
        try:
            for nome, cfg in MODOS_SQL.items():
                if nome != 'CONSUMO': continue
                resp = requests.get(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?status=eq.Aguardando Importação", headers=HEADERS_GET)
                if resp.status_code != 200: continue
                filas = resp.json()
                if not isinstance(filas, list) or not filas: continue
                
                log(f"({nome}) {len(filas)} registro(s) para importar.", "[IMPORT_CONSUMO]")
                
                conn = get_db_connection()
                for reg in filas:
                    id_req = reg['id']
                    chave_xml = reg.get('chave_xml')
                    xml_doc = reg.get("xml_doc", "")
                    
                    if not xml_doc or not chave_xml:
                        requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{id_req}", headers=HEADERS, json={"status":"Não Importada", "obs": "Sem XML ou Chave"})
                        continue
                        
                    try:
                        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_doc).replace('cte:', '').replace('nfe:', '')
                        root = ET.fromstring(clean_xml)
                        
                        # Extrair dados do XML
                        nNF_el = root.find('.//ide/nNF')
                        nNF = nNF_el.text if nNF_el is not None else "0"
                        serie_el = root.find('.//ide/serie')
                        serie = serie_el.text if serie_el is not None else "53"
                        dhEmi_el = root.find('.//ide/dhEmi')
                        dhEmi = dhEmi_el.text if dhEmi_el is not None else ""
                        dt_emissao = dhEmi[:10] if len(dhEmi) >= 10 else ""
                        dt_emissao_formatada = datetime.strptime(dt_emissao, "%Y-%m-%d").strftime("%d/%m/%Y") if dt_emissao else ""
                        
                        cnpj_emissor_el = root.find('.//emit/CNPJ')
                        cpf_emissor_el = root.find('.//emit/CPF')
                        cnpj_emissor = cnpj_emissor_el.text if cnpj_emissor_el is not None else (cpf_emissor_el.text if cpf_emissor_el is not None else "00000000000000")
                        tipo_fj = "J" if cnpj_emissor_el is not None else "F"
                        
                        xNome_emit_el = root.find('.//emit/xNome')
                        xNome_emit = xNome_emit_el.text if xNome_emit_el is not None else "DESCONHECIDO"
                        ie_emit_el = root.find('.//emit/IE')
                        ie_emit = ie_emit_el.text if ie_emit_el is not None else ""
                        
                        # Emitente Ender
                        xLgr_emit_el = root.find('.//emit/enderEmit/xLgr')
                        xLgr_emit = xLgr_emit_el.text if xLgr_emit_el is not None else ""
                        nro_emit_el = root.find('.//emit/enderEmit/nro')
                        nro_emit = nro_emit_el.text if nro_emit_el is not None else ""
                        xBairro_emit_el = root.find('.//emit/enderEmit/xBairro')
                        xBairro_emit = xBairro_emit_el.text if xBairro_emit_el is not None else ""
                        cMun_emit_el = root.find('.//emit/enderEmit/cMun')
                        cMun_emit = cMun_emit_el.text if cMun_emit_el is not None else ""
                        xMun_emit_el = root.find('.//emit/enderEmit/xMun')
                        xMun_emit = xMun_emit_el.text if xMun_emit_el is not None else ""
                        uf_emit_el = root.find('.//emit/enderEmit/UF')
                        uf_emit = uf_emit_el.text if uf_emit_el is not None else ""
                        cep_emit_el = root.find('.//emit/enderEmit/CEP')
                        cep_emit = cep_emit_el.text if cep_emit_el is not None else ""
                        
                        # Destinatario
                        cnpj_dest_el = root.find('.//dest/CNPJ')
                        cnpj_dest = cnpj_dest_el.text if cnpj_dest_el is not None else ""
                        uf_dest_el = root.find('.//dest/enderDest/UF')
                        uf_dest = uf_dest_el.text if uf_dest_el is not None else ""
                        cMun_dest_el = root.find('.//dest/enderDest/cMun')
                        cMun_dest = cMun_dest_el.text if cMun_dest_el is not None else ""
                        
                        vNF = root.find('.//vNF')
                        vltotal = float(vNF.text) if vNF is not None else 0.0
                        
                        cursor = conn.cursor()
                        # 0. A chave já existe no pcnfent?
                        cursor.execute("SELECT numtransent FROM pcnfent WHERE chavenfe = :chave", chave=chave_xml)
                        if cursor.fetchone():
                            requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{id_req}", headers=HEADERS, json={"status":"Não Importada", "obs": "A chave já foi encontrada no pcnfent!"})
                            cursor.close()
                            continue
                            
                        # 1. Obter info filial
                        filial_map_key = cnpj_dest.lstrip('0')
                        codfilial = FILIAIS_MAP.get(filial_map_key) or FILIAIS_MAP.get(cnpj_dest)
                        
                        cursor.execute("SELECT CODIGO, RAZAOSOCiAl, ENDERECO, CIDADE, UF, CEP, codfornec, CGC FROM PCFILIAL WHERE codigo = :cod OR cgc = :cgc", cod=codfilial, cgc=cnpj_dest)
                        filial_db = cursor.fetchone()
                        uf_filial = filial_db[4] if filial_db else uf_dest
                        cgc_filial = filial_db[7] if filial_db else cnpj_dest
                        codfor_filial = filial_db[6] if filial_db else None
                        
                        # IEFILIAL - precisa buscar a IE separada se nao estiver na query original ou do config
                        # Assumindo que a rotina vai inserir vazio ou vai buscar do bd
                        ie_filial = ""
                        
                        codfiscal = 199 if uf_emit == uf_filial else 299
                        codfiscal_base = 1556 if uf_emit == uf_filial else 2556
                        
                        # 2. Obter/Criar fornecedor
                        # Tomar cuidado com o '0' na frente do CNPJ
                        cursor.execute("SELECT CODFORNEC FROM PCFORNEC WHERE CGC = :cgc OR CGC = :cgc2", cgc=cnpj_emissor, cgc2=cnpj_emissor.lstrip('0'))
                        fornec_db = cursor.fetchone()
                        if fornec_db:
                            codfornec = fornec_db[0]
                        else:
                            cursor.execute("SELECT proxnumfornec FROM pcconsum FOR UPDATE")
                            codfornec = cursor.fetchone()[0]
                            novo_proxnumfornec = codfornec + 1
                            cursor.execute("UPDATE pcconsum SET proxnumfornec = :val", val=novo_proxnumfornec)
                            
                            sql_ins_forn = cfg.get('sql_import_pcfornec')
                            if sql_ins_forn:
                                cursor.execute(sql_ins_forn, {
                                    'CODFORNEC': codfornec,
                                    'FORNECEDOR': xNome_emit[:60],
                                    'TIPOPESSOA': tipo_fj,
                                    'CGC': cnpj_emissor,
                                    'IE': ie_emit[:20],
                                    'ENDER': xLgr_emit[:80],
                                    'BAIRRO': xBairro_emit[:40],
                                    'CIDADE': xMun_emit[:40],
                                    'ESTADO': uf_emit[:2],
                                    'CEP': cep_emit[:8],
                                    'CODMUNICIPIO': cMun_emit
                                })
                        
                        # 3. Gerar numtransent
                        cursor.execute("SELECT proxnumtransent FROM pcconsum FOR UPDATE")
                        numtransent = cursor.fetchone()[0]
                        novo_proxnumtransent = numtransent + 1
                        cursor.execute("UPDATE pcconsum SET proxnumtransent = :val", val=novo_proxnumtransent)
                        
                        # 4. Insert pcnfent
                        agora = datetime.now()
                        hora_lanc = agora.strftime("%H")
                        min_lanc = agora.strftime("%M")
                        dt_ent = agora.strftime("%d/%m/%Y")
                        
                        sql_ins_pcnfent = cfg.get('sql_import_pcnfent')
                        if sql_ins_pcnfent:
                            cursor.execute(sql_ins_pcnfent, {
                                'ESPECIE': 'NF', 'SERIE': serie, 'NUMNOTA': nNF, 'DTEMISSAO': dt_emissao_formatada,
                                'DTENT': dt_ent, 'CODFORNEC': codfornec, 'VLTOTAL': vltotal, 'CODCONT': 401003,
                                'CODFISCAL': codfiscal, 'CODFILIAL': codfilial, 'TIPODESCARGA': '0', 'NUMTRANSENT': numtransent,
                                'VLIPI': 0, 'VLFRETE': 0, 'VLST': 0, 'VLDESCONTO': 0, 'VLBASEIPI': 0, 'UF': uf_emit,
                                'VLOUTRAS': 0, 'CODFUNCLANC': 1, 'HORALANC': hora_lanc, 'MINUTOLANC': min_lanc,
                                'ROTINALANC': 'ZyNapse', 'FUNCLANC': 'ZyNapse', 'CHAVENFE': chave_xml, 'SITUACAONFE': 0,
                                'EMISSAOPROPRIA': 'N', 'TIPOEMISSAO': 1, 'FORNECEDOR': xNome_emit[:60], 'CGC': cnpj_emissor,
                                'IE': ie_emit[:20], 'TIPOFJ': tipo_fj, 'TIPOFORNEC': 'I', 'CODPAIS': 1058, 'DESCPAIS': 'Brasil',
                                'CGCFILIAL': cgc_filial, 'IEFILIAL': ie_filial, 'UFFILIAL': uf_filial, 'CODFORFILIAL': codfor_filial,
                                'TIPOALIQOUTRASDESP': 'P', 'CODCONTFOR': 100001, 'CODCONTFRE': 100002, 'TIPOFRETECIFFOB': 'C',
                                'REVENDA': 'S', 'UFCODIGO': uf_dest, 'HISTORICO': 'S', 'DTLANCTO': dt_ent, 'ENDERECO': xLgr_emit[:80],
                                'BAIRRO': xBairro_emit[:40], 'MUNICIPIO': xMun_emit[:40], 'CEP': cep_emit[:8], 'CODMUNICIPIO': cMun_emit,
                                'CONSUMIDORFINAL': 'S', 'CODIBGE': cMun_dest, 'SIMPLESNACIONAL': 'N'
                            })
                            
                        # 5. Insert pcnfbase
                        sql_ins_base = cfg.get('sql_import_pcnfbase')
                        if sql_ins_base:
                            cursor.execute(sql_ins_base, {
                                'ALIQUOTA': 0, 'VLBASE': 0, 'VLICMS': 0, 'NUMTRANSENT': numtransent, 'CODCONT': 401003,
                                'CODFISCAL': codfiscal_base, 'TIPO': 1, 'VLISENTAS': 0, 'VLCONTABIL': 0, 'SITTRIBUT': 90
                            })
                            
                        # 6. Insert pcnfentpiscofins
                        sql_ins_piscofins = cfg.get('sql_import_pcnfentpiscofins')
                        if sql_ins_piscofins:
                            cursor.execute(sql_ins_piscofins, {
                                'CODTRIBPISCOFINS': 70, 'VLBASEPIS': 0, 'VLBASECOFINS': 0, 'PERPIS': 0, 'PERCOFINS': 0,
                                'VLCOFINS': 0, 'VLPIS': 0, 'NUMTRANSENT': numtransent, 'CODCONT': 401003
                            })
                        
                        conn.commit()
                        cursor.close()
                        
                        # Atualizar status supabase para varredura
                        requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{id_req}", headers=HEADERS, json={"status":"Sem XML", "obs": "Importada com Sucesso - Aguardando varredura"})
                        
                    except Exception as ex:
                        conn.rollback()
                        requests.patch(f"{SUPABASE_URL}/rest/v1/{cfg['tbl_confronto']}?id=eq.{id_req}", headers=HEADERS, json={"status":"Não Importada", "obs": f"Erro Importação: {str(ex)[:200]}"})
                        
                conn.close()
        except Exception as e:
            log(f"Erro: {e}", "[IMPORT_CONSUMO]")
        time.sleep(5)

# ==========================================================
# INICIAR TUDO
# ==========================================================
def main():
    log("=== BOT MESTRE NUTRIPORT (4 MODULES IN PARALLEL) ===")
    t1 = threading.Thread(target=run_au_conta, daemon=True)
    t2 = threading.Thread(target=run_busca_isolada, daemon=True)
    t3 = threading.Thread(target=run_ve_ri, daemon=True)
    t4 = threading.Thread(target=run_toma_dif, daemon=True)
    t5 = threading.Thread(target=run_import_consumo, daemon=True)

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()

    log("Todas as rotinas conectadas e rodando em plano de fundo!")
    while True:
        time.sleep(60000)

if __name__ == "__main__":
    main()
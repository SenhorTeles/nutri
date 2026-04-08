"""
AUTOMAÇÃO DE CONFRONTO NF CONSUMO - au_conta.py
================================================
Este script roda em loop contínuo e:
1. Busca no WinThor todas as NFs não validadas (conferido='N', especie='NF')
2. Verifica no Supabase (confrontofiscalnfconsumo) quais já existem
3. Para as que NÃO existem no Supabase:
   a) Se já tem XML importado pelo front → confronta valores e determina status
   b) Se NÃO tem XML → envia com status "Sem XML"
4. Para as que JÁ existem com status "Sem XML" e agora têm XML → reavalia

O XML NÃO vem mais do SIEG. O XML é importado pelo FRONT-END.
Quando o front importa o XML, ele salva na tabela confrontofiscalnfconsumo com xml_doc preenchido.
Este script confronta os dados do WinThor com o que já está no Supabase.

SEGURANÇA:
- Logs detalhados de cada passo
"""

import os
import time
import requests
import oracledb
from datetime import datetime

# --- CONFIGURAÇÕES DO BANCO DE DADOS (WINTHOR) ---
CLIENT_LIB_DIR = r"C:\Users\informatica.ti\Documents\appdiscooveryzynapse\cmdintanci\instantclient_21_19"
USUARIO = "MIGRACAO"
SENHA = "fzabu69128XPKGY@!"

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
except Exception as e:
    print(f"Aviso: Não foi possível inicializar Oracle Client: {e}")

DSN = oracledb.makedsn("201.157.211.96", 1521, service_name="CS8NZK_190797_W_high.paas.oracle.com")

# --- CONFIGURAÇÕES SUPABASE ---
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

# --- LISTA DE FILIAIS ---
FILIAIS = [
    {"codigo": "1", "cgc": "3612312000144", "uf": "SP"},
    {"codigo": "11", "cgc": "3612312000144", "uf": "SP"},
    {"codigo": "2", "cgc": "3612312000306", "uf": "RS"},
    {"codigo": "22", "cgc": "3612312000306", "uf": "RS"},
    {"codigo": "3", "cgc": "3612312000497", "uf": "SC"},
    {"codigo": "33", "cgc": "3612312000497", "uf": "SC"},
    {"codigo": "4", "cgc": "3612312000225", "uf": "SP"}
]

TABLE_NAME = "confrontofiscalnfconsumo"


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def buscar_dados_winthor(codfilial, data_inicio, data_fim):
    """Busca NFs não validadas do WinThor para uma filial/período."""
    log(f"Buscando NFs da filial {codfilial} no período {data_inicio} até {data_fim}...")

    sql = f"""
SELECT   n.codfilialnf,
         n.codfilial,
         n.modelo,
         n.serie,
         n.especie,
         n.numnota,
         n.numtransent,
         n.vltotal, 
         n.codfornec,
         f.fornecedor, 
         f.cgc,
         n.chavenfe,
         n.chavecte,
         n.dtemissao,
         n.conferido,
         n.dtent,
         NVL(b.vlicms, 0) AS vlicms
  FROM   pcnfent n , pcfornec f  ,  pcnfbaseent b
 WHERE n.codfornec = f.codfornec(+)
   AND b.numtransent = n.numtransent
   AND n.codfilialnf = '{codfilial}'
   AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY')
   AND n.especie = 'NF'
   AND b.especie = 'NF'
   AND n.conferido = 'N'
   AND n.dtcancel is null
 ORDER BY n.codfilial , n.numnota
    """
    notas_dict = {}
    try:
        connection = oracledb.connect(user=USUARIO, password=SENHA, dsn=DSN)
        cursor = connection.cursor()
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]

        for row in cursor.fetchall():
            dado = dict(zip(columns, row))
            # Para NF, a chave fica em chavenfe (chavecte é None para NF)
            chave = str(dado.get('chavenfe') or dado.get('chavecte') or '').strip()
            if chave and chave != 'None':
                notas_dict[chave] = dado

        connection.close()
        log(f"[{len(notas_dict)}] NFs encontradas no banco para o período.")
    except Exception as e:
        log(f"Erro ao buscar no Winthor: {e}")
    return notas_dict


def enviar_supabase(data_payload):
    """Upsert um registro na tabela confrontofiscalnfconsumo."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=chave_xml"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    try:
        response = requests.post(url, headers=headers, json=data_payload)
        if response.status_code in [200, 201]:
            log(f"  [>] Upsert CHAVE: {data_payload.get('chavecte')} -> Supabase ({data_payload.get('status')}).")
        else:
            log(f"  [!] Erro ao enviar para Supabase: {response.status_code} - {response.text}")
    except Exception as e:
        log(f"Erro de conexão com Supabase: {e}")


def upsert_supabase(data_payload):
    """Upsert um registro na tabela confrontofiscalnfconsumo (INSERT ou UPDATE por chavecte)."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=chavecte"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    try:
        response = requests.post(url, headers=headers, json=data_payload)
        if response.status_code in [200, 201]:
            log(f"  [>] Upsert CHAVE: {data_payload.get('chavecte')} -> ({data_payload.get('status')})")
        else:
            log(f"  [!] Erro upsert: {response.status_code} - {response.text}")
    except Exception as e:
        log(f"Erro de conexão com Supabase (upsert): {e}")


def buscar_chaves_existentes_supabase(codfilial, data_inicio, data_fim):
    """Busca todas as chaves já existentes no Supabase para esta filial neste período."""
    existentes = {}  # {chavecte: {status, tem_xml}}
    dt_inicio_fs = datetime.strptime(data_inicio, '%d/%m/%Y').strftime('%Y-%m-%d')
    dt_fim_fs = datetime.strptime(data_fim, '%d/%m/%Y').strftime('%Y-%m-%d')

    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?select=chavecte,status,xml_doc&codfilial=eq.{codfilial}&dtent=gte.{dt_inicio_fs}&dtent=lte.{dt_fim_fs}"

    try:
        offset = 0
        limit = 1000
        while True:
            hdrs = HEADERS_GET.copy()
            hdrs["Range-Unit"] = "items"
            hdrs["Range"] = f"{offset}-{offset+limit-1}"

            resp = requests.get(url, headers=hdrs)
            if resp.status_code in [200, 206]:
                data = resp.json()
                if not data:
                    break
                for d in data:
                    if d.get("chavecte"):
                        tem_xml = bool(d.get("xml_doc") and d["xml_doc"].strip())
                        existentes[d["chavecte"]] = {
                            "status": d.get("status", ""),
                            "tem_xml": tem_xml
                        }
                if len(data) < limit:
                    break
                offset += limit
            else:
                log(f"Erro ao consultar chaves existentes no Supabase: {resp.status_code} - {resp.text}")
                break
    except Exception as e:
        log(f"Erro na conexão com Supabase (buscar chaves): {e}")

    return existentes


def buscar_registros_sem_xml_com_xml_agora():
    """Busca registros com status 'Sem XML' que agora possuem xml_doc preenchido (importado pelo front)."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?status=eq.Sem XML&xml_doc=neq.&select=id,chavecte,codfilial,codfilialnf,vltotal,vlicms,xml_doc,cnpj_tomador,cnpj_filial"
    
    registros = []
    try:
        resp = requests.get(url, headers=HEADERS_GET)
        if resp.status_code == 200:
            registros = resp.json()
    except Exception as e:
        log(f"Erro ao buscar registros Sem XML: {e}")
    
    return registros


def avaliar_xml_importado(registro):
    """
    Avalia um registro que tinha status 'Sem XML' e agora tem XML importado pelo front.
    Compara valores do WinThor (vltotal, vlicms) com os do XML (vltotal_xml, vlicms_xml).
    Atualiza o status no Supabase.
    """
    import xml.etree.ElementTree as ET
    import re
    
    registro_id = registro.get("id")
    chavecte = registro.get("chavecte")
    xml_doc = registro.get("xml_doc", "")
    vltotal_db = float(registro.get("vltotal") or 0)
    vlicms_db = float(registro.get("vlicms") or 0)
    codfilial = str(registro.get("codfilial") or "")
    codfilialnf = str(registro.get("codfilialnf") or "")
    cnpj_filial = registro.get("cnpj_filial") or ""
    
    if not xml_doc or not xml_doc.strip():
        return
    
    try:
        # Parse XML para extrair valores
        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_doc)
        clean_xml = clean_xml.replace('cte:', '')
        root = ET.fromstring(clean_xml)
        
        # Valor total da prestação
        vTPrest_elem = root.find('.//vTPrest')
        vltotal_xml = float(vTPrest_elem.text) if vTPrest_elem is not None else 0.0
        
        # ICMS
        vlicms_xml = 0.0
        for tag in ['ICMS00', 'ICMSOutraUF', 'ICMS20', 'ICMS45', 'ICMS60', 'ICMS90', 'ICMSSN']:
            node = root.find(f'.//{tag}')
            if node is not None:
                vicms = node.find('vICMS') or node.find('vICMSOutraUF')
                if vicms is not None and vicms.text:
                    vlicms_xml = float(vicms.text)
                    break
        
        # CNPJ Tomador
        toma_val = None
        for tag_path in ['.//toma3/toma', './/toma0/toma', './/toma4/toma', './/toma']:
            elem = root.find(tag_path)
            if elem is not None and elem.text:
                toma_val = elem.text.strip()
                break
        
        cnpj_tomador = ""
        toma_map = {'0': './/rem/CNPJ', '1': './/exped/CNPJ', '2': './/receb/CNPJ', '3': './/dest/CNPJ', '4': './/toma4/CNPJ'}
        if toma_val in toma_map:
            elem = root.find(toma_map[toma_val])
            cnpj_tomador = elem.text if elem is not None else ""
        
        # CNPJ Remetente
        rem_elem = root.find('.//rem/CNPJ')
        cnpj_remetente = rem_elem.text if rem_elem is not None else ""
        
        # Determinar filial CGC
        filial_obj = next((f for f in FILIAIS if str(f['codigo']) == codfilialnf), None)
        cnpj_filial_esperado = filial_obj['cgc'] if filial_obj else cnpj_filial
        
        # Comparações
        diff_tomador = bool(cnpj_tomador and cnpj_filial_esperado and (cnpj_tomador != cnpj_filial_esperado))
        diff_tot = abs(vltotal_db - vltotal_xml) > 0.01
        diff_icms = abs(vlicms_db - vlicms_xml) > 0.01
        
        if diff_tomador:
            status = "Tomador Divergente"
        elif diff_tot or diff_icms:
            status = "DIVERGENTE"
        else:
            status = "OK"
        
        obs = []
        if diff_tot:
            obs.append(f"TOTAL (DB: {vltotal_db} | XML: {vltotal_xml})")
        if diff_icms:
            obs.append(f"ICMS (DB: {vlicms_db} | XML: {vlicms_xml})")
        if diff_tomador:
            obs.append(f"Tomador ({cnpj_tomador} != {cnpj_filial_esperado})")
        obs_str = " | ".join(obs)
        
        # Atualizar no Supabase
        patch_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=eq.{registro_id}"
        payload = {
            "status": status,
            "vltotal_xml": vltotal_xml,
            "vlicms_xml": vlicms_xml,
            "cnpj_tomador": cnpj_tomador,
            "cnpj_remetente": cnpj_remetente,
            "obs": obs_str,
            "is_tomador_dif": diff_tomador
        }
        response = requests.patch(patch_url, headers=HEADERS, json=payload)
        if response.status_code in [200, 204]:
            log(f"  [XML OK] {chavecte} -> {status}")
        else:
            log(f"  [ERRO] Falha ao atualizar {chavecte}: {response.status_code}")
        
    except Exception as e:
        log(f"  [ERRO] Falha ao avaliar XML de {chavecte}: {e}")


def processar_filial(filial, data_inicio, data_fim):
    """Processa uma filial: busca NFs do WinThor e envia ao Supabase."""
    log(f"\n>>> Processando Filial: {filial['codigo']} ({filial['uf']}) | Período: {data_inicio} até {data_fim}")

    notas_periodo = buscar_dados_winthor(filial["codigo"], data_inicio, data_fim)
    if not notas_periodo:
        log("Sem notas para este período nesta filial.")
        return

    chaves_all = list(notas_periodo.keys())

    log(f"Verificando {len(chaves_all)} notas no Supabase...")
    chaves_existentes = buscar_chaves_existentes_supabase(filial["codigo"], data_inicio, data_fim)
    if chaves_existentes:
        log(f"[{len(chaves_existentes)}] notas já existem no Supabase.")

    # Filtrar apenas as que NÃO existem ainda
    chaves_novas = [c for c in chaves_all if c not in chaves_existentes]

    if not chaves_novas:
        log("Todas as notas deste período já estão no Supabase.")
        return

    log(f"Enviando {len(chaves_novas)} notas novas ao Supabase com status 'Sem XML'...")

    for chave in chaves_novas:
        dados_banco = notas_periodo[chave]
        vl_tot_bd = float(dados_banco.get('vltotal') or 0.0)
        vl_icms_bd = float(dados_banco.get('vlicms') or 0.0)

        d_em = dados_banco.get('dtemissao')
        str_em = d_em.strftime('%Y-%m-%d') if hasattr(d_em, 'strftime') else d_em
        d_ent = dados_banco.get('dtent')
        str_ent = d_ent.strftime('%Y-%m-%d') if hasattr(d_ent, 'strftime') else d_ent

        payload = {
            "codfilialnf": dados_banco.get('codfilialnf'),
            "codfilial": dados_banco.get('codfilial'),
            "modelo": dados_banco.get('modelo'),
            "serie": dados_banco.get('serie'),
            "especie": dados_banco.get('especie'),
            "numnota": dados_banco.get('numnota'),
            "numtransent": dados_banco.get('numtransent'),
            "vltotal": vl_tot_bd,
            "vlicms": vl_icms_bd,
            "vltotal_xml": 0.0,
            "vlicms_xml": 0.0,
            "codfornec": dados_banco.get('codfornec'),
            "fornecedor": dados_banco.get('fornecedor'),
            "cgc": dados_banco.get('cgc'),
            "chavenfe": dados_banco.get('chavenfe'),
            "chavecte": chave,
            "chave_winthor": chave,
            "chave_xml": chave,
            "cnpj_remetente": "",
            "cnpj_filial": filial['cgc'],
            "dtemissao": str_em,
            "dtent": str_ent,
            "xml_doc": "",
            "status": "Sem XML",
            "obs": "Aguardando importação de XML pelo front-end.",
        }
        enviar_supabase(payload)

    log(f"Período {data_inicio} a {data_fim} da filial {filial['codigo']} concluído!")


def reavaliar_sem_xml():
    """
    Busca registros com status 'Sem XML' que agora têm XML importado pelo front.
    Quando o front importa o XML, o xml_doc é preenchido.
    Este método reavalia esses registros.
    """
    log("Verificando registros 'Sem XML' que agora possuem XML importado...")
    registros = buscar_registros_sem_xml_com_xml_agora()
    
    if not registros:
        return
    
    log(f"Encontrados {len(registros)} registros para reavaliar.")
    for reg in registros:
        avaliar_xml_importado(reg)


def main():
    log("=== INICIANDO CONFRONTO NF CONSUMO AUTOMÁTICO ===")
    log("Modo: XML importado pelo Front-End (sem SIEG)")

    agora = datetime.now()
    data_atual_str = agora.strftime("%d/%m/%Y")
    data_inicio_geral = "01/01/2026"

    # 1. Varredura Global Inicial
    log(f"\n---> INICIANDO VARREDURA GERAL ({data_inicio_geral} até {data_atual_str}) <---")
    for filial in FILIAIS:
        try:
            reavaliar_sem_xml()  # Reavalia registros que receberam XML pelo front
            processar_filial(filial, data_inicio_geral, data_atual_str)
        except Exception as e:
            log(f"ERRO CRÍTICO NA VARREDURA GERAL (Filial {filial['codigo']}): {e}")

    log("\n---> VARREDURA GERAL CONCLUÍDA <---")

    ultima_varredura = time.time()

    log("Iniciando modo contínuo (Monitoramento de XMLs importados + Varredura periódica).")

    # 2. Ciclo Contínuo
    while True:
        try:
            # 1. Reavaliar registros "Sem XML" que receberam XML pelo front
            reavaliar_sem_xml()

            # 2. A cada 2 horas, refaz varredura para ver novas notas
            agora_ts = time.time()
            if (agora_ts - ultima_varredura) > (2 * 60 * 60):
                agora = datetime.now()
                primeiro_dia_mes = f"01/{agora.month:02d}/{agora.year}"
                data_atual_str = agora.strftime("%d/%m/%Y")

                log(f"\n---> INICIANDO CICLO MENSAL ({primeiro_dia_mes} até {data_atual_str}) <---")
                for filial in FILIAIS:
                    reavaliar_sem_xml()
                    processar_filial(filial, primeiro_dia_mes, data_atual_str)

                ultima_varredura = time.time()
                log(f"---> CICLO MENSAL FINALIZADO. Próximo em 2 horas. <---")

        except Exception as e:
            log(f"ERRO CRÍTICO NO MODO CONTÍNUO: {e}")

        # Espera curta para manter responsividade
        time.sleep(30)


if __name__ == "__main__":
    main()
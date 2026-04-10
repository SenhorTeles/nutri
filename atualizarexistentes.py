"""
ATUALIZAR EXISTENTES - atualizarexistentes.py
==============================================
Script ONE-SHOT para atualizar registros já existentes na tabela confrontofiscal
com os campos cnpj_tomador e status 'Tomador Divergente'.

Ele faz exatamente o que o frontend fazia (getCnpjTomador no JS),
mas direto via API REST do Supabase, sem precisar do WinThor.

USO: python atualizarexistentes.py
"""

import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# --- CONFIGURAÇÕES SUPABASE (service_role key) ---
SUPABASE_URL = "https://hzljpmjhmrgwjjskubii.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh6bGpwbWpobXJnd2pqc2t1YmlpIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDAzNTExMCwiZXhwIjoyMDg5NjExMTEwfQ.fbJw8FyRxSFD45esE-y98tMQ93-6uc4yVID20EYiBz8"

HEADERS_GET = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json"
}

HEADERS_PATCH = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

TABLE = "confrontofiscal"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_cnpj_tomador(xml_doc):
    """
    Extrai o CNPJ do tomador do XML de CT-e.
    Replica a lógica do getCnpjTomador do frontend JS.
    """
    if not xml_doc:
        return ""
    try:
        # Limpar TODOS os namespaces (incluindo prefixados como xmlns:cte)
        clean_xml = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_doc)
        clean_xml = clean_xml.replace('cte:', '')

        root = ET.fromstring(clean_xml)

        # Achar o valor de <toma> (dentro de toma0, toma3 ou toma4)
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
    except Exception:
        pass
    return ""


def buscar_registros_paginado():
    """
    Busca TODOS os registros do confrontofiscal que possuem xml_doc,
    usando paginação com header Range para não estourar limites.
    Retorna lista de dicts com id, xml_doc, cnpj_filial, status.
    """
    todos = []
    offset = 0
    limit = 500
    
    # Buscar TODOS os registros que têm XML (independente de validação)
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?select=id,xml_doc,cnpj_filial,cnpj_tomador,status&xml_doc=neq."
    
    while True:
        hdrs = HEADERS_GET.copy()
        hdrs["Range-Unit"] = "items"
        hdrs["Range"] = f"{offset}-{offset + limit - 1}"
        
        resp = requests.get(url, headers=hdrs, timeout=30)
        
        if resp.status_code in [200, 206]:
            data = resp.json()
            if not data:
                break
            todos.extend(data)
            log(f"  Buscou {len(data)} registros (total acumulado: {len(todos)})")
            
            if len(data) < limit:
                break
            offset += limit
        elif resp.status_code == 416:
            # Range not satisfiable = acabou
            break
        else:
            log(f"  ERRO ao buscar: {resp.status_code} - {resp.text}")
            break
    
    return todos


def atualizar_registro(reg_id, cnpj_tomador, novo_status, max_retries=3):
    """Atualiza um registro no Supabase com cnpj_tomador e status. Tenta várias vezes em caso de erro de rede."""
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?id=eq.{reg_id}"
    payload = {
        "cnpj_tomador": cnpj_tomador,
        "status": novo_status
    }
    for attempt in range(max_retries):
        try:
            resp = requests.patch(url, headers=HEADERS_PATCH, json=payload, timeout=15)
            if resp.status_code in [200, 204]:
                return True
        except requests.exceptions.RequestException as e:
            time.sleep(2)  # Backoff
    return False


def main():
    log("=" * 60)
    log("ATUALIZADOR DE REGISTROS EXISTENTES")
    log("Calculando cnpj_tomador e status 'Tomador Divergente'")
    log("=" * 60)
    
    # 1. Buscar todos os registros com XML
    log("\n[1/3] Buscando registros com xml_doc no Supabase...")
    registros = buscar_registros_paginado()
    log(f"  Total: {len(registros)} registros com XML encontrados.\n")
    
    if not registros:
        log("Nenhum registro para processar. Finalizando.")
        return
    
    # 2. Processar cada registro
    log("[2/3] Processando XMLs e identificando tomador divergente...\n")
    
    total = len(registros)
    atualizados = 0
    tomador_dif_count = 0
    erros = 0
    sem_tomador = 0
    
    for i, reg in enumerate(registros, 1):
        reg_id = reg["id"]
        xml_doc = reg.get("xml_doc") or ""
        cnpj_filial = reg.get("cnpj_filial") or ""
        status_atual = reg.get("status") or ""
        
        # Extrair CNPJ do tomador do XML
        cnpj_tomador = get_cnpj_tomador(xml_doc)
        
        if not cnpj_tomador:
            sem_tomador += 1
        
        # Verificar se é tomador divergente
        is_dif = bool(cnpj_tomador and cnpj_filial and (cnpj_tomador != cnpj_filial))
        
        # Determinar novo status
        if is_dif:
            novo_status = "Tomador Divergente"
        else:
            # Manter o status atual se não é tomador divergente
            novo_status = status_atual
        
        ok = atualizar_registro(reg_id, cnpj_tomador, novo_status)
        
        if ok:
            atualizados += 1
            if is_dif:
                tomador_dif_count += 1
        else:
            erros += 1
        
        # Log de progresso a cada 100
        if i % 100 == 0 or i == total:
            log(f"  Progresso: {i}/{total} | Atualizados: {atualizados} | Tomador Dif: {tomador_dif_count} | Erros: {erros}")
    
    # 3. Resumo final
    log(f"\n[3/3] FINALIZADO!")
    log("=" * 60)
    log(f"  Total processados:      {total}")
    log(f"  Atualizados:            {atualizados}")
    log(f"  Tomador Divergente:     {tomador_dif_count}")
    log(f"  Sem CNPJ tomador no XML:{sem_tomador}")
    log(f"  Erros:                  {erros}")
    log("=" * 60)


if __name__ == "__main__":
    main()

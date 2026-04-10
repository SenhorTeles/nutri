import os
import time
import zipfile
import glob
import json
import requests
import xml.etree.ElementTree as ET
import oracledb
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURAÇÕES DO BANCO DE DADOS (WINTHOR) ---
CLIENT_LIB_DIR = r"C:\Users\claudeyr.sousa\Documents\instantclient-basic-windows.x64-21.19.0.0.0dbru\instantclient_21_19"
USUARIO = "CONSULTA"
SENHA = "CONPHPCMV"

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
except Exception as e:
    pass

DSN = oracledb.makedsn("192.168.8.199", 1521, service_name="WINT")

# --- CONFIGURAÇÕES SUPABASE ---
SUPABASE_URL = "https://pdtxpsdbjrcrmockdfgw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBkdHhwc2RianJjcm1vY2tkZmd3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg1MzY4MywiZXhwIjoyMDg5NDI5NjgzfQ.49jlPkuR0kd36tb8CtlEYAoHAGfBBiRwx-QvWbw54i8"

# --- LISTA DE FILIAIS ---
FILIAIS = [
    {"codigo": "8", "cgc": "36899766000105", "uf": "GO", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000105"},
    {"codigo": "18", "cgc": "36899766000369", "uf": "MT", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000369"},
    {"codigo": "10", "cgc": "36899766000288", "uf": "SC", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000288"},
    {"codigo": "22", "cgc": "36899766000440", "uf": "SP", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000440"},
    {"codigo": "23", "cgc": "36899766000520", "uf": "SP", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000520"},
    {"codigo": "24", "cgc": "36899766000601", "uf": "MG", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000601"},
    {"codigo": "25", "cgc": "36899766000792", "uf": "MG", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000792"},
    {"codigo": "38", "cgc": "36899766001179", "uf": "RJ", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001179"},
    {"codigo": "41", "cgc": "36899766001411", "uf": "RS", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001411"},
    {"codigo": "39", "cgc": "36899766001250", "uf": "BA", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001250"},
    {"codigo": "40", "cgc": "36899766001330", "uf": "CE", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001330"},
    {"codigo": "42", "cgc": "36899766001500", "uf": "PI", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001500"},
    {"codigo": "35", "cgc": "36899766000873", "uf": "PR", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000873"},
    {"codigo": "36", "cgc": "36899766000954", "uf": "MS", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766000954"},
    {"codigo": "37", "cgc": "36899766001098", "uf": "PE", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001098"},
    {"codigo": "43", "cgc": "36899766001683", "uf": "MA", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001683"},
    {"codigo": "44", "cgc": "36899766001764", "uf": "PA", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001764"},
    {"codigo": "51", "cgc": "36899766002221", "uf": "SP", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766002221"},
    {"codigo": "48", "cgc": "36899766002060", "uf": "AL", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766002060"},
    {"codigo": "46", "cgc": "36899766001845", "uf": "GO", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001845"},
    {"codigo": "47", "cgc": "36899766001926", "uf": "RO", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766001926"},
    {"codigo": "49", "cgc": "36899766002140", "uf": "GO", "link": "https://hub.sieg.com/detalhes-do-cliente?id=19449-36899766002140"}
]

def carregar_status():
    default_status = {
        "filial_idx": 0,
        "ano": 2026,
        "mes": 1,
        "chunk_idx": 0
    }
    url = f"{SUPABASE_URL}/rest/v1/automacao_sieg_estado?id=eq.1"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json"
    }
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if data and len(data) > 0:
                row = data[0]
                return {
                    "filial_idx": row.get("filial_idx", default_status["filial_idx"]),
                    "ano": row.get("ano", default_status["ano"]),
                    "mes": row.get("mes", default_status["mes"]),
                    "chunk_idx": row.get("chunk_idx", default_status["chunk_idx"])
                }
    except Exception as e:
        print(f"Erro ao carregar status do Supabase: {e}. Usando padrão.")
    return default_status

def salvar_status(status):
    url = f"{SUPABASE_URL}/rest/v1/automacao_sieg_estado?id=eq.1"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    payload = {
        "filial_idx": status.get("filial_idx"),
        "ano": status.get("ano"),
        "mes": status.get("mes"),
        "chunk_idx": status.get("chunk_idx"),
        "updated_at": datetime.now().isoformat()
    }
    try:
        resp = requests.patch(url, headers=headers, json=payload)
        if resp.status_code not in [200, 204]:
            print(f"Erro ao salvar status no Supabase: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Erro de conexão ao salvar status: {e}")

def buscar_dados_winthor(codfilial, ano, mes):
    print(f"Conectando ao Winthor para buscar as notas da filial {codfilial} no período {mes}/{ano}...")
    
    # Calcular primeiro e último dia do mês
    data_inicio = f"01/{mes:02d}/{ano}"
    if mes == 12:
        data_fim = f"31/12/{ano}"
    else:
        import calendar
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        data_fim = f"{ultimo_dia:02d}/{mes:02d}/{ano}"

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
           AND n.codfilial = '{codfilial}'
           AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY')
           AND n.especie = 'CT'
           AND b.especie = 'CT'
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
            chave = str(dado['chavecte']).strip()
            if chave and chave != 'None':
                notas_dict[chave] = dado
                
        connection.close()
        print(f"[{len(notas_dict)}] notas CT-e encontradas no banco para o período.")
    except Exception as e:
        print(f"Erro ao buscar no Winthor: {e}")
    return notas_dict

def baixar_xmls_sieg(chaves_list, link_cliente):
    if not chaves_list:
        print("Nenhuma chave para buscar.")
        return None
        
    download_dir = os.path.abspath(os.path.join(os.getcwd(), 'downloads_sieg'))
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    # Limpar a pasta antes do novo download
    for f in glob.glob(os.path.join(download_dir, '*')):
        try: os.remove(f)
        except: pass
        
    print(f"Iniciando download de {len(chaves_list)} XMLs no Sieg...")
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    prefs = {"download.default_directory": download_dir, "download.prompt_for_download": False}
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 30)
    
    try:
        driver.get("https://hub.sieg.com/")
        time.sleep(2)
        
        # Aceitar Cookies
        try:
            btn_cookies = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'cc-ALLOW') or text()='Aceitar Cookies']")))
            driver.execute_script("arguments[0].click();", btn_cookies)
        except: pass
            
        print("Fazendo login...")
        email_input = wait.until(EC.presence_of_element_located((By.ID, "txtEmail")))
        email_input.send_keys("sollar1@somacontabilidades.com.br")
        driver.find_element(By.ID, "txtPassword").send_keys("102030@Af")
        btn_submit = driver.find_element(By.ID, "btnSubmit")
        driver.execute_script("arguments[0].click();", btn_submit)
        
        time.sleep(5) 
        
        print(f"Navegando para: {link_cliente}")
        driver.get(link_cliente)
        time.sleep(3)
        
        print("Iniciando processo de download no Dashboard...")
        xpath_btn_cte = "//a[contains(@class, 'btn-download-dashboard') and contains(@onclick, 'cte')]"
        btn_download_cte = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn_cte)))
        driver.execute_script("arguments[0].click();", btn_download_cte)
        
        time.sleep(2)
        aba_chaves = wait.until(EC.element_to_be_clickable((By.ID, "keys-tab")))
        driver.execute_script("arguments[0].click();", aba_chaves)
        
        print("Inserindo chaves...")
        texto_chaves = "\n".join(chaves_list)
        txt_area = wait.until(EC.presence_of_element_located((By.ID, "MainContent_cphMainContent_menudashboard_ClientConsume_DownloadXml_txtKeys")))
        driver.execute_script("arguments[0].value = arguments[1];", txt_area, texto_chaves)
        
        time.sleep(1)
        btn_confirmar = driver.find_element(By.ID, "MainContent_cphMainContent_menudashboard_ClientConsume_DownloadXml_btnDownload")
        driver.execute_script("arguments[0].click();", btn_confirmar)
        
        print("Aguardando download do ZIP...")
        zip_file_path = None
        for _ in range(120): # Até 2 minutos
            arquivos = glob.glob(os.path.join(download_dir, '*.zip'))
            if arquivos and not glob.glob(os.path.join(download_dir, '*.crdownload')):
                zip_file_path = arquivos[0]
                break
            time.sleep(1)
            
        return zip_file_path
            
    except Exception as e:
        print(f"Erro na automação Sieg: {e}")
        return None
    finally:
        try: driver.quit()
        except: pass

def extrair_e_parsear_xmls(zip_path):
    print("Extraindo e lendo os arquivos XML do ZIP...")
    xml_data_dict = {}
    
    extract_dir = os.path.join(os.path.dirname(zip_path), "extraidos")
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
        
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    for file_name in os.listdir(extract_dir):
        if file_name.endswith('.xml'):
            file_path = os.path.join(extract_dir, file_name)
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                ns = {'ns': 'http://www.portalfiscal.inf.br/cte'}
                
                infCte = root.find('.//ns:infCte', ns)
                if infCte is None: continue
                
                chave_xml = infCte.attrib.get('Id', '').replace('CTe', '')
                
                vTPrest_elem = root.find('.//ns:vTPrest', ns)
                vTPrest = float(vTPrest_elem.text) if vTPrest_elem is not None else 0.0
                
                # Buscar ICMS em todos os grupos possíveis do XML
                vICMS = 0.0
                icms_paths = [
                    './/ns:ICMS00/ns:vICMS',
                    './/ns:ICMSOutraUF/ns:vICMSOutraUF',
                    './/ns:ICMS20/ns:vICMS',
                    './/ns:ICMS45/ns:vICMS',
                    './/ns:ICMS60/ns:vICMS',
                    './/ns:ICMS90/ns:vICMS',
                    './/ns:ICMSSN/ns:vICMS',
                ]
                for path in icms_paths:
                    elem = root.find(path, ns)
                    if elem is not None and elem.text:
                        vICMS = float(elem.text)
                        break
                
                rem_cnpj_elem = root.find('.//ns:rem/ns:CNPJ', ns)
                cnpj_remetente = rem_cnpj_elem.text if rem_cnpj_elem is not None else ""
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    xml_content = f.read()
                    
                xml_data_dict[chave_xml] = {
                    'chave_xml': chave_xml,
                    'vTPrest': vTPrest,
                    'vICMS': vICMS,
                    'cnpj_remetente': cnpj_remetente,
                    'xml_content': xml_content
                }
            except Exception as e:
                print(f"Erro lendo xml {file_name}: {e}")
                
    # Limpeza: Remover pasta de extraídos e o arquivo ZIP original
    try:
        shutil.rmtree(extract_dir)
        if os.path.exists(zip_path):
            os.remove(zip_path)
    except Exception as e:
        print(f"Erro ao limpar arquivos temporários: {e}")
        
    return xml_data_dict

def enviar_supabase(data_payload):
    url = f"{SUPABASE_URL}/rest/v1/confrontofiscal"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    try:
        response = requests.post(url, headers=headers, json=data_payload)
        if response.status_code in [200, 201]:
            print(f"  [>] Enviado CHAVE: {data_payload.get('chavecte')} para Supabase ({data_payload.get('status')}).")
        else:
            print(f"  [!] Erro ao enviar para Supabase: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Erro de conexão com Supabase: {e}")

def proximo_periodo(ano, mes):
    if mes == 12:
        return ano + 1, 1
    else:
        return ano, mes + 1

def main():
    print("=== INICIANDO CONFRONTO FISCAL AUTOMÁTICO (ROBUSTO 24/7) ===")
    
    while True:
        try:
            status = carregar_status()
            f_idx = status["filial_idx"]
            ano = status["ano"]
            mes = status["mes"]
            chunk_idx = status["chunk_idx"]
            
            if f_idx >= len(FILIAIS):
                print("Todas as filiais processadas. Reiniciando ciclo...")
                status["filial_idx"] = 0
                salvar_status(status)
                continue
                
            filial = FILIAIS[f_idx]
            print(f"\n>>> Processando Filial: {filial['codigo']} ({filial['uf']}) | Período: {mes}/{ano}")
            
            # 1. Buscar notas no WinThor
            notas_periodo = buscar_dados_winthor(filial["codigo"], ano, mes)
            
            if not notas_periodo:
                print("Sem notas para este período.")
                agora = datetime.now()
                if ano < agora.year or (ano == agora.year and mes < agora.month):
                    print("Avançando para o próximo mês...")
                    novo_ano, novo_mes = proximo_periodo(ano, mes)
                    status["ano"], status["mes"] = novo_ano, novo_mes
                    status["chunk_idx"] = 0
                    salvar_status(status)
                else:
                    print("Mês atual processado. Avançando para próxima filial (ou aguardando novas notas)...")
                    status["filial_idx"] += 1
                    status["chunk_idx"] = 0
                    salvar_status(status)
                    time.sleep(60) # Pausa curta antes de ir para a próxima ou repetir
                continue
            
            # 2. Chunking (30 por vez)
            chaves_all = list(notas_periodo.keys())
            chunks = [chaves_all[i:i + 30] for i in range(0, len(chaves_all), 30)]
            
            print(f"Total de {len(chaves_all)} notas divididas em {len(chunks)} lotes.")
            
            for i in range(chunk_idx, len(chunks)):
                print(f"\n[Lote {i+1}/{len(chunks)}] Iniciando...")
                lote_chaves = chunks[i]
                
                # Download Sieg
                zip_path = baixar_xmls_sieg(lote_chaves, filial["link"])
                
                if zip_path:
                    # Parsear XMLs
                    dados_sieg_xml = extrair_e_parsear_xmls(zip_path)
                    
                    # Confrontar e Enviar
                    divergencias_count = 0
                    for chave in lote_chaves:
                        if chave in dados_sieg_xml:
                            dados_banco = notas_periodo[chave]
                            xml_info = dados_sieg_xml[chave]
                            
                            vl_tot_bd = float(dados_banco.get('vltotal') or 0.0)
                            vl_icms_bd = float(dados_banco.get('vlicms') or 0.0)
                            vl_tot_xml = float(xml_info.get('vTPrest', 0.0))
                            vl_icms_xml = float(xml_info.get('vICMS', 0.0))
                            
                            diff_tot = abs(vl_tot_bd - vl_tot_xml) > 0.01
                            diff_icms = abs(vl_icms_bd - vl_icms_xml) > 0.01
                            
                            status_label = "DIVERGENTE" if (diff_tot or diff_icms) else "OK"
                            obs = []
                            if diff_tot: obs.append(f"TOTAL (DB: {vl_tot_bd} | XML: {vl_tot_xml})")
                            if diff_icms: obs.append(f"ICMS (DB: {vl_icms_bd} | XML: {vl_icms_xml})")
                            obs_str = " | ".join(obs)
                            
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
                                "vltotal_xml": vl_tot_xml,
                                "vlicms_xml": vl_icms_xml,
                                "codfornec": dados_banco.get('codfornec'),
                                "fornecedor": dados_banco.get('fornecedor'),
                                "cgc": dados_banco.get('cgc'),
                                "chavenfe": dados_banco.get('chavenfe'),
                                "chavecte": chave,
                                "chave_winthor": chave,
                                "chave_xml": xml_info.get('chave_xml'),
                                "cnpj_remetente": xml_info.get('cnpj_remetente'),
                                "cnpj_filial": filial['cgc'],
                                "dtemissao": str_em,
                                "dtent": str_ent,
                                "xml_doc": xml_info.get('xml_content'),
                                "status": status_label,
                                "obs": obs_str,
                            }
                            enviar_supabase(payload)
                            divergencias_count += (1 if status_label == "DIVERGENTE" else 0)
                    
                    print(f"Lote {i+1} finalizado. Divergências: {divergencias_count}")
                    
                    # Atualizar status
                    status["chunk_idx"] = i + 1
                    salvar_status(status)
                else:
                    print(f"Falha no download do lote {i+1}. Tentando novamente em 30s...")
                    time.sleep(30)
                    break # Sai do loop de chunks para reiniciar o processo deste lote
            else:
                # Se terminou todos os chunks sem o 'break'
                print(f"Mês {mes}/{ano} da filial {filial['codigo']} concluído!")
                
                agora = datetime.now()
                if ano < agora.year or (ano == agora.year and mes < agora.month):
                    novo_ano, novo_mes = proximo_periodo(ano, mes)
                    status["ano"], status["mes"] = novo_ano, novo_mes
                else:
                    # É o mês atual, pula para a próxima filial e mantém o mês
                    status["filial_idx"] += 1
                
                status["chunk_idx"] = 0
                salvar_status(status)
                
        except Exception as e:
            print(f"ERRO CRÍTICO NO LOOP PRINCIPAL: {e}")
            print("Reiniciando em 60 segundos...")
            time.sleep(60)

if __name__ == "__main__":
    main()

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
CLIENT_LIB_DIR = r"C:\Users\informatica.ti\Documents\appdiscooveryzynapse\cmdintanci\instantclient_21_19"
USUARIO = "MIGRACAO"
SENHA = "fzabu69128XPKGY@!"

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
    else:
        print(f"Atenção: A pasta {CLIENT_LIB_DIR} não foi encontrada! O Oracle Client vai falhar.")
except Exception as e:
    print(f"Aviso: Não foi possível inicializar Oracle Client: {e}")

DSN = oracledb.makedsn("201.157.211.96", 1521, service_name="CS8NZK_190797_W_high.paas.oracle.com")

# --- CONFIGURAÇÕES SUPABASE ---
SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"

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
        
        # Verificar se a página carregou corretamente (detectar 404/erro)
        page_title = driver.title.lower() if driver.title else ""
        page_source_snippet = driver.page_source[:2000].lower() if driver.page_source else ""
        if "404" in page_title or "not found" in page_title or "erro" in page_title:
            print(f"  [!] Sieg retornou erro na página: {driver.title}")
            return None
        if "404" in page_source_snippet and ("not found" in page_source_snippet or "página não encontrada" in page_source_snippet):
            print(f"  [!] Sieg retornou página 404 para o cliente.")
            return None
        
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
        timeout_seconds = 300  # 5 minutos máximo
        for seg in range(timeout_seconds):
            # Verificar se o ZIP chegou
            arquivos = glob.glob(os.path.join(download_dir, '*.zip'))
            if arquivos and not glob.glob(os.path.join(download_dir, '*.crdownload')):
                zip_file_path = arquivos[0]
                break
            
            # A cada 10 segundos, verificar se o Sieg deu erro
            if seg > 0 and seg % 10 == 0:
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    print(f"  [!] Sieg exibiu alerta: {alert_text}")
                    alert.accept()
                    return None
                except:
                    pass
                
                try:
                    erro_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'error') or contains(@class, 'alert-danger')]")
                    for el in erro_elements:
                        if el.is_displayed() and el.text.strip():
                            print(f"  [!] Sieg exibiu erro na página: {el.text.strip()[:200]}")
                            return None
                    
                    curr_title = driver.title.lower() if driver.title else ""
                    if "404" in curr_title or "error" in curr_title:
                        print(f"  [!] Sieg redirecionou para página de erro: {driver.title}")
                        return None
                except:
                    pass
            
            # Log de progresso a cada 30 segundos
            if seg > 0 and seg % 30 == 0:
                print(f"  ... ainda aguardando ZIP ({seg}s/{timeout_seconds}s)")
            
            time.sleep(1)
        
        if not zip_file_path:
            print(f"  [!] Timeout de {timeout_seconds}s atingido esperando o ZIP. Sieg pode ter falhado.")
            
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
                
                toma_val = None
                for tag in ['.//ns:toma3/ns:toma', './/ns:toma0/ns:toma', './/ns:toma4/ns:toma', './/ns:toma']:
                    elem = root.find(tag, ns)
                    if elem is not None and elem.text:
                        toma_val = elem.text.strip()
                        break
                        
                cnpj_tomador = ""
                if toma_val == '0':
                    elem = root.find('.//ns:rem/ns:CNPJ', ns)
                    cnpj_tomador = elem.text if elem is not None else ""
                elif toma_val == '1':
                    elem = root.find('.//ns:exped/ns:CNPJ', ns)
                    cnpj_tomador = elem.text if elem is not None else ""
                elif toma_val == '2':
                    elem = root.find('.//ns:receb/ns:CNPJ', ns)
                    cnpj_tomador = elem.text if elem is not None else ""
                elif toma_val == '3':
                    elem = root.find('.//ns:dest/ns:CNPJ', ns)
                    cnpj_tomador = elem.text if elem is not None else ""
                elif toma_val == '4':
                    elem = root.find('.//ns:toma4/ns:CNPJ', ns)
                    cnpj_tomador = elem.text if elem is not None else ""
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    xml_content = f.read()
                    
                xml_data_dict[chave_xml] = {
                    'chave_xml': chave_xml,
                    'vTPrest': vTPrest,
                    'vICMS': vICMS,
                    'cnpj_remetente': cnpj_remetente,
                    'cnpj_tomador': cnpj_tomador,
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
    chave_cte = data_payload.get('chavecte')
    numnota = data_payload.get('numnota')
    
    # UPSERT nativo do Supabase: se numnota já existe, atualiza; senão, insere.
    url = f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?on_conflict=chave_xml"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

    try:
        response = requests.post(url, headers=headers, json=data_payload)
        if response.status_code in [200, 201]:
            print(f"  [>] Upsert CHAVE: {chave_cte} | Nota: {numnota} -> Supabase ({data_payload.get('status')}).")
        else:
            print(f"  [!] Erro ao enviar para Supabase: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Erro de conexão com Supabase: {e}")

def buscar_dados_winthor_por_chaves(lista_chaves):
    """Busca múltiplas chaves no WinThor de uma vez só, retorna dict {chave: dados}."""
    if not lista_chaves:
        return {}
    
    # Montar IN clause com as chaves
    chaves_sql = ",".join([f"'{c}'" for c in lista_chaves])
    sql = f"""
    SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent,
           n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, 
           n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms
    FROM pcnfent n, pcfornec f, pcnfbaseent b
    WHERE n.codfornec = f.codfornec(+)
      AND b.numtransent = n.numtransent
      AND n.chavecte IN ({chaves_sql})
      AND n.especie = 'NF'
      AND b.especie = 'NF'
      AND n.dtcancel is null
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
            if chave:
                notas_dict[chave] = dado
        connection.close()
        print(f"  [DB] {len(notas_dict)} de {len(lista_chaves)} chaves encontradas no WinThor.")
    except Exception as e:
        print(f"Erro ao buscar no Winthor por chaves: {e}")
    return notas_dict

def confrontar_e_enviar(chave_cte, dados_banco, xml_info, filial_obj, item_id, headers):
    """Compara dados do banco vs XML e envia ao Supabase. Atualiza a fila."""
    patch_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
    
    vl_tot_bd = float(dados_banco.get('vltotal') or 0.0)
    vl_icms_bd = float(dados_banco.get('vlicms') or 0.0)
    vl_tot_xml = float(xml_info.get('vTPrest', 0.0))
    vl_icms_xml = float(xml_info.get('vICMS', 0.0))
    
    cnpj_tomador_xml = xml_info.get('cnpj_tomador', "")
    cnpj_filial_db = filial_obj['cgc']
    diff_tomador = bool(cnpj_tomador_xml and cnpj_filial_db and (cnpj_tomador_xml != cnpj_filial_db))
    diff_tot = abs(vl_tot_bd - vl_tot_xml) > 0.01
    diff_icms = abs(vl_icms_bd - vl_icms_xml) > 0.01
    
    if diff_tomador:
        status_label = "Tomador Divergente"
    elif (diff_tot or diff_icms):
        status_label = "DIVERGENTE"
    else:
        status_label = "OK"
        
    obs = []
    if diff_tot: obs.append(f"TOTAL (DB: {vl_tot_bd} | XML: {vl_tot_xml})")
    if diff_icms: obs.append(f"ICMS (DB: {vl_icms_bd} | XML: {vl_icms_xml})")
    if diff_tomador: obs.append(f"Tomador ({cnpj_tomador_xml} != {cnpj_filial_db})")
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
        "chavecte": chave_cte,
        "chave_winthor": chave_cte,
        "chave_xml": xml_info.get('chave_xml'),
        "cnpj_remetente": xml_info.get('cnpj_remetente'),
        "cnpj_tomador": cnpj_tomador_xml,
        "cnpj_filial": filial_obj['cgc'],
        "dtemissao": str_em,
        "dtent": str_ent,
        "xml_doc": xml_info.get('xml_content'),
        "status": status_label,
        "obs": obs_str,
    }
    enviar_supabase(payload)
    # Apagar da fila após enviar com sucesso
    requests.delete(patch_url, headers={**headers, "Content-Type": "application/json"})
    print(f"    [Y] {chave_cte} -> {status_label} (removido da fila)")

def processar_fila_busca_isolada():
    url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?status=eq.Pendente"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json"
    }
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            return
        pendentes = resp.json()
        if not pendentes:
            return
        
        print(f"\n--- Processando [{len(pendentes)}] Chave(s) Pendente(s) na Busca Isolada ---")
        
        # 1. Marcar todas como "Processando" e montar mapa {chave: item_id}
        mapa_fila = {}  # {chavecte: item_id}
        todas_chaves = []
        for item in pendentes:
            chave_cte = item.get('chavecte')
            item_id = item.get('id')
            patch_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
            requests.patch(patch_url, headers={**headers, "Content-Type": "application/json"}, json={"status": "Processando"})
            mapa_fila[chave_cte] = item_id
            todas_chaves.append(chave_cte)
        
        # 2. Buscar TODAS as chaves no WinThor de uma vez (em blocos de 500 para não estourar o IN)
        dados_banco_total = {}
        for i in range(0, len(todas_chaves), 500):
            bloco = todas_chaves[i:i+500]
            resultado = buscar_dados_winthor_por_chaves(bloco)
            dados_banco_total.update(resultado)
        
        # 3. Separar as que não existem no WinThor
        for chave in todas_chaves:
            if chave not in dados_banco_total:
                item_id = mapa_fila[chave]
                del_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
                requests.delete(del_url, headers={**headers, "Content-Type": "application/json"})
                print(f"    [X] {chave} -> Não existe no WinThor. (removido da fila)")
        
        # 4. Agrupar chaves encontradas POR FILIAL
        grupos_por_filial = {}  # {codigo_filial: [chave1, chave2, ...]}
        for chave, dados in dados_banco_total.items():
            codfilialnf = str(dados.get('codfilialnf'))
            filial_obj = next((f for f in FILIAIS if str(f['codigo']) == codfilialnf), None)
            if not filial_obj:
                item_id = mapa_fila[chave]
                del_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
                requests.delete(del_url, headers={**headers, "Content-Type": "application/json"})
                print(f"    [X] {chave} -> Filial {codfilialnf} desconhecida. (removido da fila)")
                continue
            
            if codfilialnf not in grupos_por_filial:
                grupos_por_filial[codfilialnf] = []
            grupos_por_filial[codfilialnf].append(chave)
        
        # 5. Para CADA FILIAL, abrir UM navegador e baixar TODAS as chaves juntas
        for codfilialnf, chaves_filial in grupos_por_filial.items():
            filial_obj = next((f for f in FILIAIS if str(f['codigo']) == codfilialnf), None)
            print(f"\n  >> Filial {codfilialnf} ({filial_obj['uf']}): {len(chaves_filial)} chave(s) para baixar no Sieg")
            
            # Dividir em lotes de 2000 (limite do Sieg)
            lotes = [chaves_filial[i:i+2000] for i in range(0, len(chaves_filial), 2000)]
            
            for lote in lotes:
                zip_path = baixar_xmls_sieg(lote, filial_obj['link'])
                
                if zip_path:
                    dados_sieg_xml = extrair_e_parsear_xmls(zip_path)
                    
                    for chave in lote:
                        item_id = mapa_fila[chave]
                        dados_banco = dados_banco_total[chave]
                        
                        if chave in dados_sieg_xml:
                            xml_info = dados_sieg_xml[chave]
                            confrontar_e_enviar(chave, dados_banco, xml_info, filial_obj, item_id, headers)
                        else:
                            del_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
                            requests.delete(del_url, headers={**headers, "Content-Type": "application/json"})
                            print(f"    [X] {chave} -> XML ausente do ZIP. (removido da fila)")
                else:
                    # Falha no download - marcar todas do lote como erro
                    for chave in lote:
                        item_id = mapa_fila[chave]
                        del_url = f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?id=eq.{item_id}"
                        requests.delete(del_url, headers={**headers, "Content-Type": "application/json"})
                    print(f"    [X] Falha no download Sieg para filial {codfilialnf}. (removidos da fila)")
        
        print("\n--- Fila de Busca Isolada concluída! ---")
                    
    except Exception as e:
        print(f"Erro Processo Automático de Fila Busca Isolada: {e}")

def main():
    print("=== INICIANDO CONFRONTO FISCAL AUTOMÁTICO (SOMENTE FILA BUSCA ISOLADA) ===")
    
    print("Iniciando modo contínuo (Monitoramento de Fila).")
    
    # Ciclo Contínuo focado na Busca Isolada
    while True:
        try:
            processar_fila_busca_isolada()
                
        except Exception as e:
            print(f"ERRO CRÍTICO NO MODO CONTÍNUO: {e}")
            
        # Espera curta para manter rapidez
        time.sleep(30)

if __name__ == "__main__":
    main()

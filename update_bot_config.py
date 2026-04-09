import requests
import json

SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

# 1. Buscar a configuração atual
print("Baixando configuração atual...")
resp = requests.get(f"{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1", headers=HEADERS)
data = resp.json()

if not data:
    print("Erro: Nenhuma configuração encontrada no ID 1.")
    exit()

config_record = data[0]
config_json = config_record.get('config_json', {})

if isinstance(config_json, str):
    config_json = json.loads(config_json)

# 2. Atualizar as queries no JSON
print("Atualizando as queries SQL (com codfiscal e conferido)...")

config_json['sqls']['nf_consumo']['varredura'] = "SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent, n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms, NVL(b.vlpis,0) AS vlpis, NVL(b.vlcofins,0) AS vlcofins, b.codfiscal FROM pcnfent n, pcfornec f, pcnfbaseent b WHERE n.codfornec = f.codfornec(+) AND b.numtransent = n.numtransent AND n.codfilialnf = '{codfilial}' AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY') AND n.especie = 'NF' AND b.especie = 'NF' AND B.codfiscal IN (1556,2556) AND n.conferido = 'N' AND n.dtcancel is null ORDER BY n.codfilial , n.numnota"

config_json['sqls']['danfe']['varredura'] = "SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent, n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms, NVL(b.vlpis,0) AS vlpis, NVL(b.vlcofins,0) AS vlcofins, b.codfiscal FROM pcnfent n, pcfornec f, pcnfbaseent b WHERE n.codfornec = f.codfornec(+) AND b.numtransent = n.numtransent AND n.codfilialnf = '{codfilial}' AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY') AND n.especie = 'NF' AND b.especie = 'NF' AND B.codfiscal NOT IN (1556,2556) AND n.conferido = 'N' AND n.dtcancel is null ORDER BY n.codfilial , n.numnota"

config_json['sqls']['cte']['varredura'] = "SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent, n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms, NVL(b.vlpis,0) AS vlpis, NVL(b.vlcofins,0) AS vlcofins, b.codfiscal FROM pcnfent n, pcfornec f, pcnfbaseent b WHERE n.codfornec = f.codfornec(+) AND b.numtransent = n.numtransent AND n.codfilialnf = '{codfilial}' AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY') AND n.especie = 'CT' AND b.especie = 'CT' AND n.conferido = 'N' AND n.dtcancel is null ORDER BY n.codfilial , n.numnota"

config_json['sqls']['nf_servico']['varredura'] = "SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent, n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms, NVL(e.vlpis,0) AS vlpis, NVL(e.vlcofins,0) AS vlcofins, b.codfiscal FROM pcnfent n, pcfornec f, pcnfbase b, pcnfentpiscofins e WHERE n.codfornec = f.codfornec(+) AND b.numtransent = n.numtransent and b.numtransent = e.numtransent and n.numtransent = e.numtransent AND n.codfilialnf = '{codfilial}' AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY') AND n.especie = 'NS' AND B.codfiscal IN (1933,2933) AND n.conferido = 'N' AND n.dtcancel is null ORDER BY n.codfilial , n.numnota"


# 3. Fazer o UPDATE no Supabase
print("Enviando atualização para o Supabase...")
patch_resp = requests.patch(
    f"{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1",
    headers=HEADERS,
    json={"config_json": config_json}
)

if patch_resp.status_code in [200, 204]:
    print("✅ Sucesso! O JSON app_config_bot foi atualizado com todas as colunas ausentes no banco.")
else:
    print(f"❌ Erro ao atualizar: {patch_resp.status_code}")
    print(patch_resp.text)

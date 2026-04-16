import urllib.request
import json
import os

SUPABASE_URL = 'https://jmcwiszplkjksdvqyhbw.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM'

headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

req = urllib.request.Request(f'{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1', headers=headers)
resp = urllib.request.urlopen(req)
data = json.loads(resp.read())

config_json = data[0]['config_json']

config_json['sqls']['CONSUMO']['toma_check'] = "SELECT n.numtransent, n.vltotal, NVL(b.vlicms, 0) AS vlicms, n.codfilial, n.codfilialnf, n.conferido FROM pcnfent n, pcnfbase b WHERE b.numtransent = n.numtransent AND n.numnota = :numnota AND n.codfilialnf = :codfilialnf_correto AND n.especie = 'NF' AND n.dtcancel is null"

config_json['sqls']['CONSUMO']['veri_check'] = "SELECT n.numtransent, n.vltotal, n.codfilial, n.conferido, NVL(b.vlicms, 0) as vlicms FROM pcnfent n, pcnfbase b WHERE b.numtransent = n.numtransent AND n.numtransent IN ({binds}) AND n.especie = 'NF' AND n.dtcancel is null"

config_json['sqls']['CONSUMO']['veri_update'] = "UPDATE pcnfent n SET n.conferido = 'S' WHERE n.numtransent IN ({binds}) AND n.especie = 'NF' AND NVL(n.conferido, 'N') = 'N' AND n.dtcancel is null AND EXISTS (SELECT 1 FROM pcnfbase b WHERE b.numtransent = n.numtransent)"

payload = json.dumps({'config_json': config_json}).encode('utf-8')
patch_req = urllib.request.Request(f'{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1', data=payload, headers=headers, method='PATCH')
patch_resp = urllib.request.urlopen(patch_req)
print("Configuração atualizada no Supabase!")

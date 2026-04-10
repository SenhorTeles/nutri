import requests, json
URL='https://jmcwiszplkjksdvqyhbw.supabase.co'
KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM'
h={'apikey':KEY, 'Authorization': f'Bearer {KEY}', 'Content-Type':'application/json'}
r=requests.get(URL+'/rest/v1/app_config_bot?id=eq.1', headers=h).json()[0]
cfg=r['config_json']

q="""SELECT n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, n.numnota, n.numtransent, n.vltotal, n.codfornec, f.fornecedor, f.cgc, n.chavenfe, n.chavecte, n.dtemissao, n.conferido, n.dtent, NVL(b.vlicms, 0) AS vlicms, NVL(e.vlpis, 0) AS vlpis, NVL(e.vlcofins, 0) AS vlcofins, b.codfiscal FROM pcnfent n, pcfornec f, pcnfbase b, pcnfentpiscofins e WHERE n.codfornec = f.codfornec(+) AND b.numtransent = n.numtransent and b.numtransent = e.numtransent and n.numtransent = e.numtransent AND n.chavenfe IN ({chaves_sql}) AND n.especie = 'NF' AND B.codfiscal IN (1556,2556) AND NVL(n.conferido, 'N') = 'N' AND n.dtcancel IS NULL"""

cfg['sqls']['CONSUMO']['busca_isolada'] = q
requests.patch(URL+'/rest/v1/app_config_bot?id=eq.1', headers=h, json={'config_json': cfg})
print('OK Busca Isolada Updated')

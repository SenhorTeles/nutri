import requests, json, re
URL='https://jmcwiszplkjksdvqyhbw.supabase.co'
KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM'
h={'apikey':KEY, 'Authorization': f'Bearer {KEY}', 'Content-Type':'application/json'}
r=requests.get(URL+'/rest/v1/app_config_bot?id=eq.1', headers=h).json()[0]
cfg=r['config_json']

for k in cfg['sqls']:
    if 'busca_isolada' in cfg['sqls'][k]:
        cfg['sqls'][k]['busca_isolada'] = re.sub(r"AND\s+NVL\(n\.conferido,\s*'N'\)\s*=\s*'N'", '', cfg['sqls'][k]['busca_isolada'], flags=re.IGNORECASE)

requests.patch(URL+'/rest/v1/app_config_bot?id=eq.1', headers=h, json={'config_json': cfg})
print('OK retirado')

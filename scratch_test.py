import requests, oracledb, os
URL='https://jmcwiszplkjksdvqyhbw.supabase.co'
KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM'
h={'apikey':KEY, 'Authorization': f'Bearer {KEY}'}
c = requests.get(URL+'/rest/v1/app_config_bot?id=eq.1', headers=h).json()[0]['config_json']['oracle_credentials']
if os.path.exists(c['client_lib_dir']):
    oracledb.init_oracle_client(lib_dir=c['client_lib_dir'])
conn = oracledb.connect(user=c['user'], password=c['password'], dsn=oracledb.makedsn(c['host'], c['port'], service_name=c['service_name']))
curs = conn.cursor()
curs.execute("SELECT chavenfe, TO_CHAR(dtent, 'DD/MM/YYYY HH24:MI:SS'), numtransent, codfilialnf, codfilial FROM pcnfent WHERE numtransent = (SELECT MAX(numtransent) FROM pcnfent)")
print("PCNFENT:", curs.fetchone())
conn.close()

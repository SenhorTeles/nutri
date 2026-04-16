import requests
import oracledb
import json

SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Accept": "application/json"}

# 1. Fetch Oracle Credentials
r = requests.get(f"{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1&select=config_json", headers=HEADERS)
data = r.json()[0]['config_json']
creds = data.get('oracle_credentials', {})

user = creds.get('user')
password = creds.get('password')
host = creds.get('host')
port = creds.get('port')
service_name = creds.get('service_name')
dsn = oracledb.makedsn(host, port, service_name=service_name)

# 2. Get the record from Supabase
confronto_id = 5312
r2 = requests.get(f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?id=eq.{confronto_id}&select=numtransent,numnota", headers=HEADERS)
record = r2.json()[0]
numtransent = record.get('numtransent')
numnota = record.get('numnota')

print(f"Supabase record {confronto_id}: numtransent={numtransent}, numnota={numnota}")

if not numtransent:
    print("numtransent is null, exiting.")
    exit(0)

# 3. Check Oracle DB
print(f"Connecting to Oracle: {user}@{dsn}")
try:
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    print("\n--- PCNFENT ---")
    cursor.execute("SELECT numtransent, codfilial, especie, numnota, vltotal FROM pcnfent WHERE numtransent = :1", [numtransent])
    res1 = cursor.fetchall()
    for r in res1:
        print(r)
        
    print("\n--- PCNFBASEENT ---")
    cursor.execute("SELECT numtransent, numtranspiscofins, especie, vlcontabil FROM pcnfbaseent WHERE numtransent = :1", [numtransent])
    res2 = cursor.fetchall()
    for r in res2:
        print(r)
        
    print("\n--- PCNFBASE ---")
    cursor.execute("SELECT numtransent, numtranspiscofins, tipo, vlcontabil FROM pcnfbase WHERE numtransent = :1", [numtransent])
    res3 = cursor.fetchall()
    for r in res3:
        print(r)
        
    print("\n--- PCNFENTPISCOFINS ---")
    cursor.execute("SELECT numtransent, numtranspiscofins FROM pcnfentpiscofins WHERE numtransent = :1", [numtransent])
    res4 = cursor.fetchall()
    for r in res4:
        print(r)
        
    print("\n--- VERI_CHECK SCRIPT (as currently in DB) ---")
    cursor.execute("SELECT n.numtransent, n.vltotal, n.codfilial, n.conferido, NVL(b.vlicms, 0) as vlicms FROM pcnfent n, pcnfbaseent b WHERE b.numtransent = n.numtransent AND n.numtransent IN (:1) AND n.especie = 'NF' AND b.especie = 'NF' AND n.dtcancel is null", [numtransent])
    res5 = cursor.fetchall()
    for r in res5:
        print(r)
        
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")

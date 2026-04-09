import requests

SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# Verificar fila
resp = requests.get(f"{SUPABASE_URL}/rest/v1/busca_isolada_queue?select=*", headers=HEADERS)
data = resp.json()
print(f"=== FILA busca_isolada_queue: {len(data)} registro(s) ===")
print(data)

# Verificar registro no confronto
chave = "42260302354988000112550040000137351420885919"
resp2 = requests.get(f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?chavecte=eq.{chave}&select=id,chavecte,status,obs,vltotal,vltotal_xml", headers=HEADERS)
data2 = resp2.json()
print(f"\n=== Registro no confronto para chave {chave[:20]}... ===")
for d in data2:
    print(f"  id={d.get('id')} | status={d.get('status')} | obs={d.get('obs')}")
    print(f"  vltotal={d.get('vltotal')} | vltotal_xml={d.get('vltotal_xml')}")

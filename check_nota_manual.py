"""
Script de diagnóstico manual para a nota com chave:
33260202618774000106550530000046451935520830

Consulta todas as tabelas relevantes no Oracle para entender
por que o veri_check está dando "Nota removida DB".
"""
import requests
import oracledb

SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Accept": "application/json"}

CHAVE_XML = "33260202618774000106550530000046451935520830"

print("=" * 70)
print(f"DIAGNÓSTICO COMPLETO PARA CHAVE:")
print(f"{CHAVE_XML}")
print("=" * 70)

# 1. Buscar credenciais Oracle
print("\n[1] Baixando credenciais Oracle do Supabase...")
r = requests.get(f"{SUPABASE_URL}/rest/v1/app_config_bot?id=eq.1&select=config_json", headers=HEADERS)
data = r.json()[0]['config_json']
creds = data.get('oracle_credentials', {})
sqls = data.get('sqls', {})

# Mostrar o veri_check do CONSUMO que está no banco
print("\n[2] SQL veri_check do CONSUMO (armazenado no Supabase):")
veri_check_sql = sqls.get('CONSUMO', {}).get('veri_check', 'NÃO ENCONTRADO')
print(f"   {veri_check_sql}")

# 2. Buscar o registro no Supabase
print("\n[3] Buscando registro no Supabase (confrontofiscalnfconsumo)...")
r2 = requests.get(f"{SUPABASE_URL}/rest/v1/confrontofiscalnfconsumo?chave_xml=eq.{CHAVE_XML}&select=id,numtransent,numnota,status,obs,codfilial,codfilialnf,vltotal,vlicms", headers=HEADERS)
supa_records = r2.json()
if supa_records:
    for rec in supa_records:
        print(f"   ID: {rec.get('id')}")
        print(f"   numtransent: {rec.get('numtransent')}")
        print(f"   numnota: {rec.get('numnota')}")
        print(f"   status: {rec.get('status')}")
        print(f"   obs: {rec.get('obs')}")
        print(f"   codfilial: {rec.get('codfilial')}")
        print(f"   codfilialnf: {rec.get('codfilialnf')}")
        print(f"   vltotal: {rec.get('vltotal')}")
        print(f"   vlicms: {rec.get('vlicms')}")
    numtransent = supa_records[0].get('numtransent')
else:
    print("   ⚠️ NENHUM REGISTRO ENCONTRADO NO SUPABASE!")
    numtransent = None

# 3. Consultar Oracle
print(f"\n[4] Conectando ao Oracle ({creds.get('host')}:{creds.get('port')})...")
dsn = oracledb.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])

try:
    import os
    if os.path.exists(creds.get('client_lib_dir', '')):
        oracledb.init_oracle_client(lib_dir=creds['client_lib_dir'])
except:
    pass

conn = oracledb.connect(user=creds['user'], password=creds['password'], dsn=dsn)
cursor = conn.cursor()

# CONSULTA 1: Buscar pela CHAVE no PCNFENT
print("\n" + "=" * 70)
print("[5] PCNFENT - Busca por CHAVENFE:")
cursor.execute("""
    SELECT numtransent, codfilial, codfilialnf, especie, serie, numnota, 
           vltotal, conferido, dtcancel, chavenfe 
    FROM pcnfent 
    WHERE chavenfe = :chave
""", chave=CHAVE_XML)
cols = [c[0] for c in cursor.description]
rows = cursor.fetchall()
if rows:
    for row in rows:
        for col, val in zip(cols, row):
            print(f"   {col}: {val}")
        numtransent = row[0]  # Pegar o numtransent real do Oracle
        print(f"   >>> numtransent encontrado: {numtransent}")
else:
    print("   ❌ NENHUM REGISTRO! A nota NÃO EXISTE no pcnfent pela chave.")

# CONSULTA 2: Se temos numtransent, buscar nas tabelas dependentes
if numtransent:
    print(f"\n[6] PCNFBASEENT - Busca por numtransent={numtransent}:")
    cursor.execute("""
        SELECT numtransent, codfiscal, especie, aliquota, vlbase, vlicms, 
               vlcontabil, sittribut, numtranspiscofins 
        FROM pcnfbaseent 
        WHERE numtransent = :num
    """, num=numtransent)
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            for col, val in zip(cols, row):
                print(f"   {col}: {val}")
    else:
        print("   ❌ NENHUM REGISTRO no PCNFBASEENT! << ESTA É PROVAVELMENTE A CAUSA DO ERRO!")
        print("   A nota existe no PCNFENT mas NÃO tem registro na PCNFBASEENT.")
        print("   O veri_check faz JOIN entre pcnfent e pcnfbaseent, então não encontra nada.")

    print(f"\n[7] PCNFBASE - Busca por numtransent={numtransent}:")
    cursor.execute("""
        SELECT numtransent, codfiscal, tipo, vlcontabil, numtranspiscofins 
        FROM pcnfbase 
        WHERE numtransent = :num
    """, num=numtransent)
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            for col, val in zip(cols, row):
                print(f"   {col}: {val}")
    else:
        print("   Nenhum registro no PCNFBASE.")

    print(f"\n[8] PCNFENTPISCOFINS - Busca por numtransent={numtransent}:")
    cursor.execute("""
        SELECT numtransent, numtranspiscofins, codtribpiscofins 
        FROM pcnfentpiscofins 
        WHERE numtransent = :num
    """, num=numtransent)
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            for col, val in zip(cols, row):
                print(f"   {col}: {val}")
    else:
        print("   Nenhum registro no PCNFENTPISCOFINS.")

    # CONSULTA FINAL: Simular o VERI_CHECK exatamente como o bot faz
    print(f"\n{'=' * 70}")
    print(f"[9] SIMULAÇÃO DO VERI_CHECK (exatamente como o bot executa):")
    print(f"    numtransent = {numtransent}")
    
    # Se temos o SQL veri_check do Supabase, usá-lo
    if veri_check_sql and veri_check_sql != 'NÃO ENCONTRADO':
        test_sql = veri_check_sql.replace('{binds}', str(numtransent))
        print(f"    SQL: {test_sql[:200]}...")
        try:
            cursor.execute(test_sql)
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            if rows:
                print(f"    ✅ ENCONTROU {len(rows)} registro(s):")
                for row in rows:
                    for col, val in zip(cols, row):
                        print(f"       {col}: {val}")
            else:
                print("    ❌ NENHUM RESULTADO! É por isso que dá 'Nota removida DB'!")
        except Exception as e:
            print(f"    ❌ ERRO ao executar: {e}")
    else:
        # Fallback - simular manualmente
        test_sql = f"""
            SELECT n.numtransent, n.vltotal, n.codfilial, n.conferido, NVL(b.vlicms, 0) as vlicms 
            FROM pcnfent n, pcnfbaseent b 
            WHERE b.numtransent = n.numtransent 
            AND n.numtransent IN ({numtransent}) 
            AND n.especie = 'NF' 
            AND b.especie = 'NF' 
            AND n.dtcancel is null
        """
        print(f"    SQL (fallback manual): {test_sql.strip()}")
        cursor.execute(test_sql)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        if rows:
            print(f"    ✅ ENCONTROU {len(rows)} registro(s):")
            for row in rows:
                for col, val in zip(cols, row):
                    print(f"       {col}: {val}")
        else:
            print("    ❌ NENHUM RESULTADO! É por isso que dá 'Nota removida DB'!")

else:
    print("\n[!] Não foi possível obter numtransent. Não dá pra continuar o diagnóstico.")

cursor.close()
conn.close()

print(f"\n{'=' * 70}")
print("DIAGNÓSTICO FINALIZADO.")
print("=" * 70)
input("\nPressione ENTER para fechar...")

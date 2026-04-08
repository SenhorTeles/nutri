import os
import oracledb
from datetime import datetime

ORACLE_USUARIO = "MIGRACAO"
ORACLE_SENHA = "fzabu69128XPKGY@!"
ORACLE_HOST = "201.157.211.96"
ORACLE_PORTA = 1521
ORACLE_SERVICE = "CS8NZK_190797_W_high.paas.oracle.com"

CLIENT_LIB_DIR = r"C:\Users\informatica.ti\Documents\appdiscooveryzynapse\cmdintanci\instantclient_21_19"

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

try:
    oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
    log("[OK] Oracle Client inicializado!")
except Exception as e:
    log(f"[ERRO] Oracle Client: {e}")

def conectar():
    dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORTA, service_name=ORACLE_SERVICE)
    log(f"Conectando: {ORACLE_HOST}:{ORACLE_PORTA} / {ORACLE_SERVICE}")
    connection = oracledb.connect(user=ORACLE_USUARIO, password=ORACLE_SENHA, dsn=dsn)
    log(f"[OK] Conectado! (User: {ORACLE_USUARIO})")
    return connection

if __name__ == "__main__":
    print("=" * 60)
    print("  TESTE DE CONEXAO - WINTHOR ONLINE (TOTVS CLOUD)")
    print("=" * 60)
    print(f"  Host:    {ORACLE_HOST}")
    print(f"  Porta:   {ORACLE_PORTA}")
    print(f"  Service: {ORACLE_SERVICE}")
    print(f"  User:    {ORACLE_USUARIO}")
    print("=" * 60)

    connection = None
    try:
        connection = conectar()
        cursor = connection.cursor()

        cursor.execute("SELECT USER, SYSDATE FROM dual")
        user, data_srv = cursor.fetchone()
        log(f"Usuario conectado: {user}")
        log(f"Data/hora servidor: {data_srv}")

        log("")
        log("--- FILIAIS CADASTRADAS (PCFILIAL) ---")
        cursor.execute("SELECT codigo, razaosocial FROM pcfilial ORDER BY codigo FETCH FIRST 20 ROWS ONLY")
        filiais = cursor.fetchall()
        if filiais:
            for f in filiais:
                log(f"  Filial {f[0]}: {f[1]}")
        else:
            log("  Nenhuma filial encontrada.")

        log("")
        log("[SUCESSO] Conexao ao Winthor Online OK!")

    except Exception as e:
        log(f"[ERRO] {e}")

    finally:
        if connection:
            connection.close()
            log("Conexao fechada.")
import os
import time
import sys
import subprocess
import traceback
from datetime import datetime

def __global_exception_handler(exctype, value, tb):
    print("\n[ERRO FATAL NO INICIALIZADOR DETECTADO]")
    print(f"Tipo: {exctype.__name__}")
    print(f"Mensagem: {value}")
    traceback.print_tb(tb)
    input("\nPressione ENTER para fechar o programa e ler o erro...")
    sys.exit(1)

sys.excepthook = __global_exception_handler

# --- AUTO-INSTALAÇÃO DE BIBLIOTECAS ---
# Caso você prefira jogar só o arquivo .py cru lá no servidor,
# ele vai checar e instalar automaticamente o que precisar (Requests é o cara que faz o download HTTP).
try:
    import requests
except ImportError:
    print("[AUTO-SETUP] Instalando biblioteca 'requests' requerida...")
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
    import requests


# --- CONFIGURAÇÕES E CREDENCIAIS (Mesmas da automação principal) ---
SUPABASE_URL = "https://jmcwiszplkjksdvqyhbw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptY3dpc3pwbGtqa3NkdnF5aGJ3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTU4NzM4MiwiZXhwIjoyMDkxMTYzMzgyfQ.ZY1eEvUyjfyG3kfTqhahtDaWWngPhw3IMdOzOk4cigM"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

HEADERS_GET = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json"
}

# --- PASTA ONDE TUDO VAI SER BAIXADO E RODADO ---
DIR_INSTALADOR = r"C:\Users\informatica.ti\Documents\appdiscooveryzynapse\controlev"
WATCHDOG_FILE = os.path.join(DIR_INSTALADOR, "ultimo_instalado.txt")


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def kill_process(filename):
    """
    Se o Python (ou o arquivo) com esse nome estiver rodando, ele é finalizado e expulso da memória 
    para liberar espaço para a nova versão ser baixada e substituir a antiga.
    """
    try:
        log(f"Tentando parar possível execução antiga de: {filename}...")
        # Usa taskkill com /F (forçar) /IM (pelo nome da imagem)
        os.system(f'taskkill /F /IM "{filename}" /T >nul 2>&1')
        # Pequena pausa pro HD do Windows assimilar o comando e soltar a trava do arquivo
        time.sleep(2)
    except Exception as e:
        log(f"Aviso ao tentar parar o processo: {e}")

def atualizar_supabase(record_id, status, log_message=None):
    """Atualiza o banco do front-end."""
    payload = {"status": status}
    if log_message:
        payload["log"] = str(log_message)
    
    url = f"{SUPABASE_URL}/rest/v1/atualizacoes?id=eq.{record_id}"
    try:
        requests.patch(url, headers=HEADERS, json=payload)
    except Exception as e:
        log(f"Erro ao salvar status no banco: {e}")

def baixar_arquivo(filename, filepath):
    """Baixa a última versão em tempo real do Storage."""
    # Como você ativou o modo public no SQL, podemos baixar da rota 'public' de forma super rápida:
    url = f"{SUPABASE_URL}/storage/v1/object/public/versoes_app/{filename}"
    log(f"Fazendo download de {url}...")
    
    # Faz requisição aceitando stream (para baixar partes por vez sem ocupar toda RAM se for muito pesado)
    resp = requests.get(url, headers={"Authorization": f"Bearer {SUPABASE_KEY}"}, stream=True)
    
    # Fallback caso o bucket tenha ficado como restrito no banco:
    if resp.status_code != 200:
        url_auth = f"{SUPABASE_URL}/storage/v1/object/authenticated/versoes_app/{filename}"
        resp = requests.get(url_auth, headers={"Authorization": f"Bearer {SUPABASE_KEY}"}, stream=True)
        if resp.status_code != 200:
            raise Exception(f"Erro HTTP {resp.status_code} na hora do Download: {resp.text}")

    with open(filepath, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                
    log(f"Download concluído 100%! Salvo em: {filepath}")

# =========================================================
# === WATCHDOG: O CÃO DE GUARDA ===========================
# =========================================================
def salvar_no_watchdog(filename):
    """Guarda qual foi o último arquivo oficial instalado"""
    with open(WATCHDOG_FILE, 'w') as f:
        f.write(filename)

def ler_arquivo_watchdog():
    if os.path.exists(WATCHDOG_FILE):
        with open(WATCHDOG_FILE, 'r') as f:
            return f.read().strip()
    return None

def is_process_running(filename):
    """Verifica silenciosamente na memória do Windows se o programa está ativo"""
    try:
        # Usa tasklist para checar sem estourar janela de cmd no fundo 
        output = subprocess.check_output(
            f'tasklist /FI "IMAGENAME eq {filename}" /NH', 
            shell=True, text=True, 
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )
        if filename.lower() in output.lower():
            return True
    except Exception:
        pass
    return False

def aplicar_watchdog_agora():
    """Confere se o arquivo alvo atual não foi fechado por invasores/erros"""
    alvo = ler_arquivo_watchdog()
    if not alvo:
        return # Nenhuma instalação oficial detectada ainda
        
    if not is_process_running(alvo):
        filepath = os.path.join(DIR_INSTALADOR, alvo)
        if os.path.exists(filepath):
            log(f"[WATCHDOG] ⚠️ ALARME VERMELHO: '{alvo}' foi encerrado! Reiniciando a força...")
            try:
                subprocess.Popen(f'start "" "{filepath}"', cwd=DIR_INSTALADOR, shell=True)
                log(f"[WATCHDOG] ✅ '{alvo}' re-estabelecido em memória!")
            except Exception as e:
                log(f"[WATCHDOG] ❌ Falha ao tentar ressuscitar '{alvo}': {e}")

def rodar_loop_agente_invisivel():
    """Fica 24 hrs por dia escutando os chamados do supabase."""
    # Garante que a pasta vai existir antes de baixar qualquer coisa
    if not os.path.exists(DIR_INSTALADOR):
        os.makedirs(DIR_INSTALADOR)
        
    ciclo_watchdog = 5
        
    while True:
        try:
            # 1. Puxa do Supabase se tem alguém pedindo atualização (Status 'Pendente')
            url_get = f"{SUPABASE_URL}/rest/v1/atualizacoes?status=eq.Pendente&order=created_at.asc"
            resp = requests.get(url_get, headers=HEADERS_GET, timeout=10)
            
            if resp.status_code == 200:
                pendentes = resp.json()
                
                for p in pendentes:
                    record_id = p.get('id')
                    filename = p.get('arquivo')
                    
                    if not filename:
                        continue
                        
                    log(f"\n--- [!!!] ALARME! NOVA ATUALIZAÇÃO DETECTADA: {filename} ---")
                    
                    # Comunica a tela do usuário que a rotina vai começar (front muda a barra)
                    atualizar_supabase(record_id, "Instalando...", "Desligando a versão anterior e baixando a nova...")
                    
                    filepath = os.path.join(DIR_INSTALADOR, filename)
                    
                    try:
                        # Passo A: Mata o executável atual com crueldade
                        kill_process(filename)
                        
                        # Passo B: Limpa da pasta o arquivo arcaico 
                        if os.path.exists(filepath):
                            log(f"Deletando a versão antiga ({filepath})...")
                            os.remove(filepath)
                            
                        # Passo C: Faz o download da nuvem fresco
                        baixar_arquivo(filename, filepath)
                        
                        # Passo D: Acende a fagulha (Cria o processo e desanexa o Python principal dele)
                        log(f"Lançando {filename} na máquina...")
                        
                        # O start fará ele rodar de forma isolada, permitindo a nossa rotina do while não travar esperando ele fechar
                        subprocess.Popen(f'start "" "{filepath}"', cwd=DIR_INSTALADOR, shell=True)
                        
                        # Alimenta o cão de guarda para a nova versão
                        salvar_no_watchdog(filename)
                        
                        # Passo E: Fim do turno, avisa a tela. 
                        # Isso vai fazer o front-end na hora dar "OK" e tirar o modal 
                        atualizar_supabase(record_id, "OK", f"Arquivo {filename} atualizado com êxito!")
                        log(f"--- FIM DO SERVIÇO! Sistema {filename} no Ar! Escutando próximos pedidos... ---\n")
                        
                    except Exception as ex:
                        log(f"FALHA NO PROCESSO: {ex}")
                        atualizar_supabase(record_id, "Erro", str(ex))
                        
        except requests.exceptions.RequestException:
            # Falha boba na internet, a gente nem chuta erro pro console, ele só espera 3 segundos e repete
            pass
        except Exception as e:
            log(f"ERRO ESTANHO NO LOOP: {e}")
            
        # O Watchdog vai latir a cada 15 segundos
        ciclo_watchdog += 1
        if ciclo_watchdog >= 5: # 5 ciclos de 3s = 15s
            aplicar_watchdog_agora()
            ciclo_watchdog = 0
            
        # Uma respiração no servidor a cada 3 segundos, ele lê seu comando 20 vezes por minuto!
        time.sleep(3)

if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    log("=============================================================")
    log("🚀 ZYNAPSE UPDATE DEAMON - RODANDO EM SEGUNDO PLANO")
    log("📡 Conectado ao Supabase: AGUARDANDO DEPLOY REMOTO")
    log(f"📂 Workspace de Instalação: {DIR_INSTALADOR}")
    log("=============================================================\n")
    
    rodar_loop_agente_invisivel()

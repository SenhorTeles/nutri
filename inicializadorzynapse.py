import os
import time
import sys
import subprocess
import traceback
from datetime import datetime

# Intercepta exceções globais mas NÃO fecha o programa
def __global_exception_handler(exctype, value, tb):
    try:
        print(f"\n[ERRO GLOBAL INTERCEPTADO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"Tipo: {exctype.__name__}")
        print(f"Mensagem: {value}")
        traceback.print_tb(tb)
        print("[O DAEMON CONTINUA RODANDO - ESTE ERRO FOI APENAS LOGADO]\n")
    except Exception:
        pass  # Nunca morrer, nem logando erro

sys.excepthook = __global_exception_handler

# --- AUTO-INSTALAÇÃO DE BIBLIOTECAS ---
# Caso você prefira jogar só o arquivo .py cru lá no servidor,
# ele vai checar e instalar automaticamente o que precisar (Requests é o cara que faz o download HTTP).
try:
    import requests
except ImportError:
    try:
        print("[AUTO-SETUP] Instalando biblioteca 'requests' requerida...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
        import requests
    except Exception as e:
        print(f"\n[ERRO FATAL] Não foi possível instalar a biblioteca 'requests': {e}")
        traceback.print_exc()
        input("\nPressione ENTER para fechar...")
        sys.exit(1)


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
        requests.patch(url, headers=HEADERS, json=payload, timeout=10)
    except Exception as e:
        log(f"Erro ao salvar status no banco: {e}")

def deletar_linha_supabase(record_id):
    """Deleta uma linha problemática do Supabase para não travar o loop."""
    url = f"{SUPABASE_URL}/rest/v1/atualizacoes?id=eq.{record_id}"
    try:
        requests.delete(url, headers=HEADERS, timeout=10)
        log(f"🗑️ Linha {record_id} removida do banco para evitar loop de erro.")
    except Exception as e:
        log(f"Erro ao deletar linha {record_id}: {e}")

def notificar_erro_supabase(record_id, erro):
    """Notifica erro no Supabase e depois deleta a linha problemática."""
    try:
        atualizar_supabase(record_id, "Erro", f"ERRO CRITICO: {erro}")
        time.sleep(1)  # Dá tempo do front-end ler o status
        deletar_linha_supabase(record_id)
    except Exception:
        pass  # Nunca morrer

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
    """Fica 24 hrs por dia escutando os chamados do supabase. NUNCA PODE MORRER."""
    # Garante que a pasta vai existir antes de baixar qualquer coisa
    try:
        if not os.path.exists(DIR_INSTALADOR):
            os.makedirs(DIR_INSTALADOR)
    except Exception as e:
        log(f"Aviso: Não conseguiu criar pasta {DIR_INSTALADOR}: {e}")
        
    ciclo_watchdog = 5
    erros_consecutivos = 0
    MAX_ERROS_ANTES_PAUSA = 10  # Se errar 10x seguidas, dá uma pausa maior
        
    while True:
        try:
            # 1. Puxa do Supabase se tem alguém pedindo atualização (Status 'Pendente')
            url_get = f"{SUPABASE_URL}/rest/v1/atualizacoes?status=eq.Pendente&order=created_at.asc"
            resp = requests.get(url_get, headers=HEADERS_GET, timeout=15)
            
            if resp.status_code == 200:
                pendentes = resp.json()
                
                for p in pendentes:
                    record_id = p.get('id')
                    filename = p.get('arquivo')
                    
                    if not filename:
                        # Linha sem arquivo = lixo, deleta e segue
                        try:
                            deletar_linha_supabase(record_id)
                        except Exception:
                            pass
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
                        # Erro ao processar UM item: notifica, deleta a linha, e segue para o próximo
                        erro_msg = f"{type(ex).__name__}: {ex}"
                        log(f"❌ FALHA NO PROCESSO [{filename}]: {erro_msg}")
                        log(f"   Traceback: {traceback.format_exc()}")
                        notificar_erro_supabase(record_id, erro_msg)
                        log(f"🔄 Continuando o loop normalmente...")
                        
            # Reset do contador de erros (se chegou aqui, deu certo)
            erros_consecutivos = 0
                        
        except requests.exceptions.RequestException as e:
            # Falha de rede - normal, só espera e tenta de novo
            erros_consecutivos += 1
            if erros_consecutivos % 20 == 0:  # Só loga a cada 20 falhas pra não poluir
                log(f"⚠️ Sem conexão com Supabase ({erros_consecutivos}x): {type(e).__name__}")
        except Exception as e:
            # Qualquer outro erro estranho - NUNCA MORRE
            erros_consecutivos += 1
            log(f"⚠️ ERRO NO LOOP (tentativa {erros_consecutivos}): {type(e).__name__}: {e}")
            log(f"   Traceback: {traceback.format_exc()}")
            log(f"🔄 O daemon vai continuar rodando...")
            
        # Se está errando demais, dá uma pausa maior pra não fritar CPU/rede
        if erros_consecutivos >= MAX_ERROS_ANTES_PAUSA:
            log(f"⏸️ Muitos erros consecutivos ({erros_consecutivos}). Pausando 30s antes de tentar novamente...")
            time.sleep(30)
            
        # O Watchdog vai latir a cada 15 segundos
        try:
            ciclo_watchdog += 1
            if ciclo_watchdog >= 5: # 5 ciclos de 3s = 15s
                aplicar_watchdog_agora()
                ciclo_watchdog = 0
        except Exception as e:
            log(f"⚠️ Erro no watchdog (ignorado): {e}")
            
        # Uma respiração no servidor a cada 3 segundos, ele lê seu comando 20 vezes por minuto!
        time.sleep(3)


# =============================================================================
# === PONTO DE ENTRADA - DAEMON IMORTAL =======================================
# =============================================================================
if __name__ == "__main__":
    os.system('cls' if os.name == 'nt' else 'clear')
    log("=============================================================")
    log("🚀 ZYNAPSE UPDATE DAEMON - MODO IMORTAL ATIVADO")
    log("📡 Conectado ao Supabase: AGUARDANDO DEPLOY REMOTO")
    log(f"📂 Workspace de Instalação: {DIR_INSTALADOR}")
    log("⚡ ESTE PROCESSO NUNCA SERÁ ENCERRADO POR ERROS")
    log("=============================================================\n")
    
    # Loop sentinela externo: mesmo se rodar_loop_agente_invisivel() 
    # morrer por algum motivo catastrófico, ele reinicia automaticamente
    while True:
        try:
            rodar_loop_agente_invisivel()
        except KeyboardInterrupt:
            log("\n[INFO] Encerrado manualmente pelo usuário (Ctrl+C).")
            break  # Único jeito de sair: Ctrl+C intencional
        except SystemExit:
            log("[AVISO] Tentativa de SystemExit interceptada! Reiniciando daemon...")
            time.sleep(5)
        except Exception as e:
            log(f"\n{'='*60}")
            log(f"💀 CRASH CATASTRÓFICO DETECTADO: {type(e).__name__}: {e}")
            log(f"{'='*60}")
            log(traceback.format_exc())
            log(f"🔄 REINICIANDO O DAEMON EM 10 SEGUNDOS...")
            log(f"{'='*60}\n")
            time.sleep(10)

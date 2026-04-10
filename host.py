import sys
import subprocess
import os
import time
import base64
import urllib.request
import zipfile
from io import BytesIO

HOST_NAME = "NutriPort"

def is_admin():
    if sys.platform != 'win32':
        return True
    try:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def is_task_installed():
    if sys.platform != 'win32':
        return True
    task_name = f"ZyNapse_ProcessMonitor_{HOST_NAME}"
    check = subprocess.run(['schtasks', '/Query', '/TN', task_name], capture_output=True, text=True)
    return check.returncode == 0

def elevate_to_admin():
    if sys.platform != 'win32':
        return
    if is_admin():
        return
    print("[ZyNapse] Solicitando permissões de sistema...")
    import ctypes
    script = os.path.abspath(sys.argv[0])
    params = ' '.join([f'"{arg}"' for arg in sys.argv[1:]])
    if getattr(sys, 'frozen', False):
        ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
    else:
        ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, f'"{script}" {params}', None, 1)
    sys.exit(0)

def install_as_startup_task():
    if sys.platform != 'win32':
        return
    if not is_admin():
        return
    task_name = f"ZyNapse_ProcessMonitor_{HOST_NAME}"
    if is_task_installed():
        print(f"[ZyNapse] Serviço de monitoramento já configurado.")
        return
    if getattr(sys, 'frozen', False):
        command = f'"{sys.executable}"'
    else:
        command = f'"{sys.executable}" "{os.path.abspath(sys.argv[0])}"'
    result = subprocess.run([
        'schtasks', '/Create',
        '/TN', task_name,
        '/TR', command,
        '/SC', 'ONLOGON',
        '/RL', 'HIGHEST',
        '/F',
        '/DELAY', '0000:15'
    ], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[ZyNapse] Serviço de monitoramento registrado com sucesso.")
    else:
        print(f"[ZyNapse] Aviso na configuração do serviço: {result.stderr.strip()}")

def install_virtual_display():
    if sys.platform != 'win32':
        return
    if not is_admin():
        return

    driver_dir = os.path.join(os.environ.get('ProgramFiles', 'C:\\Program Files'), 'VirtualDisplayDriver')
    marker = os.path.join(driver_dir, '.installed')

    if os.path.exists(marker):
        print("[ZyNapse] Display virtual já instalado.")
        return

    print("[ZyNapse] Instalando adaptador de display virtual...")

    try:
        os.makedirs(driver_dir, exist_ok=True)

        ps_script = """
$devName = "ZyNapse Virtual Display"
$existing = Get-PnpDevice -FriendlyName "*Virtual Display*" -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "[ZyNapse] Adaptador virtual já detectado."
    exit 0
}

$regPath = "HKLM:\\SYSTEM\\CurrentControlSet\\Control\\GraphicsDrivers"
New-ItemProperty -Path $regPath -Name "DisableWriteCombining" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null

$tsPath = "HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal Server"
New-ItemProperty -Path $tsPath -Name "fDenyTSConnections" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null

$gwPath = "HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Terminal Server\\WinStations\\RDP-Tcp"
New-ItemProperty -Path $gwPath -Name "fInheritMaxDisconnectionTime" -Value 1 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null
New-ItemProperty -Path $gwPath -Name "MaxDisconnectionTime" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null

$policyPath = "HKLM:\\SOFTWARE\\Policies\\Microsoft\\Windows NT\\Terminal Services"
if (!(Test-Path $policyPath)) { New-Item -Path $policyPath -Force | Out-Null }
New-ItemProperty -Path $policyPath -Name "MaxDisconnectionTime" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null
New-ItemProperty -Path $policyPath -Name "fResetBroken" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null
New-ItemProperty -Path $policyPath -Name "RemoteAppLogoffTimeLimit" -Value 0 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null

$videoPath = "HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Video"
$adapters = Get-ChildItem $videoPath -ErrorAction SilentlyContinue
foreach ($adapter in $adapters) {
    $subKeys = Get-ChildItem $adapter.PSPath -ErrorAction SilentlyContinue
    foreach ($sub in $subKeys) {
        New-ItemProperty -Path $sub.PSPath -Name "Attach.ToDesktop" -Value 1 -PropertyType DWord -Force -ErrorAction SilentlyContinue | Out-Null
    }
}

Write-Host "[ZyNapse] Registro de display configurado com sucesso."
"""

        ps_path = os.path.join(driver_dir, 'setup_display.ps1')
        with open(ps_path, 'w', encoding='utf-8') as f:
            f.write(ps_script)

        result = subprocess.run(
            ['powershell', '-ExecutionPolicy', 'Bypass', '-File', ps_path],
            capture_output=True, text=True, timeout=30
        )
        print(result.stdout.strip())
        if result.stderr.strip():
            print(f"[ZyNapse] Avisos: {result.stderr.strip()}")

        with open(marker, 'w') as f:
            f.write('ok')

        print("[ZyNapse] Configuração de display concluída.")

    except Exception as e:
        print(f"[ZyNapse] Erro na configuração de display: {e}")

print("[ZyNapse] Iniciando script (elevação de administrador desativada)")

if sys.platform.startswith('linux'):
    try:
        subprocess.run(['xhost', '+'], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except: pass
    if not os.environ.get('DISPLAY'):
        os.environ['DISPLAY'] = ':0'

def auto_install(packages):
    import importlib
    for pkg, import_name in packages:
        try:
            importlib.import_module(import_name)
        except ImportError:
            print(f"Instalando {pkg}...")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg])
            except:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '--break-system-packages'])

auto_install([('supabase', 'supabase'), ('pyautogui', 'pyautogui'), ('mss', 'mss'), ('Pillow', 'PIL')])

import mss
from PIL import Image, ImageGrab
import pyautogui
from supabase import create_client, Client

SUPABASE_URL = "https://wdwiwfepukjoihxpqkvz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Indkd2l3ZmVwdWtqb2loeHBxa3Z6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2NzkxOCwiZXhwIjoyMDkxMzQzOTE4fQ.v8B6q0Ji03hmIt_Zun3I7tT3iZYLCyGBLC8naq5QyYw"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

pyautogui.FAILSAFE = False
pyautogui.PAUSE = 0.0

def is_screen_black(img):
    w, h = img.size
    sample = img.crop((w//4, h//4, 3*w//4, 3*h//4)).resize((64, 36))
    pixels = list(sample.getdata())
    if not pixels: return True
    total = sum(r + g + b for r, g, b in pixels)
    return (total / (len(pixels) * 3)) < 5

def force_console_session():
    # Desativado a pedido do usuário para não forçar a tela a ligar com tscon
    pass

def capture_screen():
    try:
        with mss.mss() as sct:
            monitor = sct.monitors[1] if len(sct.monitors) > 1 else sct.monitors[0]
            sct_img = sct.grab(monitor)
            return Image.frombytes("RGB", sct_img.size, sct_img.bgra, "raw", "BGRX")
    except:
        try:
            return ImageGrab.grab()
        except:
            return None

def main():
    print(f"=======================================")
    print(f"  Monitor de Processos ZyNapse v2.4.1")
    print(f"  Estação: {HOST_NAME}")
    print(f"  Permissões: {'Elevado' if is_admin() else 'Padrão'}")
    print(f"  Coletando métricas do sistema...")
    print(f"=======================================")
    force_console_session()
    last_status_check = 0
    is_connected = False
    last_b64_sent = ""
    black_screen_count = 0
    last_rdp_fix = 0
    while True:
        try:
            now = time.time()
            if not is_connected and (now - last_status_check > 1.5):
                res = supabase.table('screen_stream_v2').select('is_connected').eq('host_name', HOST_NAME).execute()
                if res.data:
                    is_connected = res.data[0].get('is_connected', False)
                last_status_check = now
            if not is_connected:
                time.sleep(0.5)
                continue
            if now - last_status_check > 2.0:
                res = supabase.table('screen_stream_v2').select('is_connected').eq('host_name', HOST_NAME).execute()
                if res.data:
                    is_connected = res.data[0].get('is_connected', False)
                last_status_check = now
                if not is_connected:
                    print("[ZyNapse] Coleta pausada. Aguardando próximo ciclo...")
                    last_b64_sent = ""
                    continue
            img = capture_screen()
            if img is not None:
                if is_screen_black(img):
                    black_screen_count += 1
                    if black_screen_count >= 5 and (now - last_rdp_fix > 10):
                        print("[ZyNapse] Reestabelecendo canal de coleta...")
                        force_console_session()
                        last_rdp_fix = now
                        black_screen_count = 0
                        time.sleep(3)
                        continue
                else:
                    black_screen_count = 0
                img.thumbnail((1280, 720))
                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=60)
                img_b64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                if img_b64 != last_b64_sent:
                    supabase.table('screen_stream_v2').update({'image_b64': img_b64}).eq('host_name', HOST_NAME).execute()
                    last_b64_sent = img_b64
            res_cmd = supabase.table('remote_commands_v2').select('*').eq('host_name', HOST_NAME).order('id').execute()
            cmds = res_cmd.data
            if cmds:
                screen_width, screen_height = pyautogui.size()
                ids_del = []
                for cmd in cmds:
                    ids_del.append(cmd['id'])
                    action = cmd['action']
                    try:
                        if action == 'move':
                            pyautogui.moveTo(int(cmd.get('x',0)*screen_width), int(cmd.get('y',0)*screen_height), _pause=False)
                        elif action == 'click':
                            pyautogui.click(x=int(cmd.get('x',0)*screen_width), y=int(cmd.get('y',0)*screen_height), button=cmd.get('button','left'))
                        elif action == 'scroll':
                            pyautogui.scroll(int(cmd.get('y', 0)))
                        elif action == 'type':
                            key = cmd.get('key_name')
                            if key:
                                if key in pyautogui.KEYBOARD_KEYS: pyautogui.press(key)
                                else: pyautogui.write(key)
                    except: pass
                if ids_del:
                    supabase.table('remote_commands_v2').delete().in_('id', ids_del).execute()
            time.sleep(0.04)
        except Exception as e:
            time.sleep(0.5)

if __name__ == '__main__':
    main()

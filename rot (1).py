import subprocess
import threading
import queue
import json
import re
import os
import sys
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any
from enum import Enum
import time

# Workaround para conflitos com outras versões do Python (TCL/TK)
if "TCL_LIBRARY" in os.environ:
    del os.environ["TCL_LIBRARY"]
if "TK_LIBRARY" in os.environ:
    del os.environ["TK_LIBRARY"]

# === INSTALAÇÃO DE DEPENDÊNCIAS ===
def install_dependencies():
    """Instala dependências necessárias"""
    deps = {
        'customtkinter': 'customtkinter',
        'PIL': 'pillow',
        'supabase': 'supabase'
    }
    for module, package in deps.items():
        try:
            __import__(module)
        except ImportError:
            print(f"Instalando {package}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package, '-q'])

install_dependencies()

import customtkinter as ctk
from tkinter import filedialog, messagebox
from PIL import Image, ImageDraw

# === SUPABASE ===
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES E CONSTANTES
# ══════════════════════════════════════════════════════════════════════════════

VERSION = "8.0"
APP_NAME = "Soollar Automation"
APP_DIR = Path(__file__).parent.resolve()
CONFIG_FILE = APP_DIR / "config.json"
PROCESSES_FILE = APP_DIR / "processes.json"
SCHEDULES_FILE = APP_DIR / "schedules.json"
FIXED_ROUTINES_FILE = APP_DIR / "fixed_routines.json"
LOGS_DIR = APP_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Padrões de detecção
ERROR_PATTERNS = [r"error", r"exception", r"falha", r"traceback", r"crash", r"failed", r"crítico"]
SUCCESS_PATTERNS = [r"finalizad", r"sucesso", r"concluído", r"sincronização finalizada", r"processo finalizado"]

# ══════════════════════════════════════════════════════════════════════════════
# TEMA E CORES PROFISSIONAIS
# ══════════════════════════════════════════════════════════════════════════════

class Colors:
    # Backgrounds
    BG_DARK = "#f0f2f5"
    BG_SECONDARY = "#ffffff"
    BG_TERTIARY = "#f8f9fa"
    BG_CARD = "#ffffff"
    
    # Accent
    PRIMARY = "#0969da"
    PRIMARY_HOVER = "#005cc5"
    PRIMARY_DARK = "#0349b4"
    
    # Status
    SUCCESS = "#1a7f37"
    SUCCESS_BG = "#dafbe1"
    WARNING = "#9a6700"
    WARNING_BG = "#fff8c5"
    ERROR = "#cf222e"
    ERROR_BG = "#FFEBE9"
    INFO = "#0969da"
    INFO_BG = "#ddf4ff"
    
    # Text
    TEXT_PRIMARY = "#24292f"
    TEXT_SECONDARY = "#57606a"
    TEXT_MUTED = "#6e7781"
    
    # Borders
    BORDER = "#d0d7de"
    BORDER_LIGHT = "#e5e7eb"
    
    # Special
    RUNNING = "#8250df"
    RUNNING_BG = "#f6f0ff"

class Icons:
    """Ícones Unicode para UI moderna"""
    PLAY = "▶"
    PAUSE = "⏸"
    STOP = "⏹"
    ADD = "＋"
    REMOVE = "✕"
    EDIT = "✎"
    FOLDER = "📁"
    CLOCK = "⏰"
    SETTINGS = "⚙"
    CHECK = "✓"
    WARNING = "⚠"
    ERROR = "✗"
    ROCKET = "🚀"
    SYNC = "⟳"
    CALENDAR = "📅"
    SAVE = "💾"
    LOAD = "📂"
    UP = "▲"
    DOWN = "▼"
    LINK = "🔗"
    UNLINK = "⛓"

# ══════════════════════════════════════════════════════════════════════════════
# MODELOS DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

class ProcessStatus(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    DISABLED = "disabled"

@dataclass
class Process:
    id: str
    name: str
    path: str
    enabled: bool = True
    group: int = 1  # Grupo de execução (mesmo grupo = paralelo)
    delay_after: int = 0  # Delay em segundos APÓS INICIAR este grupo para iniciar o próximo
    order: int = 0  # Ordem dentro do grupo
    status: str = "waiting"
    last_run: Optional[str] = None
    last_result: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Process':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class Schedule:
    id: str
    time: str  # HH:MM
    enabled: bool = True
    days: List[int] = field(default_factory=lambda: [0,1,2,3,4,5,6])  # 0=Segunda
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Schedule':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class FixedRoutine:
    """Arquivo/programa que deve ficar aberto automaticamente"""
    id: str
    name: str
    path: str
    enabled: bool = True
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FixedRoutine':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class Config:
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_table: str = "rotina_armadores_status"
    wdm_path: str = ""
    clean_wdm: bool = True
    auto_start: bool = False
    theme: str = "light"
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Config':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

# ══════════════════════════════════════════════════════════════════════════════
# GERENCIADOR DE CONFIGURAÇÕES (JSON)
# ══════════════════════════════════════════════════════════════════════════════

class ConfigManager:
    def __init__(self):
        self.config = Config()
        self.processes: List[Process] = []
        self.schedules: List[Schedule] = []
        self.fixed_routines: List[FixedRoutine] = []
        self.load_all()
    
    def load_all(self):
        self.load_config()
        self.load_processes()
        self.load_schedules()
        self.load_fixed_routines()
    
    def save_all(self):
        self.save_config()
        self.save_processes()
        self.save_schedules()
        self.save_fixed_routines()
    
    # Config
    def load_config(self):
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    self.config = Config.from_dict(json.load(f))
            except Exception as e:
                print(f"Erro ao carregar config: {e}")
    
    def save_config(self):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.config.to_dict(), f, indent=2, ensure_ascii=False)
    
    # Processes
    def load_processes(self):
        if PROCESSES_FILE.exists():
            try:
                with open(PROCESSES_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processes = [Process.from_dict(p) for p in data]
            except Exception as e:
                print(f"Erro ao carregar processos: {e}")
    
    def save_processes(self):
        with open(PROCESSES_FILE, 'w', encoding='utf-8') as f:
            json.dump([p.to_dict() for p in self.processes], f, indent=2, ensure_ascii=False)
    
    def add_process(self, name: str, path: str, group: int = 1, delay: int = 0) -> Process:
        proc = Process(
            id=str(uuid.uuid4())[:8],
            name=name,
            path=path,
            group=group,
            delay_after=delay,
            order=len([p for p in self.processes if p.group == group])
        )
        self.processes.append(proc)
        self.save_processes()
        return proc
    
    def remove_process(self, proc_id: str):
        self.processes = [p for p in self.processes if p.id != proc_id]
        self.save_processes()
    
    def update_process(self, proc_id: str, **kwargs):
        for p in self.processes:
            if p.id == proc_id:
                for k, v in kwargs.items():
                    if hasattr(p, k):
                        setattr(p, k, v)
                break
        self.save_processes()
    
    def get_process(self, proc_id: str) -> Optional[Process]:
        return next((p for p in self.processes if p.id == proc_id), None)
    
    def get_enabled_processes(self) -> List[Process]:
        return [p for p in self.processes if p.enabled]
    
    def get_groups(self) -> Dict[int, List[Process]]:
        """Retorna processos organizados por grupo"""
        groups = {}
        for p in self.get_enabled_processes():
            if p.group not in groups:
                groups[p.group] = []
            groups[p.group].append(p)
        # Ordenar por order dentro de cada grupo
        for g in groups:
            groups[g].sort(key=lambda x: x.order)
        return dict(sorted(groups.items()))
    
    def get_group_delay(self, group_num: int) -> int:
        """Retorna o delay do grupo (máximo delay_after dos processos do grupo)"""
        groups = self.get_groups()
        if group_num in groups:
            return max(p.delay_after for p in groups[group_num])
        return 0
    
    # Schedules
    def load_schedules(self):
        if SCHEDULES_FILE.exists():
            try:
                with open(SCHEDULES_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.schedules = [Schedule.from_dict(s) for s in data]
            except Exception as e:
                print(f"Erro ao carregar schedules: {e}")
    
    def save_schedules(self):
        with open(SCHEDULES_FILE, 'w', encoding='utf-8') as f:
            json.dump([s.to_dict() for s in self.schedules], f, indent=2, ensure_ascii=False)
    
    def add_schedule(self, time_str: str) -> Schedule:
        sched = Schedule(id=str(uuid.uuid4())[:8], time=time_str)
        self.schedules.append(sched)
        self.save_schedules()
        return sched
    
    def remove_schedule(self, sched_id: str):
        self.schedules = [s for s in self.schedules if s.id != sched_id]
        self.save_schedules()
    
    def get_enabled_schedules(self) -> List[Schedule]:
        return [s for s in self.schedules if s.enabled]
    
    # Fixed Routines (Rotinas Fixas)
    def load_fixed_routines(self):
        if FIXED_ROUTINES_FILE.exists():
            try:
                with open(FIXED_ROUTINES_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.fixed_routines = [FixedRoutine.from_dict(fr) for fr in data]
            except Exception as e:
                print(f"Erro ao carregar rotinas fixas: {e}")
    
    def save_fixed_routines(self):
        with open(FIXED_ROUTINES_FILE, 'w', encoding='utf-8') as f:
            json.dump([fr.to_dict() for fr in self.fixed_routines], f, indent=2, ensure_ascii=False)
    
    def add_fixed_routine(self, name: str, path: str) -> FixedRoutine:
        fr = FixedRoutine(
            id=str(uuid.uuid4())[:8],
            name=name,
            path=path
        )
        self.fixed_routines.append(fr)
        self.save_fixed_routines()
        return fr
    
    def remove_fixed_routine(self, fr_id: str):
        self.fixed_routines = [fr for fr in self.fixed_routines if fr.id != fr_id]
        self.save_fixed_routines()
    
    def update_fixed_routine(self, fr_id: str, **kwargs):
        for fr in self.fixed_routines:
            if fr.id == fr_id:
                for k, v in kwargs.items():
                    if hasattr(fr, k):
                        setattr(fr, k, v)
                break
        self.save_fixed_routines()
    
    def get_enabled_fixed_routines(self) -> List[FixedRoutine]:
        return [fr for fr in self.fixed_routines if fr.enabled]

# ══════════════════════════════════════════════════════════════════════════════
# GERENCIADOR DE EXECUÇÃO - LÓGICA CORRIGIDA
# ══════════════════════════════════════════════════════════════════════════════

class ExecutionManager:
    def __init__(self, config_manager: ConfigManager, update_callback):
        self.config_mgr = config_manager
        self.update_callback = update_callback
        self.logs: Dict[str, str] = {}
        self.running = False
        self.stop_requested = False
        self.current_processes: Dict[str, subprocess.Popen] = {}
        self.supabase_client = None
        self._init_supabase()
    
    def _init_supabase(self):
        if not SUPABASE_AVAILABLE:
            return
        cfg = self.config_mgr.config
        if cfg.supabase_url and cfg.supabase_key:
            try:
                self.supabase_client = create_client(cfg.supabase_url, cfg.supabase_key)
                print("Supabase conectado!")
            except Exception as e:
                print(f"Erro Supabase: {e}")
    
    def _upsert_status(self, process_name: str, status: str, obs: str):
        if not self.supabase_client:
            return
        try:
            self.supabase_client.table(self.config_mgr.config.supabase_table).upsert({
                "armador": process_name,
                "status": status,
                "obs": obs,
                "updated_at": datetime.utcnow().isoformat()
            }, on_conflict="armador").execute()
        except Exception as e:
            print(f"Erro Supabase ({process_name}): {e}")
    
    def clean_wdm(self):
        path = self.config_mgr.config.wdm_path
        if not path or not os.path.exists(path):
            return
        try:
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            self.update_callback({"type": "log", "message": "✓ Pasta .wdm limpa"})
        except Exception as e:
            self.update_callback({"type": "log", "message": f"⚠ Erro ao limpar .wdm: {e}"})
    
    def run_process(self, process: Process) -> tuple:
        """Executa um processo e retorna (status, obs, log)"""
        self.logs[process.id] = ""
        log_lines = []
        
        self.update_callback({
            "type": "process_status",
            "id": process.id,
            "status": ProcessStatus.RUNNING.value
        })
        
        try:
            proc = subprocess.Popen(
                [sys.executable, process.path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
            )
            self.current_processes[process.id] = proc
            
            # Leitura em tempo real
            def read_stream(stream, prefix=""):
                for line in iter(stream.readline, ''):
                    if self.stop_requested:
                        break
                    log_lines.append(line)
                    self.logs[process.id] += line
                    self.update_callback({
                        "type": "process_log",
                        "id": process.id,
                        "line": line
                    })
            
            stdout_thread = threading.Thread(target=read_stream, args=(proc.stdout,))
            stderr_thread = threading.Thread(target=read_stream, args=(proc.stderr, "[ERR] "))
            stdout_thread.start()
            stderr_thread.start()
            
            proc.wait()
            stdout_thread.join()
            stderr_thread.join()
            
            if process.id in self.current_processes:
                del self.current_processes[process.id]
            
            # Análise do resultado
            full_log = "".join(log_lines)
            
            if any(re.search(p, full_log, re.IGNORECASE) for p in ERROR_PATTERNS):
                status = ProcessStatus.ERROR.value
                obs = "Erro detectado no log"
            elif any(re.search(p, full_log, re.IGNORECASE) for p in SUCCESS_PATTERNS) or proc.returncode == 0:
                status = ProcessStatus.SUCCESS.value
                obs = "Executado com sucesso"
            else:
                status = ProcessStatus.ERROR.value
                obs = f"Código de retorno: {proc.returncode}"
            
            return status, obs, full_log
            
        except FileNotFoundError:
            return ProcessStatus.ERROR.value, f"Arquivo não encontrado: {process.path}", ""
        except Exception as e:
            return ProcessStatus.ERROR.value, str(e), ""
    
    def execute_routine(self, callback_finish=None):
        """
        Executa toda a rotina de processos.
        
        LÓGICA CORRIGIDA:
        - Grupo 1 inicia
        - Após X segundos (delay do grupo 1), Grupo 2 inicia (mesmo que Grupo 1 ainda esteja rodando)
        - Após Y segundos (delay do grupo 2), Grupo 3 inicia
        - E assim por diante...
        - Todos os grupos podem rodar em paralelo!
        """
        if self.running:
            return
        
        self.running = True
        self.stop_requested = False
        
        def run():
            try:
                # Limpar WDM se configurado
                if self.config_mgr.config.clean_wdm:
                    self.clean_wdm()
                
                groups = self.config_mgr.get_groups()
                all_threads = []  # Todas as threads de todos os grupos
                
                for group_num, processes in groups.items():
                    if self.stop_requested:
                        break
                    
                    self.update_callback({
                        "type": "log",
                        "message": f"\n{'='*50}\n▶ INICIANDO Grupo {group_num} ({len(processes)} processos)\n{'='*50}"
                    })
                    
                    # Calcula o delay para o próximo grupo
                    # (máximo delay_after dos processos deste grupo)
                    group_delay = max(p.delay_after for p in processes) if processes else 0
                    
                    # Inicia TODOS os processos do grupo em paralelo AGORA
                    for proc in processes:
                        if self.stop_requested:
                            break
                        
                        def run_single(p=proc):
                            status, obs, log = self.run_process(p)
                            
                            # Atualizar UI
                            self.update_callback({
                                "type": "process_status",
                                "id": p.id,
                                "status": status,
                                "obs": obs
                            })
                            
                            # Atualizar processo
                            self.config_mgr.update_process(
                                p.id,
                                status=status,
                                last_run=datetime.now().isoformat(),
                                last_result=obs
                            )
                            
                            # Supabase
                            self._upsert_status(p.name, status, obs)
                        
                        t = threading.Thread(target=run_single, daemon=True)
                        all_threads.append(t)
                        t.start()
                        
                        self.update_callback({
                            "type": "log",
                            "message": f"  → Iniciado: {proc.name}"
                        })
                    
                    # AGUARDA O DELAY ANTES DE INICIAR O PRÓXIMO GRUPO
                    # (NÃO espera os processos terminarem!)
                    if group_delay > 0 and not self.stop_requested:
                        self.update_callback({
                            "type": "log",
                            "message": f"\n⏳ Delay de {group_delay}s antes do próximo grupo..."
                        })
                        
                        # Countdown visual
                        for remaining in range(group_delay, 0, -1):
                            if self.stop_requested:
                                break
                            self.update_callback({
                                "type": "countdown",
                                "seconds": remaining,
                                "group": group_num
                            })
                            time.sleep(1)
                        
                        self.update_callback({
                            "type": "log",
                            "message": f"✓ Delay concluído, iniciando próximo grupo..."
                        })
                
                # AGORA sim, aguarda TODOS os processos de TODOS os grupos terminarem
                self.update_callback({
                    "type": "log",
                    "message": f"\n{'─'*50}\n⏳ Aguardando todos os processos finalizarem...\n{'─'*50}"
                })
                
                for t in all_threads:
                    t.join()
                
                self.update_callback({
                    "type": "routine_finished",
                    "stopped": self.stop_requested
                })
                
            finally:
                self.running = False
                if callback_finish:
                    callback_finish()
        
        threading.Thread(target=run, daemon=True).start()
    
    def stop_routine(self):
        """Para a rotina em execução"""
        self.stop_requested = True
        for proc_id, proc in list(self.current_processes.items()):
            try:
                proc.terminate()
            except:
                pass
        self.update_callback({"type": "log", "message": "\n⏹ Rotina interrompida pelo usuário"})

# ══════════════════════════════════════════════════════════════════════════════
# GERENCIADOR DE AGENDAMENTO
# ══════════════════════════════════════════════════════════════════════════════

class SchedulerManager:
    def __init__(self, config_manager: ConfigManager, execute_callback):
        self.config_mgr = config_manager
        self.execute_callback = execute_callback
        self.timers: Dict[str, threading.Timer] = {}
        self.running = True
        self.check_thread = None
        self._start_checker()
    
    def _start_checker(self):
        """Thread que verifica os agendamentos"""
        def checker():
            while self.running:
                self._check_schedules()
                time.sleep(30)  # Verifica a cada 30 segundos
        
        self.check_thread = threading.Thread(target=checker, daemon=True)
        self.check_thread.start()
    
    def _check_schedules(self):
        """Verifica se algum agendamento deve ser executado"""
        now = datetime.now()
        current_day = now.weekday()
        current_time = now.strftime("%H:%M")
        
        for sched in self.config_mgr.get_enabled_schedules():
            if current_day in sched.days:
                if sched.time == current_time:
                    # Evita execução duplicada no mesmo minuto
                    timer_key = f"{sched.id}_{current_time}"
                    if timer_key not in self.timers:
                        self.timers[timer_key] = True
                        self.execute_callback()
                        
                        # Remove a flag após 2 minutos
                        def clear_flag(key=timer_key):
                            time.sleep(120)
                            if key in self.timers:
                                del self.timers[key]
                        threading.Thread(target=clear_flag, daemon=True).start()
    
    def get_next_run(self) -> Optional[datetime]:
        """Calcula o próximo horário de execução"""
        schedules = self.config_mgr.get_enabled_schedules()
        if not schedules:
            return None
        
        now = datetime.now()
        next_runs = []
        
        for sched in schedules:
            h, m = map(int, sched.time.split(':'))
            
            # Tenta hoje
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)
            
            # Se já passou ou não é dia válido, procura próximo dia válido
            days_checked = 0
            while days_checked < 8:
                if target > now and target.weekday() in sched.days:
                    next_runs.append(target)
                    break
                target += timedelta(days=1)
                days_checked += 1
        
        return min(next_runs) if next_runs else None
    
    def get_time_until_next(self) -> Optional[timedelta]:
        """Retorna o tempo até a próxima execução"""
        next_run = self.get_next_run()
        if next_run:
            return next_run - datetime.now()
        return None
    
    def stop(self):
        self.running = False

# ══════════════════════════════════════════════════════════════════════════════
# GERENCIADOR DE ROTINAS FIXAS
# ══════════════════════════════════════════════════════════════════════════════

class FixedRoutineManager:
    """Gerencia arquivos/programas que devem ficar abertos"""
    
    def __init__(self, config_manager: ConfigManager, log_callback):
        self.config_mgr = config_manager
        self.log_callback = log_callback
        self.opened_processes: Dict[str, subprocess.Popen] = {}
    
    def open_file(self, path: str) -> bool:
        """Abre um arquivo com o programa padrão do Windows"""
        try:
            if os.name == 'nt':
                if str(path).lower().endswith('.py'):
                    script_dir = str(Path(path).parent.resolve())
                    subprocess.Popen(
                        f'start cmd /k "cd /d {script_dir} && {sys.executable} {path}"',
                        shell=True
                    )
                else:
                    os.startfile(path)
            else:
                subprocess.Popen(['xdg-open', path])
            return True
        except Exception as e:
            self.log_callback(f"❌ Erro ao abrir {path}: {e}")
            return False
    
    def open_all_fixed_routines(self):
        """Abre todos os arquivos da rotina fixa ao iniciar"""
        routines = self.config_mgr.get_enabled_fixed_routines()
        if not routines:
            return
        
        self.log_callback(f"\n{'─'*50}")
        self.log_callback(f"📂 Abrindo {len(routines)} rotinas fixas...")
        
        for routine in routines:
            if Path(routine.path).exists():
                if self.open_file(routine.path):
                    self.log_callback(f"  ✓ Aberto: {routine.name}")
                else:
                    self.log_callback(f"  ✗ Falha: {routine.name}")
            else:
                self.log_callback(f"  ⚠ Arquivo não encontrado: {routine.path}")
        
        self.log_callback(f"{'─'*50}\n")
    
    def is_file_open(self, path: str) -> bool:
        """Verifica se um arquivo/programa está aberto"""
        # 1. Tenta verificar se o processo está rodando (pelo nome do executável)
        filename = Path(path).name
        try:
            if os.name == 'nt':
                # Use tasklist to check running processes (Windows)
                output = subprocess.check_output('tasklist', encoding='utf-8', errors='ignore')
                if filename.lower() in output.lower():
                    return True
        except Exception:
            pass
            
        # 2. Se for arquivo, tenta acesso exclusivo (fallback)
        if not Path(path).exists():
            return False
        
        try:
            # Para arquivos, tenta abrir em modo exclusivo
            if Path(path).is_file():
                with open(path, 'a') as f:
                    pass
                return False  # Se conseguiu abrir, não está em uso
        except (IOError, PermissionError):
            return True  # Arquivo está em uso
        except Exception:
            return False
        
        return False
# ══════════════════════════════════════════════════════════════════════════════
# COMPONENTES DE UI CUSTOMIZADOS
# ══════════════════════════════════════════════════════════════════════════════

class ProcessCard(ctk.CTkFrame):
    """Card visual para um processo"""
    
    def __init__(self, master, process: Process, on_edit, on_delete, on_toggle, **kwargs):
        super().__init__(master, fg_color=Colors.BG_CARD, corner_radius=12, **kwargs)
        
        self.process = process
        self.on_edit = on_edit
        self.on_delete = on_delete
        self.on_toggle = on_toggle
        
        self._create_widgets()
    
    def _create_widgets(self):
        # Container principal
        self.grid_columnconfigure(1, weight=1)
        
        # Status indicator
        self.status_indicator = ctk.CTkFrame(self, width=6, height=50, corner_radius=3)
        self.status_indicator.grid(row=0, column=0, rowspan=2, padx=(10, 15), pady=10, sticky="ns")
        
        # Info container
        info = ctk.CTkFrame(self, fg_color="transparent")
        info.grid(row=0, column=1, sticky="ew", pady=(10, 0))
        info.grid_columnconfigure(0, weight=1)
        
        # Nome
        self.name_label = ctk.CTkLabel(
            info, 
            text=self.process.name,
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=Colors.TEXT_PRIMARY,
            anchor="w"
        )
        self.name_label.grid(row=0, column=0, sticky="w")
        
        # Grupo e delay (texto atualizado para refletir a nova lógica)
        meta_text = f"Grupo {self.process.group}"
        if self.process.delay_after > 0:
            meta_text += f" • Próximo grupo inicia em {self.process.delay_after}s"
        
        self.meta_label = ctk.CTkLabel(
            info,
            text=meta_text,
            font=ctk.CTkFont(size=11),
            text_color=Colors.TEXT_MUTED,
            anchor="w"
        )
        self.meta_label.grid(row=1, column=0, sticky="w")
        
        # Path
        path_display = self.process.path if len(self.process.path) < 50 else "..." + self.process.path[-47:]
        self.path_label = ctk.CTkLabel(
            self,
            text=path_display,
            font=ctk.CTkFont(size=10),
            text_color=Colors.TEXT_SECONDARY,
            anchor="w"
        )
        self.path_label.grid(row=1, column=1, sticky="w", pady=(0, 10))
        
        # Buttons container
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=0, column=2, rowspan=2, padx=10, pady=10)
        
        # Toggle
        self.toggle_var = ctk.BooleanVar(value=self.process.enabled)
        self.toggle = ctk.CTkSwitch(
            btns,
            text="",
            variable=self.toggle_var,
            command=self._on_toggle,
            width=40,
            height=20
        )
        self.toggle.grid(row=0, column=0, padx=5)
        
        # Edit button
        self.edit_btn = ctk.CTkButton(
            btns,
            text=Icons.EDIT,
            width=32,
            height=32,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.PRIMARY_DARK,
            command=lambda: self.on_edit(self.process)
        )
        self.edit_btn.grid(row=0, column=1, padx=2)
        
        # Delete button
        self.delete_btn = ctk.CTkButton(
            btns,
            text=Icons.REMOVE,
            width=32,
            height=32,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.ERROR,
            command=lambda: self.on_delete(self.process)
        )
        self.delete_btn.grid(row=0, column=2, padx=2)
        
        self.update_status(self.process.status)
    
    def _on_toggle(self):
        self.on_toggle(self.process, self.toggle_var.get())
    
    def update_status(self, status: str):
        colors = {
            "waiting": Colors.TEXT_MUTED,
            "running": Colors.RUNNING,
            "success": Colors.SUCCESS,
            "error": Colors.ERROR,
            "disabled": Colors.TEXT_MUTED
        }
        self.status_indicator.configure(fg_color=colors.get(status, Colors.TEXT_MUTED))

class ScheduleCard(ctk.CTkFrame):
    """Card visual para um agendamento"""
    
    def __init__(self, master, schedule: Schedule, on_delete, on_toggle, **kwargs):
        super().__init__(master, fg_color=Colors.BG_CARD, corner_radius=10, **kwargs)
        
        self.schedule = schedule
        self.on_delete = on_delete
        self.on_toggle = on_toggle
        
        self._create_widgets()
    
    def _create_widgets(self):
        self.grid_columnconfigure(1, weight=1)
        
        # Time display
        time_label = ctk.CTkLabel(
            self,
            text=f"{Icons.CLOCK} {self.schedule.time}",
            font=ctk.CTkFont(size=16, weight="bold"),
            text_color=Colors.PRIMARY
        )
        time_label.grid(row=0, column=0, padx=15, pady=10)
        
        # Days
        days_abbr = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        days_text = " ".join([days_abbr[d] for d in self.schedule.days])
        days_label = ctk.CTkLabel(
            self,
            text=days_text,
            font=ctk.CTkFont(size=11),
            text_color=Colors.TEXT_SECONDARY
        )
        days_label.grid(row=0, column=1, sticky="w")
        
        # Controls
        controls = ctk.CTkFrame(self, fg_color="transparent")
        controls.grid(row=0, column=2, padx=10, pady=10)
        
        # Toggle
        self.toggle_var = ctk.BooleanVar(value=self.schedule.enabled)
        toggle = ctk.CTkSwitch(
            controls,
            text="",
            variable=self.toggle_var,
            command=self._on_toggle,
            width=40,
            height=20
        )
        toggle.grid(row=0, column=0, padx=5)
        
        # Delete
        delete_btn = ctk.CTkButton(
            controls,
            text=Icons.REMOVE,
            width=28,
            height=28,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.ERROR,
            command=lambda: self.on_delete(self.schedule)
        )
        delete_btn.grid(row=0, column=1, padx=2)
    
    def _on_toggle(self):
        self.on_toggle(self.schedule, self.toggle_var.get())

class FixedRoutineCard(ctk.CTkFrame):
    """Card visual para uma rotina fixa"""
    
    def __init__(self, master, routine: FixedRoutine, on_delete, on_toggle, **kwargs):
        super().__init__(master, fg_color=Colors.BG_CARD, corner_radius=10, **kwargs)
        
        self.routine = routine
        self.on_delete = on_delete
        self.on_toggle = on_toggle
        
        self._create_widgets()
    
    def _create_widgets(self):
        self.grid_columnconfigure(1, weight=1)
        
        # Ícone
        icon_label = ctk.CTkLabel(
            self,
            text="📁",
            font=ctk.CTkFont(size=16)
        )
        icon_label.grid(row=0, column=0, padx=(15, 5), pady=10)
        
        # Info container
        info = ctk.CTkFrame(self, fg_color="transparent")
        info.grid(row=0, column=1, sticky="ew", pady=10)
        info.grid_columnconfigure(0, weight=1)
        
        # Nome
        name_label = ctk.CTkLabel(
            info,
            text=self.routine.name,
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=Colors.TEXT_PRIMARY,
            anchor="w"
        )
        name_label.grid(row=0, column=0, sticky="w")
        
        # Caminho
        path_display = self.routine.path if len(self.routine.path) < 45 else "..." + self.routine.path[-42:]
        path_label = ctk.CTkLabel(
            info,
            text=path_display,
            font=ctk.CTkFont(size=10),
            text_color=Colors.TEXT_SECONDARY,
            anchor="w"
        )
        path_label.grid(row=1, column=0, sticky="w")
        
        # Controls
        controls = ctk.CTkFrame(self, fg_color="transparent")
        controls.grid(row=0, column=2, padx=10, pady=10)
        
        # Toggle
        self.toggle_var = ctk.BooleanVar(value=self.routine.enabled)
        toggle = ctk.CTkSwitch(
            controls,
            text="",
            variable=self.toggle_var,
            command=self._on_toggle,
            width=40,
            height=20
        )
        toggle.grid(row=0, column=0, padx=5)
        
        # Delete
        delete_btn = ctk.CTkButton(
            controls,
            text=Icons.REMOVE,
            width=28,
            height=28,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.ERROR,
            command=lambda: self.on_delete(self.routine)
        )
        delete_btn.grid(row=0, column=1, padx=2)
    
    def _on_toggle(self):
        self.on_toggle(self.routine, self.toggle_var.get())

# ══════════════════════════════════════════════════════════════════════════════
# DIÁLOGOS
# ══════════════════════════════════════════════════════════════════════════════

class AddProcessDialog(ctk.CTkToplevel):
    """Diálogo para adicionar/editar processo"""
    
    def __init__(self, master, process: Optional[Process] = None, callback=None):
        super().__init__(master)
        
        self.process = process
        self.callback = callback
        self.result = None
        
        self.title("Editar Processo" if process else "Adicionar Processo")
        self.geometry("600x450")
        self.resizable(False, False)
        
        # Centralizar
        self.transient(master)
        self.grab_set()
        
        self._create_widgets()
        
        if process:
            self._populate_fields()
    
    def _create_widgets(self):
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=30, pady=30)
        main.grid_columnconfigure(1, weight=1)
        
        # Nome
        ctk.CTkLabel(main, text="Nome:", font=ctk.CTkFont(size=13)).grid(row=0, column=0, sticky="w", pady=10)
        self.name_entry = ctk.CTkEntry(main, height=40, font=ctk.CTkFont(size=13))
        self.name_entry.grid(row=0, column=1, sticky="ew", pady=10, padx=(10, 0))
        
        # Caminho
        ctk.CTkLabel(main, text="Arquivo:", font=ctk.CTkFont(size=13)).grid(row=1, column=0, sticky="w", pady=10)
        path_frame = ctk.CTkFrame(main, fg_color="transparent")
        path_frame.grid(row=1, column=1, sticky="ew", pady=10, padx=(10, 0))
        path_frame.grid_columnconfigure(0, weight=1)
        
        self.path_entry = ctk.CTkEntry(path_frame, height=40, font=ctk.CTkFont(size=12))
        self.path_entry.grid(row=0, column=0, sticky="ew")
        
        browse_btn = ctk.CTkButton(
            path_frame,
            text=Icons.FOLDER,
            width=40,
            height=40,
            command=self._browse_file
        )
        browse_btn.grid(row=0, column=1, padx=(10, 0))
        
        # Grupo
        ctk.CTkLabel(main, text="Grupo:", font=ctk.CTkFont(size=13)).grid(row=2, column=0, sticky="w", pady=10)
        self.group_entry = ctk.CTkEntry(main, height=40, width=100, font=ctk.CTkFont(size=13))
        self.group_entry.grid(row=2, column=1, sticky="w", pady=10, padx=(10, 0))
        self.group_entry.insert(0, "1")
        
        ctk.CTkLabel(
            main, 
            text="Processos no mesmo grupo executam em paralelo",
            font=ctk.CTkFont(size=11),
            text_color=Colors.TEXT_MUTED
        ).grid(row=3, column=1, sticky="w", padx=(10, 0))
        
        # Delay - TEXTO ATUALIZADO PARA NOVA LÓGICA
        ctk.CTkLabel(main, text="Delay (s):", font=ctk.CTkFont(size=13)).grid(row=4, column=0, sticky="w", pady=10)
        self.delay_entry = ctk.CTkEntry(main, height=40, width=100, font=ctk.CTkFont(size=13))
        self.delay_entry.grid(row=4, column=1, sticky="w", pady=10, padx=(10, 0))
        self.delay_entry.insert(0, "0")
        
        # Explicação clara da nova lógica
        delay_info = ctk.CTkFrame(main, fg_color=Colors.INFO_BG, corner_radius=8)
        delay_info.grid(row=5, column=0, columnspan=2, sticky="ew", pady=10)
        
        ctk.CTkLabel(
            delay_info, 
            text="ℹ️  O próximo grupo inicia X segundos após ESTE grupo COMEÇAR\n"
                 "    (não espera este grupo terminar!)",
            font=ctk.CTkFont(size=11),
            text_color=Colors.INFO,
            justify="left"
        ).pack(padx=15, pady=10)
        
        # Botões
        btn_frame = ctk.CTkFrame(main, fg_color="transparent")
        btn_frame.grid(row=6, column=0, columnspan=2, pady=(30, 0), sticky="e")
        
        ctk.CTkButton(
            btn_frame,
            text="Cancelar",
            width=100,
            height=40,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.BORDER,
            command=self.destroy
        ).grid(row=0, column=0, padx=5)
        
        ctk.CTkButton(
            btn_frame,
            text="Salvar",
            width=100,
            height=40,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._save
        ).grid(row=0, column=1, padx=5)
    
    def _populate_fields(self):
        self.name_entry.insert(0, self.process.name)
        self.path_entry.insert(0, self.process.path)
        self.group_entry.delete(0, "end")
        self.group_entry.insert(0, str(self.process.group))
        self.delay_entry.delete(0, "end")
        self.delay_entry.insert(0, str(self.process.delay_after))
    
    def _browse_file(self):
        path = filedialog.askopenfilename(
            title="Selecionar Script Python",
            filetypes=[("Python Files", "*.py"), ("All Files", "*.*")]
        )
        if path:
            self.path_entry.delete(0, "end")
            self.path_entry.insert(0, path)
            
            # Auto-preencher nome se vazio
            if not self.name_entry.get():
                name = Path(path).stem
                self.name_entry.insert(0, name)
    
    def _save(self):
        name = self.name_entry.get().strip()
        path = self.path_entry.get().strip()
        
        if not name or not path:
            messagebox.showerror("Erro", "Nome e caminho são obrigatórios!")
            return
        
        if not Path(path).exists():
            if not messagebox.askyesno("Aviso", f"Arquivo não encontrado:\n{path}\n\nDeseja continuar mesmo assim?"):
                return
        
        try:
            group = int(self.group_entry.get())
            delay = int(self.delay_entry.get())
        except ValueError:
            messagebox.showerror("Erro", "Grupo e delay devem ser números!")
            return
        
        self.result = {
            "name": name,
            "path": path,
            "group": group,
            "delay_after": delay
        }
        
        if self.callback:
            self.callback(self.result, self.process)
        
        self.destroy()

class AddScheduleDialog(ctk.CTkToplevel):
    """Diálogo para adicionar agendamento"""
    
    def __init__(self, master, callback=None):
        super().__init__(master)
        
        self.callback = callback
        
        self.title("Adicionar Agendamento")
        self.geometry("400x350")
        self.resizable(False, False)
        
        self.transient(master)
        self.grab_set()
        
        self._create_widgets()
    
    def _create_widgets(self):
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=30, pady=30)
        
        # Horário
        ctk.CTkLabel(main, text="Horário (HH:MM):", font=ctk.CTkFont(size=14)).pack(anchor="w")
        
        time_frame = ctk.CTkFrame(main, fg_color="transparent")
        time_frame.pack(fill="x", pady=(10, 20))
        
        self.hour_entry = ctk.CTkEntry(time_frame, width=60, height=45, font=ctk.CTkFont(size=18), justify="center")
        self.hour_entry.pack(side="left")
        self.hour_entry.insert(0, "08")
        
        ctk.CTkLabel(time_frame, text=":", font=ctk.CTkFont(size=24, weight="bold")).pack(side="left", padx=10)
        
        self.min_entry = ctk.CTkEntry(time_frame, width=60, height=45, font=ctk.CTkFont(size=18), justify="center")
        self.min_entry.pack(side="left")
        self.min_entry.insert(0, "00")
        
        # Dias da semana
        ctk.CTkLabel(main, text="Dias da semana:", font=ctk.CTkFont(size=14)).pack(anchor="w", pady=(10, 10))
        
        days_frame = ctk.CTkFrame(main, fg_color="transparent")
        days_frame.pack(fill="x")
        
        days = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
        self.day_vars = []
        
        for i, day in enumerate(days):
            var = ctk.BooleanVar(value=True)
            self.day_vars.append(var)
            cb = ctk.CTkCheckBox(
                days_frame,
                text=day,
                variable=var,
                width=60,
                checkbox_width=20,
                checkbox_height=20
            )
            cb.grid(row=i//4, column=i%4, padx=5, pady=5, sticky="w")
        
        # Botões
        btn_frame = ctk.CTkFrame(main, fg_color="transparent")
        btn_frame.pack(pady=(30, 0), fill="x")
        
        ctk.CTkButton(
            btn_frame,
            text="Cancelar",
            width=100,
            height=40,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.BORDER,
            command=self.destroy
        ).pack(side="left", expand=True, padx=5)
        
        ctk.CTkButton(
            btn_frame,
            text="Adicionar",
            width=100,
            height=40,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._save
        ).pack(side="left", expand=True, padx=5)
    
    def _save(self):
        try:
            hour = int(self.hour_entry.get())
            minute = int(self.min_entry.get())
            
            if not (0 <= hour < 24 and 0 <= minute < 60):
                raise ValueError()
            
            time_str = f"{hour:02d}:{minute:02d}"
            days = [i for i, var in enumerate(self.day_vars) if var.get()]
            
            if not days:
                messagebox.showerror("Erro", "Selecione pelo menos um dia!")
                return
            
            if self.callback:
                self.callback(time_str, days)
            
            self.destroy()
            
        except ValueError:
            messagebox.showerror("Erro", "Horário inválido!")

class AddFixedRoutineDialog(ctk.CTkToplevel):
    """Diálogo para adicionar rotina fixa"""
    
    def __init__(self, master, callback=None):
        super().__init__(master)
        
        self.callback = callback
        
        self.title("Adicionar Rotina Fixa")
        self.geometry("600x350")
        self.resizable(False, False)
        
        self.transient(master)
        self.grab_set()
        
        self._create_widgets()
    
    def _create_widgets(self):
        main = ctk.CTkFrame(self, fg_color="transparent")
        main.pack(fill="both", expand=True, padx=30, pady=30)
        main.grid_columnconfigure(1, weight=1)
        
        # Info
        info_frame = ctk.CTkFrame(main, fg_color=Colors.INFO_BG, corner_radius=8)
        info_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 20))
        
        ctk.CTkLabel(
            info_frame,
            text="📁 Rotinas fixas são arquivos que abrem automaticamente\n     quando a aplicação inicia e são verificados nos horários agendados.",
            font=ctk.CTkFont(size=11),
            text_color=Colors.INFO,
            justify="left"
        ).pack(padx=15, pady=10)
        
        # Nome
        ctk.CTkLabel(main, text="Nome:", font=ctk.CTkFont(size=13)).grid(row=1, column=0, sticky="w", pady=10)
        self.name_entry = ctk.CTkEntry(main, height=40, font=ctk.CTkFont(size=13))
        self.name_entry.grid(row=1, column=1, sticky="ew", pady=10, padx=(10, 0))
        
        # Caminho
        ctk.CTkLabel(main, text="Arquivo:", font=ctk.CTkFont(size=13)).grid(row=2, column=0, sticky="w", pady=10)
        path_frame = ctk.CTkFrame(main, fg_color="transparent")
        path_frame.grid(row=2, column=1, sticky="ew", pady=10, padx=(10, 0))
        path_frame.grid_columnconfigure(0, weight=1)
        
        self.path_entry = ctk.CTkEntry(path_frame, height=40, font=ctk.CTkFont(size=12))
        self.path_entry.grid(row=0, column=0, sticky="ew")
        
        browse_btn = ctk.CTkButton(
            path_frame,
            text=Icons.FOLDER,
            width=40,
            height=40,
            command=self._browse_file
        )
        browse_btn.grid(row=0, column=1, padx=(10, 0))
        
        # Botões
        btn_frame = ctk.CTkFrame(main, fg_color="transparent")
        btn_frame.grid(row=3, column=0, columnspan=2, pady=(20, 0), sticky="e")
        
        ctk.CTkButton(
            btn_frame,
            text="Cancelar",
            width=100,
            height=40,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.BORDER,
            command=self.destroy
        ).grid(row=0, column=0, padx=5)
        
        ctk.CTkButton(
            btn_frame,
            text="Adicionar",
            width=100,
            height=40,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._save
        ).grid(row=0, column=1, padx=5)
    
    def _browse_file(self):
        path = filedialog.askopenfilename(
            title="Selecionar Arquivo",
            filetypes=[("Todos os Arquivos", "*.*")]
        )
        if path:
            self.path_entry.delete(0, "end")
            self.path_entry.insert(0, path)
            
            # Auto-preencher nome se vazio
            if not self.name_entry.get():
                name = Path(path).stem
                self.name_entry.insert(0, name)
    
    def _save(self):
        name = self.name_entry.get().strip()
        path = self.path_entry.get().strip()
        
        if not name or not path:
            messagebox.showerror("Erro", "Nome e caminho são obrigatórios!")
            return
        
        if not Path(path).exists():
            if not messagebox.askyesno("Aviso", f"Arquivo não encontrado:\n{path}\n\nDeseja continuar mesmo assim?"):
                return
        
        if self.callback:
            self.callback(name, path)
        
        self.destroy()

class SettingsDialog(ctk.CTkToplevel):
    """Diálogo de configurações"""
    
    def __init__(self, master, config: Config, callback=None):
        super().__init__(master)
        
        self.config = config
        self.callback = callback
        
        self.title("Configurações")
        self.geometry("600x500")
        self.resizable(False, False)
        
        self.transient(master)
        self.grab_set()
        
        self._create_widgets()
    
    def _create_widgets(self):
        # Tabs
        tabview = ctk.CTkTabview(self)
        tabview.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Tab Geral
        tab_geral = tabview.add("Geral")
        self._create_general_tab(tab_geral)
        
        # Botões
        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(pady=(0, 20), padx=20, fill="x")
        
        ctk.CTkButton(
            btn_frame,
            text="Cancelar",
            width=100,
            height=40,
            fg_color=Colors.BG_TERTIARY,
            command=self.destroy
        ).pack(side="left", padx=5)
        
        ctk.CTkButton(
            btn_frame,
            text="Salvar",
            width=100,
            height=40,
            fg_color=Colors.PRIMARY,
            command=self._save
        ).pack(side="right", padx=5)
    
    def _create_supabase_tab(self, parent):
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(frame, text="URL do Supabase:", font=ctk.CTkFont(size=13)).pack(anchor="w", pady=(0, 5))
        self.supa_url = ctk.CTkEntry(frame, height=40)
        self.supa_url.pack(fill="x", pady=(0, 15))
        self.supa_url.insert(0, self.config.supabase_url)
        
        ctk.CTkLabel(frame, text="Chave API:", font=ctk.CTkFont(size=13)).pack(anchor="w", pady=(0, 5))
        self.supa_key = ctk.CTkEntry(frame, height=40, show="•")
        self.supa_key.pack(fill="x", pady=(0, 15))
        self.supa_key.insert(0, self.config.supabase_key)
        
        ctk.CTkLabel(frame, text="Nome da Tabela:", font=ctk.CTkFont(size=13)).pack(anchor="w", pady=(0, 5))
        self.supa_table = ctk.CTkEntry(frame, height=40)
        self.supa_table.pack(fill="x")
        self.supa_table.insert(0, self.config.supabase_table)
    
    def _create_general_tab(self, parent):
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(frame, text="Caminho pasta .wdm:", font=ctk.CTkFont(size=13)).pack(anchor="w", pady=(0, 5))
        
        wdm_frame = ctk.CTkFrame(frame, fg_color="transparent")
        wdm_frame.pack(fill="x", pady=(0, 15))
        wdm_frame.grid_columnconfigure(0, weight=1)
        
        self.wdm_path = ctk.CTkEntry(wdm_frame, height=40)
        self.wdm_path.grid(row=0, column=0, sticky="ew")
        self.wdm_path.insert(0, self.config.wdm_path)
        
        ctk.CTkButton(
            wdm_frame,
            text=Icons.FOLDER,
            width=40,
            height=40,
            command=self._browse_wdm
        ).grid(row=0, column=1, padx=(10, 0))
        
        self.clean_wdm_var = ctk.BooleanVar(value=self.config.clean_wdm)
        ctk.CTkCheckBox(
            frame,
            text="Limpar pasta .wdm antes de executar",
            variable=self.clean_wdm_var
        ).pack(anchor="w", pady=10)
        
        self.auto_start_var = ctk.BooleanVar(value=self.config.auto_start)
        ctk.CTkCheckBox(
            frame,
            text="Iniciar agendamento automaticamente ao abrir",
            variable=self.auto_start_var
        ).pack(anchor="w", pady=10)
    
    def _browse_wdm(self):
        path = filedialog.askdirectory(title="Selecionar pasta .wdm")
        if path:
            self.wdm_path.delete(0, "end")
            self.wdm_path.insert(0, path)
    
    def _save(self):
        self.config.supabase_url = self.supa_url.get().strip()
        self.config.supabase_key = self.supa_key.get().strip()
        self.config.supabase_table = self.supa_table.get().strip()
        self.config.wdm_path = self.wdm_path.get().strip()
        self.config.clean_wdm = self.clean_wdm_var.get()
        self.config.auto_start = self.auto_start_var.get()
        
        if self.callback:
            self.callback(self.config)
        
        self.destroy()

# ══════════════════════════════════════════════════════════════════════════════
# APLICAÇÃO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

class AutomationApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # Configuração da janela
        self.title(f"{APP_NAME} v{VERSION}")
        self.geometry("1400x900")
        self.minsize(1200, 700)
        
        # Configurar tema
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        
        # Managers
        self.config_mgr = ConfigManager()
        self.exec_mgr = ExecutionManager(self.config_mgr, self._handle_update)
        self.scheduler = SchedulerManager(self.config_mgr, self._start_routine_with_check)
        self.fixed_routine_mgr = FixedRoutineManager(self.config_mgr, self._log)
        
        # UI State
        self.process_cards: Dict[str, ProcessCard] = {}
        self.schedule_cards: Dict[str, ScheduleCard] = {}
        self.fixed_routine_cards: Dict[str, FixedRoutineCard] = {}
        
        # Criar UI
        self._create_ui()
        
        # Iniciar atualizações
        self._start_countdown_update()
        
        # Abrir rotinas fixas ao iniciar
        self.after(1000, self.fixed_routine_mgr.open_all_fixed_routines)
        
        # Auto-start
        if self.config_mgr.config.auto_start:
            self._start_routine()
    
    def _create_ui(self):
        # Container principal
        self.main_container = ctk.CTkFrame(self, fg_color=Colors.BG_DARK, corner_radius=0)
        self.main_container.pack(fill="both", expand=True)
        self.main_container.grid_columnconfigure(1, weight=1)
        self.main_container.grid_rowconfigure(0, weight=1)
        
        # Sidebar
        self._create_sidebar()
        
        # Content area
        self._create_content()
        
        # Status bar
        self._create_status_bar()
    
    def _create_sidebar(self):
        sidebar = ctk.CTkFrame(
            self.main_container,
            width=320,
            fg_color=Colors.BG_SECONDARY,
            corner_radius=0
        )
        sidebar.grid(row=0, column=0, sticky="nsew")
        sidebar.grid_propagate(False)
        sidebar.grid_rowconfigure(4, weight=1)
        
        # Logo/Título
        header = ctk.CTkFrame(sidebar, fg_color="transparent", height=80)
        header.pack(fill="x", padx=20, pady=(20, 10))
        header.pack_propagate(False)
        
        ctk.CTkLabel(
            header,
            text=f"{Icons.ROCKET} {APP_NAME}",
            font=ctk.CTkFont(size=22, weight="bold"),
            text_color=Colors.TEXT_PRIMARY
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            header,
            text=f"Versão {VERSION}",
            font=ctk.CTkFont(size=12),
            text_color=Colors.TEXT_MUTED
        ).pack(anchor="w")
        
        # Controles principais
        controls = ctk.CTkFrame(sidebar, fg_color=Colors.BG_TERTIARY, corner_radius=12)
        controls.pack(fill="x", padx=15, pady=15)
        
        self.start_btn = ctk.CTkButton(
            controls,
            text=f"{Icons.PLAY}  Executar Agora",
            height=50,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=Colors.SUCCESS,
            hover_color="#2ea043",
            command=self._start_routine
        )
        self.start_btn.pack(fill="x", padx=15, pady=(15, 10))
        
        self.stop_btn = ctk.CTkButton(
            controls,
            text=f"{Icons.STOP}  Parar Execução",
            height=40,
            font=ctk.CTkFont(size=13),
            fg_color=Colors.ERROR,
            hover_color="#da3633",
            command=self._stop_routine,
            state="disabled"
        )
        self.stop_btn.pack(fill="x", padx=15, pady=(0, 15))
        
        # Próxima execução
        next_frame = ctk.CTkFrame(sidebar, fg_color=Colors.BG_TERTIARY, corner_radius=12)
        next_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(
            next_frame,
            text="Próxima Execução",
            font=ctk.CTkFont(size=12),
            text_color=Colors.TEXT_SECONDARY
        ).pack(pady=(15, 5))
        
        self.countdown_label = ctk.CTkLabel(
            next_frame,
            text="--:--:--",
            font=ctk.CTkFont(size=28, weight="bold"),
            text_color=Colors.PRIMARY
        )
        self.countdown_label.pack(pady=(0, 5))
        
        self.next_time_label = ctk.CTkLabel(
            next_frame,
            text="Sem agendamentos",
            font=ctk.CTkFont(size=11),
            text_color=Colors.TEXT_MUTED
        )
        self.next_time_label.pack(pady=(0, 15))
        
        # Agendamentos
        sched_header = ctk.CTkFrame(sidebar, fg_color="transparent")
        sched_header.pack(fill="x", padx=15, pady=(10, 5))
        
        ctk.CTkLabel(
            sched_header,
            text=f"{Icons.CALENDAR} Agendamentos",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=Colors.TEXT_PRIMARY
        ).pack(side="left")
        
        ctk.CTkButton(
            sched_header,
            text=Icons.ADD,
            width=30,
            height=30,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._add_schedule
        ).pack(side="right")
        
        # Lista de agendamentos
        self.schedules_container = ctk.CTkScrollableFrame(
            sidebar,
            fg_color="transparent",
            height=120
        )
        self.schedules_container.pack(fill="x", padx=15, pady=5)
        
        self._refresh_schedules()
        
        # Configurações
        # Configurações
        ctk.CTkButton(
            sidebar,
            text=f"{Icons.SETTINGS}  Configurações",
            height=40,
            font=ctk.CTkFont(size=13),
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.BORDER,
            command=self._open_settings
        ).pack(fill="x", padx=15, pady=(10, 20), side="bottom")
    
    def _create_content(self):
        content = ctk.CTkFrame(self.main_container, fg_color=Colors.BG_DARK, corner_radius=0)
        content.grid(row=0, column=1, sticky="nsew", padx=0, pady=0)
        content.grid_columnconfigure(0, weight=1)
        content.grid_rowconfigure(1, weight=1)
        
        # Header
        header = ctk.CTkFrame(content, fg_color="transparent", height=60)
        header.grid(row=0, column=0, sticky="ew", padx=30, pady=(20, 10))
        
        ctk.CTkLabel(
            header,
            text="Painel de Controle",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color=Colors.TEXT_PRIMARY
        ).pack(side="left")
        
        # Paned window para conteúdo e log
        paned = ctk.CTkFrame(content, fg_color="transparent")
        paned.grid(row=1, column=0, sticky="nsew", padx=20, pady=(0, 20))
        paned.grid_columnconfigure(0, weight=3)
        paned.grid_columnconfigure(1, weight=2)
        paned.grid_rowconfigure(0, weight=1)
        
        # === ESQUERDA: ABAS (Processos / Rotinas Fixas) ===
        tabs = ctk.CTkTabview(paned, corner_radius=12, fg_color=Colors.BG_SECONDARY)
        tabs.grid(row=0, column=0, sticky="nsew", padx=(10, 10))
        
        # --- Tab Processos ---
        tab_proc = tabs.add("Processos")
        
        # Header Processos
        proc_header = ctk.CTkFrame(tab_proc, fg_color="transparent")
        proc_header.pack(fill="x", pady=5)
        
        ctk.CTkButton(
            proc_header,
            text=f"{Icons.ADD} Adicionar Processo",
            height=32,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._add_process
        ).pack(anchor="e")
        
        # Container Processos
        self.processes_container = ctk.CTkScrollableFrame(
            tab_proc,
            fg_color="transparent"
        )
        self.processes_container.pack(fill="both", expand=True, pady=10)
        
        self._refresh_processes()
        
        # --- Tab Rotinas Fixas ---
        tab_fixed = tabs.add("Rotinas Fixas")
        
        # Header Rotinas Fixas
        fixed_header = ctk.CTkFrame(tab_fixed, fg_color="transparent")
        fixed_header.pack(fill="x", pady=5)
        
        ctk.CTkLabel(
            fixed_header,
            text="📁  Rotinas que iniciam automaticamente",
            font=ctk.CTkFont(size=12),
            text_color=Colors.TEXT_MUTED
        ).pack(side="left", padx=5)
        
        ctk.CTkButton(
            fixed_header,
            text="Abrir Todas",
            height=32,
            fg_color=Colors.BG_TERTIARY,
            text_color=Colors.TEXT_PRIMARY,
            hover_color=Colors.BORDER,
            command=self.fixed_routine_mgr.open_all_fixed_routines
        ).pack(side="right", padx=(5, 0))
        
        ctk.CTkButton(
            fixed_header,
            text=f"{Icons.ADD} Adicionar Rotina",
            height=32,
            fg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_HOVER,
            command=self._add_fixed_routine
        ).pack(side="right")
        
        # Container Rotinas Fixas
        self.fixed_routines_container = ctk.CTkScrollableFrame(
            tab_fixed,
            fg_color="transparent"
        )
        self.fixed_routines_container.pack(fill="both", expand=True, pady=10)
        
        self._refresh_fixed_routines()
        
        # === DIREITA: LOG ===
        log_frame = ctk.CTkFrame(paned, fg_color=Colors.BG_SECONDARY, corner_radius=12)
        log_frame.grid(row=0, column=1, sticky="nsew", padx=(10, 10))
        
        log_header = ctk.CTkFrame(log_frame, fg_color="transparent", height=40)
        log_header.pack(fill="x", padx=15, pady=(15, 10))
        
        ctk.CTkLabel(
            log_header,
            text="Log de Execução",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=Colors.TEXT_PRIMARY
        ).pack(side="left")
        
        ctk.CTkButton(
            log_header,
            text="Limpar",
            width=60,
            height=28,
            fg_color=Colors.BG_TERTIARY,
            hover_color=Colors.BORDER,
            command=self._clear_log
        ).pack(side="right")
        
        self.log_text = ctk.CTkTextbox(
            log_frame,
            font=ctk.CTkFont(family="Consolas", size=11),
            fg_color=Colors.BG_DARK,
            text_color=Colors.TEXT_SECONDARY,
            corner_radius=8
        )
        self.log_text.pack(fill="both", expand=True, padx=15, pady=(0, 15))
    
    def _create_status_bar(self):
        status_bar = ctk.CTkFrame(
            self.main_container,
            height=35,
            fg_color=Colors.BG_TERTIARY,
            corner_radius=0
        )
        status_bar.grid(row=1, column=0, columnspan=2, sticky="ew")
        
        self.status_label = ctk.CTkLabel(
            status_bar,
            text="Pronto",
            font=ctk.CTkFont(size=11),
            text_color=Colors.TEXT_SECONDARY
        )
        self.status_label.pack(side="left", padx=20, pady=5)
        
        self.progress = ctk.CTkProgressBar(status_bar, width=200, height=8)
        self.progress.pack(side="right", padx=20, pady=10)
        self.progress.set(0)
    
    # === REFRESH UI ===
    
    def _refresh_processes(self):
        """Atualiza a lista de processos"""
        for widget in self.processes_container.winfo_children():
            widget.destroy()
        
        self.process_cards.clear()
        
        if not self.config_mgr.processes:
            empty_label = ctk.CTkLabel(
                self.processes_container,
                text="Nenhum processo configurado.\nClique em 'Adicionar Processo' para começar.",
                font=ctk.CTkFont(size=13),
                text_color=Colors.TEXT_MUTED
            )
            empty_label.pack(pady=50)
            return
        
        # Agrupar por grupo
        groups = {}
        for proc in self.config_mgr.processes:
            if proc.group not in groups:
                groups[proc.group] = []
            groups[proc.group].append(proc)
        
        for group_num in sorted(groups.keys()):
            # Calcular delay do grupo (máximo delay_after)
            group_delay = max(p.delay_after for p in groups[group_num])
            
            # Header do grupo
            group_header = ctk.CTkFrame(self.processes_container, fg_color="transparent")
            group_header.pack(fill="x", pady=(10, 5))
            
            ctk.CTkLabel(
                group_header,
                text=f"Grupo {group_num}",
                font=ctk.CTkFont(size=12, weight="bold"),
                text_color=Colors.PRIMARY
            ).pack(side="left")
            
            # Info do grupo
            info_parts = []
            if len(groups[group_num]) > 1:
                info_parts.append("executam em paralelo")
            if group_delay > 0:
                info_parts.append(f"próximo grupo em {group_delay}s")
            
            if info_parts:
                ctk.CTkLabel(
                    group_header,
                    text=f"({', '.join(info_parts)})",
                    font=ctk.CTkFont(size=11),
                    text_color=Colors.TEXT_MUTED
                ).pack(side="left", padx=10)
            
            # Processos do grupo
            for proc in groups[group_num]:
                card = ProcessCard(
                    self.processes_container,
                    proc,
                    on_edit=self._edit_process,
                    on_delete=self._delete_process,
                    on_toggle=self._toggle_process
                )
                card.pack(fill="x", pady=3)
                self.process_cards[proc.id] = card
    
    def _refresh_schedules(self):
        """Atualiza a lista de agendamentos"""
        for widget in self.schedules_container.winfo_children():
            widget.destroy()
        
        self.schedule_cards.clear()
        
        if not self.config_mgr.schedules:
            ctk.CTkLabel(
                self.schedules_container,
                text="Nenhum agendamento",
                font=ctk.CTkFont(size=11),
                text_color=Colors.TEXT_MUTED
            ).pack(pady=10)
            return
        
        for sched in self.config_mgr.schedules:
            card = ScheduleCard(
                self.schedules_container,
                sched,
                on_delete=self._delete_schedule,
                on_toggle=self._toggle_schedule
            )
            card.pack(fill="x", pady=3)
            self.schedule_cards[sched.id] = card
    
    # === ACTIONS ===
    
    def _add_process(self):
        def callback(data, _):
            self.config_mgr.add_process(
                data["name"],
                data["path"],
                data["group"],
                data["delay_after"]
            )
            self._refresh_processes()
            self._log(f"✓ Processo adicionado: {data['name']}")
        
        AddProcessDialog(self, callback=callback)
    
    def _edit_process(self, process: Process):
        def callback(data, proc):
            self.config_mgr.update_process(
                proc.id,
                name=data["name"],
                path=data["path"],
                group=data["group"],
                delay_after=data["delay_after"]
            )
            self._refresh_processes()
            self._log(f"✓ Processo atualizado: {data['name']}")
        
        AddProcessDialog(self, process=process, callback=callback)
    
    def _delete_process(self, process: Process):
        if messagebox.askyesno("Confirmar", f"Remover '{process.name}'?"):
            self.config_mgr.remove_process(process.id)
            self._refresh_processes()
            self._log(f"✗ Processo removido: {process.name}")
    
    def _toggle_process(self, process: Process, enabled: bool):
        self.config_mgr.update_process(process.id, enabled=enabled)
        status = "ativado" if enabled else "desativado"
        self._log(f"→ {process.name} {status}")
    
    def _add_schedule(self):
        def callback(time_str, days):
            sched = self.config_mgr.add_schedule(time_str)
            sched.days = days
            self.config_mgr.save_schedules()
            self._refresh_schedules()
            self._log(f"✓ Agendamento adicionado: {time_str}")
        
        AddScheduleDialog(self, callback=callback)
    
    def _delete_schedule(self, schedule: Schedule):
        if messagebox.askyesno("Confirmar", f"Remover agendamento das {schedule.time}?"):
            self.config_mgr.remove_schedule(schedule.id)
            self._refresh_schedules()
            self._log(f"✗ Agendamento removido: {schedule.time}")
    
    def _toggle_schedule(self, schedule: Schedule, enabled: bool):
        for s in self.config_mgr.schedules:
            if s.id == schedule.id:
                s.enabled = enabled
                break
        self.config_mgr.save_schedules()
    
    def _add_fixed_routine(self):
        def callback(name, path):
            self.config_mgr.add_fixed_routine(name, path)
            self._refresh_fixed_routines()
            self._log(f"✓ Rotina fixa adicionada: {name}")
            # Abre o arquivo imediatamente
            self.fixed_routine_mgr.open_file(path)
        
        AddFixedRoutineDialog(self, callback=callback)
    
    def _delete_fixed_routine(self, routine: FixedRoutine):
        if messagebox.askyesno("Confirmar", f"Remover rotina fixa '{routine.name}'?"):
            self.config_mgr.remove_fixed_routine(routine.id)
            self._refresh_fixed_routines()
            self._log(f"✗ Rotina fixa removida: {routine.name}")
    
    def _toggle_fixed_routine(self, routine: FixedRoutine, enabled: bool):
        self.config_mgr.update_fixed_routine(routine.id, enabled=enabled)
        status = "ativada" if enabled else "desativada"
        self._log(f"→ Rotina fixa {routine.name} {status}")
    
    def _refresh_fixed_routines(self):
        """Atualiza a lista de rotinas fixas"""
        for widget in self.fixed_routines_container.winfo_children():
            widget.destroy()
        
        self.fixed_routine_cards.clear()
        
        if not self.config_mgr.fixed_routines:
            ctk.CTkLabel(
                self.fixed_routines_container,
                text="Nenhuma rotina fixa",
                font=ctk.CTkFont(size=11),
                text_color=Colors.TEXT_MUTED
            ).pack(pady=10)
            return
        
        for routine in self.config_mgr.fixed_routines:
            card = FixedRoutineCard(
                self.fixed_routines_container,
                routine,
                on_delete=self._delete_fixed_routine,
                on_toggle=self._toggle_fixed_routine
            )
            card.pack(fill="x", pady=3)
            self.fixed_routine_cards[routine.id] = card
    
    def _start_routine_with_check(self):
        """Inicia a rotina no agendamento sem reabrir fixas repetidamente"""
        self._start_routine()
    
    def _open_settings(self):
        def callback(config):
            self.config_mgr.config = config
            self.config_mgr.save_config()
            self.exec_mgr._init_supabase()
            self._log("✓ Configurações salvas")
        
        SettingsDialog(self, self.config_mgr.config, callback=callback)
    
    def _start_routine(self):
        if self.exec_mgr.running:
            return
        
        if not self.config_mgr.get_enabled_processes():
            messagebox.showwarning("Aviso", "Nenhum processo habilitado para executar!")
            return
        
        # Reset status visual
        for proc in self.config_mgr.processes:
            if proc.id in self.process_cards:
                self.process_cards[proc.id].update_status("waiting")
        
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.progress.configure(mode="indeterminate")
        self.progress.start()
        
        self._log(f"\n{'='*60}")
        self._log(f"▶ INICIANDO ROTINA - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        self._log(f"{'='*60}")
        self._log(f"ℹ️  Modo: Grupos iniciam após DELAY do grupo anterior COMEÇAR")
        
        def on_finish():
            self.after(0, self._routine_finished)
        
        self.exec_mgr.execute_routine(callback_finish=on_finish)
    
    def _stop_routine(self):
        self.exec_mgr.stop_routine()
    
    def _routine_finished(self):
        self.start_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.progress.stop()
        self.progress.set(1)
        self._log(f"\n{'='*60}")
        self._log(f"✓ ROTINA FINALIZADA - {datetime.now().strftime('%H:%M:%S')}")
        self._log(f"{'='*60}\n")
    
    # === UPDATES ===
    
    def _handle_update(self, msg: dict):
        """Processa atualizações do ExecutionManager"""
        msg_type = msg.get("type")
        
        if msg_type == "log":
            self.after(0, lambda: self._log(msg["message"]))
        
        elif msg_type == "process_status":
            proc_id = msg["id"]
            status = msg["status"]
            
            def update():
                # Atualizar card de processo
                if proc_id in self.process_cards:
                    self.process_cards[proc_id].update_status(status)
                
                obs = msg.get("obs", "")
                if obs:
                    self._log(f"  └─ {obs}")
            
            self.after(0, update)
        
        elif msg_type == "process_log":
            proc_id = msg.get("id", "")
            line = msg["line"].rstrip()
            
            def update_log():
                # Log no console geral (prefixado)
                self._log(line, prefix="    ")
            
            self.after(0, update_log)
        
        elif msg_type == "countdown":
            # Opcional: mostrar countdown visual
            pass
        
        elif msg_type == "routine_finished":
            pass  # Tratado pelo callback
    
    def _log(self, message: str, prefix: str = ""):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{timestamp}] {prefix}{message}\n")
        self.log_text.see("end")
    
    def _clear_log(self):
        self.log_text.delete("1.0", "end")
    
    def _start_countdown_update(self):
        """Atualiza o countdown periodicamente"""
        def update():
            time_left = self.scheduler.get_time_until_next()
            next_run = self.scheduler.get_next_run()
            
            if time_left:
                total_seconds = int(time_left.total_seconds())
                if total_seconds > 0:
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    self.countdown_label.configure(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")
                    
                    if next_run:
                        self.next_time_label.configure(
                            text=next_run.strftime("%d/%m às %H:%M")
                        )
                else:
                    self.countdown_label.configure(text="Executando...")
            else:
                self.countdown_label.configure(text="--:--:--")
                self.next_time_label.configure(text="Sem agendamentos")
            
            # Status
            if self.exec_mgr.running:
                self.status_label.configure(text="Executando rotina...")
            else:
                enabled = len(self.config_mgr.get_enabled_processes())
                total = len(self.config_mgr.processes)
                self.status_label.configure(text=f"Pronto • {enabled}/{total} processos ativos")
            
            self.after(1000, update)
        
        update()
    
    def on_closing(self):
        """Cleanup ao fechar"""
        self.scheduler.stop()
        self.exec_mgr.stop_routine()
        self.destroy()

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app = AutomationApp()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()
import tkinter as tk
from tkinter import filedialog, messagebox
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.dialogs import Messagebox
from ttkbootstrap.scrolled import ScrolledText
import oracledb
import os
import threading
import pandas as pd
from datetime import datetime
import time
import sys
import subprocess

# --- Configuração do Oracle Client ---
CLIENT_LIB_DIR = r"C:\Users\claudeyr.sousa\Documents\instantclient-basic-windows.x64-21.19.0.0.0dbru\instantclient_21_19"

try:
    if os.path.exists(CLIENT_LIB_DIR):
        oracledb.init_oracle_client(lib_dir=CLIENT_LIB_DIR)
except Exception as e:
    print(f"Aviso Oracle Client: {e}")

# --- Credenciais ---
USUARIO = "CONSULTA"
SENHA = "CONPHPCMV"
DSN = oracledb.makedsn("192.168.8.199", 1521, service_name="WINT")

class ModernOracleApp:
    """
    Aplicação moderna e profissional para consultas Oracle (WinThor).
    Versão Corrigida: Erro de Escopo de Variável (Scope Fix)
    """
    
    def __init__(self):
        # Tema escuro moderno
        self.root = ttk.Window(
            title="🔷 Oracle Query Manager Pro - WinThor",
            themename="darkly",
            size=(1400, 850),
            minsize=(1000, 600)
        )
        self.root.place_window_center()
        
        # Variáveis de Estado
        self.query_history = []
        self.current_data = None
        self.current_columns = None
        self.is_connected = False
        self.execution_time = 0
        self._sort_reverse = False
        
        self._create_menu()
        self._create_toolbar()
        self._create_main_layout()
        self._create_status_bar()
        self._bind_shortcuts()
        
        # Testar conexão inicial em background
        self.test_connection()
    
    def _create_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # Menu Arquivo
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="📁 Arquivo", menu=file_menu)
        file_menu.add_command(label="📂 Abrir Query (.sql)", command=self.open_sql_file, accelerator="Ctrl+O")
        file_menu.add_command(label="💾 Salvar Query", command=self.save_sql_file, accelerator="Ctrl+S")
        file_menu.add_separator()
        file_menu.add_command(label="📊 Exportar Excel", command=self.export_to_excel, accelerator="Ctrl+E")
        file_menu.add_command(label="📄 Exportar CSV", command=self.export_to_csv)
        file_menu.add_separator()
        file_menu.add_command(label="🚪 Sair", command=self.root.quit)
        
        # Menu Editar
        edit_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="✏️ Editar", menu=edit_menu)
        edit_menu.add_command(label="🔄 Limpar Query", command=self.clear_query)
        edit_menu.add_command(label="🗑️ Limpar Resultados", command=self.clear_results)
        edit_menu.add_separator()
        edit_menu.add_command(label="📋 Copiar Resultados", command=self.copy_results)
        
        # Menu Temas
        theme_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="🎨 Temas", menu=theme_menu)
        themes = ['darkly', 'superhero', 'cyborg', 'vapor', 'solar', 'cosmo', 'flatly', 'litera']
        for theme in themes:
            theme_menu.add_command(label=theme.capitalize(), 
                                   command=lambda t=theme: self.change_theme(t))
        
        # Menu Ajuda
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="❓ Ajuda", menu=help_menu)
        help_menu.add_command(label="📖 Atalhos de Teclado", command=self.show_shortcuts)
        help_menu.add_command(label="ℹ️ Sobre", command=self.show_about)
    
    def _create_toolbar(self):
        toolbar = ttk.Frame(self.root, padding=5)
        toolbar.pack(fill=X, padx=10, pady=(10, 0))
        
        # Botões principais
        ttk.Button(toolbar, text="▶️ Executar", command=self.execute_query, 
                   bootstyle='success', width=12).pack(side=LEFT, padx=2)
        
        ttk.Button(toolbar, text="⏹️ Parar", command=self.stop_query, 
                   bootstyle='danger-outline', width=10).pack(side=LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=VERTICAL).pack(side=LEFT, fill=Y, padx=10)
        
        ttk.Button(toolbar, text="📊 Excel", command=self.export_to_excel, 
                   bootstyle='primary-outline', width=12).pack(side=LEFT, padx=2)
        
        ttk.Button(toolbar, text="📄 CSV", command=self.export_to_csv, 
                   bootstyle='info-outline', width=12).pack(side=LEFT, padx=2)
        
        ttk.Button(toolbar, text="📋 Copiar", command=self.copy_results, 
                   bootstyle='secondary-outline', width=12).pack(side=LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=VERTICAL).pack(side=LEFT, fill=Y, padx=10)
        
        ttk.Button(toolbar, text="🗑️ Limpar", command=self.clear_all, 
                   bootstyle='warning-outline', width=12).pack(side=LEFT, padx=2)
        
        # Indicador de conexão
        self.connection_frame = ttk.Frame(toolbar)
        self.connection_frame.pack(side=RIGHT, padx=5)
        
        self.connection_indicator = ttk.Label(
            self.connection_frame, 
            text="⚫ Desconectado",
            font=('Segoe UI', 10)
        )
        self.connection_indicator.pack(side=LEFT)
        
        ttk.Button(
            self.connection_frame, 
            text="🔌 Testar", 
            command=self.test_connection,
            bootstyle='info-outline',
            width=10
        ).pack(side=LEFT, padx=5)
    
    def _create_main_layout(self):
        self.paned = ttk.Panedwindow(self.root, orient=VERTICAL)
        self.paned.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        self._create_query_panel()
        self._create_results_panel()
    
    def _create_query_panel(self):
        query_frame = ttk.Labelframe(self.paned, text="📝 Editor SQL", padding=10)
        self.paned.add(query_frame, weight=1)
        
        # Botões de templates
        template_frame = ttk.Frame(query_frame)
        template_frame.pack(fill=X, pady=(0, 5))
        
        ttk.Label(template_frame, text="Templates Rápidos:", font=('Segoe UI', 9)).pack(side=LEFT)
        
        templates = [
            ("PCPEDIDO", "SELECT NUMPED, DATA, CODCLIENTE, VALOR FROM PCPEDIDO WHERE DATA >= TRUNC(SYSDATE) AND ROWNUM <= 50 ORDER BY NUMPED DESC;"),
            ("PCPRODUT", "SELECT CODPROD, DESCRICAO, EMBALAGEM, CODFAB FROM PCPRODUT WHERE ROWNUM <= 50;"),
            ("PCCLIENT", "SELECT CODCLI, CLIENTE, CGCENT, ESTADO FROM PCCLIENT WHERE ROWNUM <= 50;"),
            ("Estoque", "SELECT P.CODPROD, P.DESCRICAO, E.QT FROM PCPRODUT P, PCEST E WHERE P.CODPROD = E.CODPROD AND P.CODPROD = 1;"),
        ]
        
        for name, sql in templates:
            ttk.Button(
                template_frame, 
                text=name, 
                command=lambda s=sql: self.insert_template(s),
                bootstyle='secondary-outline'
            ).pack(side=LEFT, padx=3)
        
        self.sql_editor = ScrolledText(query_frame, height=8, autohide=True)
        self.sql_editor.pack(fill=BOTH, expand=True)
        self.sql_editor.text.configure(font=('Consolas', 11), wrap='word')
        
        # Query padrão
        self.sql_editor.text.insert('1.0', "SELECT * FROM PCPEDIDO WHERE ROWNUM <= 20 ORDER BY NUMPED DESC;")
        
        # Histórico
        history_frame = ttk.Frame(query_frame)
        history_frame.pack(fill=X, pady=(5, 0))
        
        ttk.Label(history_frame, text="📜 Histórico:", font=('Segoe UI', 9)).pack(side=LEFT)
        
        self.history_combo = ttk.Combobox(history_frame, width=80, state='readonly')
        self.history_combo.pack(side=LEFT, padx=5, fill=X, expand=True)
        self.history_combo.bind('<<ComboboxSelected>>', self.load_from_history)
    
    def _create_results_panel(self):
        results_frame = ttk.Labelframe(self.paned, text="📊 Resultados", padding=10)
        self.paned.add(results_frame, weight=2)
        
        controls_frame = ttk.Frame(results_frame)
        controls_frame.pack(fill=X, pady=(0, 10))
        
        ttk.Label(controls_frame, text="🔍 Filtrar:").pack(side=LEFT)
        self.filter_var = tk.StringVar()
        self.filter_var.trace('w', self.filter_results)
        self.filter_entry = ttk.Entry(controls_frame, textvariable=self.filter_var, width=30)
        self.filter_entry.pack(side=LEFT, padx=5)
        
        ttk.Button(
            controls_frame, 
            text="❌ Limpar", 
            command=lambda: self.filter_var.set(''),
            bootstyle='secondary-outline'
        ).pack(side=LEFT, padx=5)
        
        self.results_info = ttk.Label(controls_frame, text="Nenhum resultado", font=('Segoe UI', 10, 'bold'))
        self.results_info.pack(side=RIGHT)
        
        # Tabela
        table_frame = ttk.Frame(results_frame)
        table_frame.pack(fill=BOTH, expand=True)
        
        self.tree_scroll_y = ttk.Scrollbar(table_frame, orient=VERTICAL)
        self.tree_scroll_y.pack(side=RIGHT, fill=Y)
        self.tree_scroll_x = ttk.Scrollbar(table_frame, orient=HORIZONTAL)
        self.tree_scroll_x.pack(side=BOTTOM, fill=X)
        
        self.tree = ttk.Treeview(
            table_frame,
            yscrollcommand=self.tree_scroll_y.set,
            xscrollcommand=self.tree_scroll_x.set,
            show='headings',
            selectmode='extended',
            bootstyle='primary'
        )
        self.tree.pack(fill=BOTH, expand=True)
        
        self.tree_scroll_y.config(command=self.tree.yview)
        self.tree_scroll_x.config(command=self.tree.xview)
        
        self.tree.bind('<Control-c>', self.copy_selected_rows)
        self.tree.bind('<Double-1>', self.show_row_detail)
        self.tree.bind('<Button-3>', self.show_context_menu)
        
        self.context_menu = tk.Menu(self.tree, tearoff=0)
        self.context_menu.add_command(label="📋 Copiar Linha(s)", command=self.copy_selected_rows)
        self.context_menu.add_command(label="👁️ Ver Detalhes", command=self.show_row_detail)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="📊 Exportar Seleção", command=self.export_selection)

    def _create_status_bar(self):
        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill=X, padx=10, pady=5)
        
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate', length=200, bootstyle='success-striped')
        self.progress.pack(side=LEFT)
        
        self.status_var = tk.StringVar(value="✅ Pronto")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, font=('Segoe UI', 10))
        self.status_label.pack(side=LEFT, padx=20)
        
        self.time_label = ttk.Label(status_frame, text="⏱️ 0.00s", font=('Segoe UI', 10))
        self.time_label.pack(side=RIGHT, padx=10)
        
        self.row_count_label = ttk.Label(status_frame, text="📝 0 linhas", font=('Segoe UI', 10, 'bold'))
        self.row_count_label.pack(side=RIGHT, padx=10)
    
    def _bind_shortcuts(self):
        self.root.bind('<Control-Return>', lambda e: self.execute_query())
        self.root.bind('<F5>', lambda e: self.execute_query())
        self.root.bind('<Control-e>', lambda e: self.export_to_excel())
        self.root.bind('<Control-o>', lambda e: self.open_sql_file())
        self.root.bind('<Control-s>', lambda e: self.save_sql_file())
        self.root.bind('<Escape>', lambda e: self.stop_query())
    
    # --- Lógica do Banco de Dados ---
    
    def test_connection(self):
        def _test():
            try:
                self.update_status("🔄 Conectando ao WinThor...")
                conn = oracledb.connect(user=USUARIO, password=SENHA, dsn=DSN)
                conn.close()
                self.is_connected = True
                self.root.after(0, lambda: self.connection_indicator.configure(text="🟢 Conectado", foreground='#28a745'))
                self.update_status("✅ Conexão OK!")
            except Exception as e:
                # CORREÇÃO: Capturar mensagem antes do lambda
                err_msg = str(e)
                self.is_connected = False
                self.root.after(0, lambda: self.connection_indicator.configure(text="🔴 Erro", foreground='#dc3545'))
                self.update_status(f"❌ Erro: {err_msg[:50]}...")
        
        threading.Thread(target=_test, daemon=True).start()
    
    def execute_query(self):
        sql_query = self.sql_editor.text.get("1.0", tk.END).strip()
        
        if not sql_query:
            Messagebox.show_warning("A query está vazia!", "Aviso", parent=self.root)
            return
        
        # Validar SELECT apenas
        if not sql_query.upper().lstrip().startswith("SELECT"):
            Messagebox.show_error("Por segurança, apenas comandos SELECT são permitidos.", "Bloqueado", parent=self.root)
            return
        
        # Histórico
        if sql_query not in self.query_history:
            self.query_history.insert(0, sql_query)
            self.query_history = self.query_history[:20]
            display_hist = [q[:80].replace('\n', ' ') + "..." for q in self.query_history]
            self.history_combo['values'] = display_hist
        
        threading.Thread(target=self._run_query, args=(sql_query,), daemon=True).start()
    
    def _run_query(self, sql_query):
        self.root.after(0, self._start_loading)
        start_time = time.time()
        
        try:
            connection = oracledb.connect(user=USUARIO, password=SENHA, dsn=DSN)
            cursor = connection.cursor()
            cursor.execute(sql_query)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                self.current_columns = columns
                self.current_data = rows
                
                self.execution_time = time.time() - start_time
                self.root.after(0, lambda: self._display_results(columns, rows))
            else:
                self.root.after(0, lambda: Messagebox.show_info("Comando executado sem retorno de dados.", "Info", parent=self.root))
            
            connection.close()
            
        except Exception as e:
            # CORREÇÃO CRÍTICA AQUI
            # O Python deleta 'e' ao sair do bloco except.
            # Precisamos salvar a string antes de passar para o lambda do Tkinter.
            erro_str = str(e)
            self.root.after(0, lambda: Messagebox.show_error(f"Erro na execução:\n\n{erro_str}", "Erro Oracle", parent=self.root))
        finally:
            self.root.after(0, self._stop_loading)
    
    def _start_loading(self):
        self.progress.start(10)
        self.update_status("⏳ Buscando dados...")
        self.tree.delete(*self.tree.get_children())
    
    def _stop_loading(self):
        self.progress.stop()
        self.update_status("✅ Pronto")
        self.time_label.configure(text=f"⏱️ {self.execution_time:.2f}s")
    
    def _display_results(self, columns, rows):
        self.tree["columns"] = columns
        
        for col in columns:
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_column(c))
            self.tree.column(col, anchor=W, width=120, minwidth=80)
        
        self.tree.tag_configure('oddrow', background='#2d2d2d')
        self.tree.tag_configure('evenrow', background='#1e1e1e')
        
        display_rows = rows[:5000] 
        
        for i, row in enumerate(display_rows):
            tag = 'oddrow' if i % 2 else 'evenrow'
            display_vals = [str(v) if v is not None else '' for v in row]
            self.tree.insert("", tk.END, values=display_vals, tags=(tag,))
        
        count_msg = f"{len(rows):,} linhas"
        if len(rows) > 5000:
            count_msg += " (Exibindo 5000)"
            
        self.row_count_label.configure(text=f"📝 {count_msg}")
        self.results_info.configure(text=f"✅ {len(rows)} res. | {len(columns)} col.")
        
        if self.tree.get_children():
            self.tree.selection_set(self.tree.get_children()[0])
            self.tree.focus()

    # --- Exportação e Utilitários ---
    
    def export_to_excel(self):
        if not self.current_data or not self.current_columns:
            Messagebox.show_warning("Execute uma query primeiro!", "Sem Dados", parent=self.root)
            return
        
        try:
            filename = filedialog.asksaveasfilename(
                title="Salvar Excel",
                defaultextension=".xlsx",
                filetypes=[("Excel", "*.xlsx")],
                initialfile=f"WinThor_Export_{datetime.now().strftime('%d%m%Y_%H%M')}.xlsx",
                parent=self.root
            )
            
            if not filename: return
            
            if not filename.lower().endswith(".xlsx"):
                filename += ".xlsx"
            
            self.update_status("⏳ Gerando Excel...")
            self.root.update()
            
            df = pd.DataFrame(self.current_data, columns=self.current_columns)
            
            try:
                import openpyxl
            except ImportError:
                Messagebox.show_error("Biblioteca 'openpyxl' não encontrada.\nInstale com: pip install openpyxl", "Erro de Dependência", parent=self.root)
                self.update_status("❌ Erro: openpyxl ausente")
                return

            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Dados WinThor')
                
                worksheet = writer.sheets['Dados WinThor']
                for i, col in enumerate(df.columns):
                    worksheet.column_dimensions[chr(65 + i) if i < 26 else 'A'].width = 15
            
            self.update_status("✅ Excel salvo!")
            
            ask = Messagebox.show_question(f"Arquivo salvo com sucesso:\n{filename}\n\nDeseja abrir agora?", "Sucesso", parent=self.root)
            if ask == 'Yes':
                try:
                    os.startfile(filename)
                except:
                    subprocess.Popen(f'explorer /select,"{filename}"')

        except Exception as e:
            Messagebox.show_error(f"Não foi possível salvar o arquivo.\nErro: {e}", "Erro ao Salvar", parent=self.root)
            self.update_status("❌ Erro ao exportar")

    def export_to_csv(self):
        if not self.current_data:
            Messagebox.show_warning("Sem dados!", "Aviso", parent=self.root)
            return
            
        try:
            filename = filedialog.asksaveasfilename(
                title="Salvar CSV",
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile=f"WinThor_Export_{datetime.now().strftime('%d%m%Y_%H%M')}.csv",
                parent=self.root
            )
            
            if not filename: return
            
            if not filename.lower().endswith(".csv"):
                filename += ".csv"
                
            self.update_status("⏳ Gerando CSV...")
            self.root.update()
            
            df = pd.DataFrame(self.current_data, columns=self.current_columns)
            df.to_csv(filename, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            
            self.update_status("✅ CSV salvo!")
            
            ask = Messagebox.show_question(f"Arquivo salvo!\nDeseja abrir a pasta?", "Sucesso", parent=self.root)
            if ask == 'Yes':
                subprocess.Popen(f'explorer /select,"{filename}"')
                
        except Exception as e:
            Messagebox.show_error(f"Erro: {e}", "Falha", parent=self.root)

    def export_selection(self):
        selected = self.tree.selection()
        if not selected: return
        
        rows = [self.tree.item(i)['values'] for i in selected]
        df = pd.DataFrame(rows, columns=self.current_columns)
        
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")], parent=self.root)
        if filename:
            if not filename.endswith(".xlsx"): filename += ".xlsx"
            df.to_excel(filename, index=False)
            Messagebox.show_info("Seleção salva!", "Sucesso", parent=self.root)

    # --- Funções Auxiliares UI ---
    
    def sort_column(self, col):
        if not self.current_data: return
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=self._sort_reverse)
        for index, (val, k) in enumerate(l):
            self.tree.move(k, '', index)
        self._sort_reverse = not self._sort_reverse

    def filter_results(self, *args):
        search = self.filter_var.get().lower()
        if not self.current_data: return
        
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        count = 0
        for i, row in enumerate(self.current_data):
            if count > 2000: break
            
            row_text = " ".join([str(x).lower() for x in row if x is not None])
            if search in row_text:
                tag = 'oddrow' if count % 2 else 'evenrow'
                d_vals = [str(v) if v is not None else '' for v in row]
                self.tree.insert("", tk.END, values=d_vals, tags=(tag,))
                count += 1
        
        self.row_count_label.configure(text=f"📝 {count} linhas (Filtrado)")

    def copy_results(self):
        if self.current_data:
            df = pd.DataFrame(self.current_data, columns=self.current_columns)
            df.to_clipboard(index=False, sep='\t')
            self.update_status("📋 Tudo copiado!")

    def copy_selected_rows(self, event=None):
        selected = self.tree.selection()
        if not selected: return
        lines = []
        for i in selected:
            vals = self.tree.item(i)['values']
            lines.append('\t'.join([str(v) for v in vals]))
        self.root.clipboard_clear()
        self.root.clipboard_append('\n'.join(lines))
        self.update_status(f"📋 {len(selected)} linhas copiadas")

    def show_row_detail(self, event=None):
        sel = self.tree.selection()
        if not sel: return
        
        vals = self.tree.item(sel[0])['values']
        top = ttk.Toplevel(self.root)
        top.title("Detalhes")
        top.geometry("600x500")
        
        txt = ScrolledText(top)
        txt.pack(fill=BOTH, expand=True)
        
        for col, val in zip(self.current_columns, vals):
            txt.text.insert(tk.END, f"📌 {col}:\n   {val}\n\n")

    def show_context_menu(self, event):
        try: self.context_menu.tk_popup(event.x_root, event.y_root)
        finally: self.context_menu.grab_release()

    def insert_template(self, sql):
        self.sql_editor.text.delete('1.0', tk.END)
        self.sql_editor.text.insert('1.0', sql)

    def load_from_history(self, event=None):
        sel = self.history_combo.current()
        if sel >= 0:
            full_sql = self.query_history[sel]
            self.sql_editor.text.delete('1.0', tk.END)
            self.sql_editor.text.insert('1.0', full_sql)

    def open_sql_file(self):
        f = filedialog.askopenfilename(filetypes=[("SQL", "*.sql")], parent=self.root)
        if f:
            with open(f, 'r', encoding='utf-8') as file:
                self.sql_editor.text.delete('1.0', tk.END)
                self.sql_editor.text.insert('1.0', file.read())

    def save_sql_file(self):
        f = filedialog.asksaveasfilename(defaultextension=".sql", filetypes=[("SQL", "*.sql")], parent=self.root)
        if f:
            with open(f, 'w', encoding='utf-8') as file:
                file.write(self.sql_editor.text.get('1.0', tk.END))
            self.update_status(f"💾 Salvo: {os.path.basename(f)}")

    def clear_query(self):
        self.sql_editor.text.delete('1.0', tk.END)

    def clear_results(self):
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = ()
        self.current_data = None
        self.row_count_label.configure(text="📝 0 linhas")

    def clear_all(self):
        self.clear_query()
        self.clear_results()

    def stop_query(self):
        self.update_status("⏹️ Interrompido pelo usuário")

    def change_theme(self, theme):
        self.root.style.theme_use(theme)

    def update_status(self, msg):
        self.status_var.set(msg)

    def show_shortcuts(self):
        Messagebox.show_info("F5: Executar\nCtrl+E: Excel\nCtrl+S: Salvar SQL", "Atalhos", parent=self.root)

    def show_about(self):
        Messagebox.show_info("Oracle Query Manager - WinThor\nVersão 2.1 Fixed", "Sobre", parent=self.root)

    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    app = ModernOracleApp()
    app.run()
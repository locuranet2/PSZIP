import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
import zipfile
import ftplib
import os
import threading
import concurrent.futures
import subprocess
import time
import json
import tempfile
import shutil

try:
    import rarfile
    RAR_SUPPORTED = True
except ImportError:
    RAR_SUPPORTED = False

CONFIG_FILE = "config_zip_ftp.json"

class ZipToFtpApp:
    def __init__(self, root):
        self.root = root
        
        # Iniciar variables de estado UI de forma persistente
        self.ftp_host_var = tk.StringVar()
        self.ftp_port_var = tk.StringVar(value="21")
        self.ftp_dest_var = tk.StringVar(value="/")
        self.zip_path_var = tk.StringVar()
        self.create_root_var = tk.BooleanVar(value=True)
        self.streaming_var = tk.BooleanVar(value=True)
        self.concurrent_var = tk.IntVar(value=5)
        
        self._load_i18n()
        self.current_lang = "en-us" # default per GitHub version request
        
        self.root.title("ZIP/RAR to FTP Streamer")
        self.root.geometry("800x700")
        self.root.minsize(700, 600)
        
        self._find_external_tools()
        self.load_config() # Carga lang y rellena vars, y re-aplica geometría si existe
        
        self.is_running = False
        self.is_connected = False
        
        self.bytes_lock = threading.Lock()
        self.total_bytes = 0
        self.uploaded_bytes = 0
        self.start_time = 0
        self.show_progress = False
        
        self.spinner_chars = ['|', '/', '-', '\\']
        self.spinner_idx = 0
        
        self.scanned_f = 0
        self.scanned_d = 0
        
        self.rar_extract_lock = threading.Lock()
        
        self.overwrite_policy = "ASK"
        self.policy_lock = threading.Lock()
        self.policy_event = threading.Event()
        self.policy_choice = None
        self.prompt_lock = threading.Lock()
        
        self.setup_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def on_close(self):
        if self.is_running:
            if not messagebox.askyesno(self.t("dlg_confirm_exit_title"), self.t("dlg_confirm_exit_msg")):
                return
            self.cancel_processing()
            
        self.save_config()
        self.root.destroy()

    def _load_i18n(self):
        # Fallback dictionary (Hardcoded English) to prevent Crashes if JSON is missing
        default_en = {
            "section_conn": "2. FTP Connection Settings", "host": "Host:", "port": "Port:", 
            "remote_path": "FTP Target Path:", "section_file": "1. File Selection", 
            "select_zip": "Select Archive (ZIP/RAR):", "browse": "Browse...", 
            "section_proc": "3. Processing Options", "create_root": "Create root folder with Archive name", 
            "streaming": "Direct Streaming (extract on-the-fly)", "concurrent": "Concurrent Files (Max):", 
            "btn_start_init": "Start Upload", "btn_start_active": "Start Upload ({files} files, {folders} folders)", 
            "btn_cancel": "Cancel", "btn_connect": "Connect", "btn_reconnect": "Re-Connect", 
            "tab_log": "Log", "tab_queued": "Queued Files ({count})", "tab_success": "Successful Transfers ({count})", 
            "tab_failed": "Failed Transfers ({count})", "col_local": "Local", "col_dest": "Destination", 
            "col_size": "Size", "col_status": "Status", "col_info": "Info", "btn_retry_sel": "Retry Selected", 
            "btn_retry_all": "Retry All Failed", "btn_clear": "Clear List", "menu_retry": "Retry Selected", 
            "menu_all": "Select All (Ctrl+A)", "menu_clear": "Clear List", "status_connecting": "Connecting to {host}...", 
            "status_connected": "Connected to {host}", "status_disconnected": "Disconnected", 
            "status_pending": "Pending", "status_speed": "Average Speed: {mb:.2f} MB/s", 
            "ftp_error": "FTP Error", "log_validating": "Validating destination path...", 
            "log_root_info": "All folders will be created under: {path}", "log_analyzing": "Analyzing archive...", 
            "log_creating_folders": "Creating {count} folders on FTP...", "log_folders_ready": "Folder structure ready.", 
            "log_starting_upload": "Starting upload of {count} files ({size})...", 
            "log_uploading": "Uploading: {prog}% (ETA: {eta})", "log_error": "[ERROR] {file}: {msg}", 
            "log_ok": "[OK] {path}", "log_cancel_user": "Cancelled by user", 
            "log_cancel_request": "🛑 Process cancelled. Moving remaining files to 'Failed'...", 
            "log_finished": "🏁 Tasks finished.", "log_summary": "✅ Success: {success} | ❌ Failed: {fail} | 📦 Total: {total}", 
            "log_retrying": "🕯️ Retrying {count} failed files...", "log_retry_item": "  └─ [{idx}] {name}", 
            "log_fail_retry": "[FAIL-RETRY] {path}: {msg}", "log_ok_retry": "[OK-RETRY] {path}", 
            "log_retry_finished": "🏁 Retry finished. ✅: {success} | ❌: {fail} | 📦 Total: {total}", 
            "prog_total": "Total Progress:", "prog_abort": "Total Progress: Aborted ❌", 
            "prog_done": "Total Progress: [{bar}] 100% ✓", "prog_retry_abort": "Total Progress: 🔁 Abort (Retry) ❌", 
            "prog_retry_done": "Total Progress: [{bar}] 100% ✨ (Completed)", 
            "dlg_overwrite_title": "File Already Exists", "dlg_overwrite_msg": "The file already exists on the FTP server:\n{path}\n\nWhat do you want to do?", 
            "dlg_confirm_exit_title": "Confirm Exit", "dlg_confirm_exit_msg": "A transfer is in progress.\nDo you want to cancel it and close the application?", 
            "btn_replace": "Replace", "btn_replace_always": "Always Replace", "btn_skip": "Skip", 
            "btn_cancel_proc": "Cancel Process", "msg_select_failed": "Select one or more files from the failed list.", 
            "msg_omitido": "(Skipped)", "msg_no_iniciado": "Not started (Cancelled)", "msg_interrumpido": "Interrupted", 
            "msg_cancelado": "Cancelled", "msg_subiendo": "Uploading: {prog}% (ETA: {eta})", 
            "msg_procesando": "Processing...", "msg_conectando": "Connecting...", "msg_streaming": "🚀 Streaming (RAM)...", 
            "msg_extracting": "📦 Extracting...", "msg_extracting_prog": "📦 Extracting: {cur}/{tot} MB ({pct}%)", 
            "msg_uploading_tree": "🚀 Uploading...", "msg_retrying_tree": "Retrying..."
        }
        
        self.lang_data = {"en-us": default_en}
        json_path = "i18n_zip_ftp.json"
        
        try:
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    self.lang_data = json.load(f)
            else:
                # Reconstruir automáticamente el archivo si falta usando el fallback inglés
                print("i18n file missing. Recreating with default English values.")
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(self.lang_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error loading i18n JSON: {e}")
            self.lang_data = {"en-us": default_en} # Forzar fallback si hay error en formato JSON

    def t(self, key, **kwargs):
        lang = getattr(self, "current_lang", "es-ar")
        # Si el idioma no existe, buscar el primer diccionario disponible que no sea de metadatos
        if lang not in self.lang_data or not isinstance(self.lang_data[lang], dict):
            fallback_options = [l for l in self.lang_data if isinstance(self.lang_data[l], dict)]
            if "es-ar" in fallback_options: lang = "es-ar"
            elif "es" in fallback_options: lang = "es"
            elif fallback_options: lang = fallback_options[0]
            else: return key
            
        if lang in self.lang_data and key in self.lang_data[lang]:
            text = self.lang_data[lang][key]
            if kwargs:
                try: 
                    return text.format(**kwargs)
                except: return text
            return text
        return key

    def switch_lang(self, new_lang):
        if self.is_running:
            messagebox.showwarning(self.t("dlg_confirm_exit_title"), "Please wait for current tasks to finish before changing language.")
            return

        if self.current_lang != new_lang:
            self.current_lang = new_lang
            self.save_config()
            
            # Guardar log actual
            old_log = ""
            if hasattr(self, 'log_text'):
                old_log = self.log_text.get(1.0, tk.END).strip()

            # Guardar contenido de las tablas de transferencia
            q_rows = [self.tree_queued.item(c, 'values') for c in self.tree_queued.get_children()] if hasattr(self, 'tree_queued') else []
            s_rows = [self.tree_success.item(c, 'values') for c in self.tree_success.get_children()] if hasattr(self, 'tree_success') else []
            f_rows_data = []
            if hasattr(self, 'tree_failed'):
                for c in self.tree_failed.get_children():
                    f_rows_data.append((self.tree_failed.item(c, 'values'), self.failed_data.get(c)))

            # Reiniciar UI para aplicar cambios
            for widget in self.root.winfo_children():
                widget.destroy()
            self.setup_ui()
            
            # Restaurar log
            if old_log:
                self.log_text.config(state='normal')
                self.log_text.insert(tk.END, old_log + "\n")
                self.log_text.see(tk.END)
                self.log_text.config(state='disabled')

            # Restaurar contenido de transferencias
            for vals in q_rows: self.tree_queued.insert("", tk.END, values=vals)
            for vals in s_rows: self.tree_success.insert("", tk.END, values=vals)
            
            self.failed_data = {}
            for vals, d_entry in f_rows_data:
                nid = self.tree_failed.insert("", tk.END, values=vals)
                if d_entry: self.failed_data[nid] = d_entry
            
            self.update_tab_titles()
                
            self.log(f"Language changed to: {new_lang.upper()}")
            
            # Si estábamos conectados, refrescar el árbol automáticamente
            if self.is_connected:
                host = self.ftp_host_var.get().strip()
                self.lbl_status_conn.config(text="🟢 " + self.t("status_connected", host=host), fg="green")
                self.btn_connect.config(text=self.t("btn_reconnect"))
                threading.Thread(target=self._connect_ftp_thread, args=(host, int(self.ftp_port_var.get())), daemon=True).start()

    def _find_external_tools(self):
        self.seven_zip_exe = shutil.which("7z.exe") or shutil.which("7z")
        self.unrar_exe = shutil.which("UnRAR.exe") or shutil.which("UnRAR")
        
        # Si no estan en el PATH, buscar en rutas comunes
        prog_files = [os.environ.get("ProgramFiles", "C:\\Program Files"), 
                      os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")]
        
        if not self.seven_zip_exe:
            for pf in prog_files:
                p = os.path.join(pf, "7-Zip", "7z.exe")
                if os.path.exists(p):
                    self.seven_zip_exe = p
                    break
        
        if not self.unrar_exe:
            for pf in prog_files:
                p = os.path.join(pf, "WinRAR", "UnRAR.exe")
                if os.path.exists(p):
                    self.unrar_exe = p
                    break
        
        # Configurar la libreria rarfile si encontramos UnRAR
        if RAR_SUPPORTED and self.unrar_exe:
            rarfile.UNRAR_TOOL = self.unrar_exe

    def get_archive_class(self, file_path):
        if file_path.lower().endswith('.rar'):
            if not RAR_SUPPORTED:
                raise Exception("Librería 'rarfile' no detectada. Ejecuta: pip install rarfile")
            if not self.seven_zip_exe and not self.unrar_exe:
                raise Exception("⚠️ No se encontró 7z.exe ni UnRAR.exe. Instala 7-Zip o WinRAR para abrir archivos .rar")
            return rarfile.RarFile
        return zipfile.ZipFile

    def setup_ui(self):
        self.root.title("PSZIP: ZIP/RAR to FTP Streamer (Pro Edition) by Eduardo B")
        pad = {'padx': 10, 'pady': 5}
        
        # --- File Selection (Sección 1) ---
        frame_file = tk.LabelFrame(self.root, text=self.t("section_file"))
        frame_file.pack(fill=tk.X, **pad)
        
        top_file_frame = tk.Frame(frame_file)
        top_file_frame.pack(fill=tk.X, expand=True)
        
        tk.Label(top_file_frame, text=self.t("select_zip")).pack(side=tk.LEFT, padx=5)
        self.zip_path_entry = tk.Entry(top_file_frame, textvariable=self.zip_path_var)
        self.zip_path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.btn_browse = tk.Button(top_file_frame, text=self.t("browse"), command=self.browse_file)
        self.btn_browse.pack(side=tk.LEFT, padx=5)

        # Selector de Idioma (Dinámico desde JSON)
        ui_langs = sorted([l.upper() for l in self.lang_data.keys() if isinstance(self.lang_data[l], dict)])
        lang_map = {l: l.lower() for l in ui_langs}
        inv_lang_map = {v: k for k, v in lang_map.items()}
        
        lang_var = tk.StringVar(value=inv_lang_map.get(self.current_lang, ui_langs[0]))
        self.lang_opt = tk.OptionMenu(top_file_frame, lang_var, *ui_langs, 
                                command=lambda v: self.switch_lang(lang_map[v]))
        self.lang_opt.config(width=6, font=("Arial", 8))
        self.lang_opt.pack(side=tk.LEFT, padx=5)

        # --- FTP Configuration (Sección 2) ---
        frame_ftp = tk.LabelFrame(self.root, text=self.t("section_conn"))
        frame_ftp.pack(fill=tk.X, **pad)
        
        grid_ftp = tk.Frame(frame_ftp)
        grid_ftp.pack(fill=tk.X, padx=5, pady=5)
        grid_ftp.columnconfigure(1, weight=1)
        
        tk.Label(grid_ftp, text=self.t("host")).grid(row=0, column=0, sticky=tk.W)
        self.host_entry = tk.Entry(grid_ftp, textvariable=self.ftp_host_var)
        self.host_entry.grid(row=0, column=1, sticky=tk.EW, padx=5)
        
        tk.Label(grid_ftp, text=self.t("port")).grid(row=0, column=2, sticky=tk.W)
        self.port_entry = tk.Entry(grid_ftp, textvariable=self.ftp_port_var, width=10)
        self.port_entry.grid(row=0, column=3, padx=5)
        
        tk.Label(grid_ftp, text=self.t("remote_path")).grid(row=1, column=0, sticky=tk.W)
        self.entry_dest = tk.Entry(grid_ftp, textvariable=self.ftp_dest_var)
        self.entry_dest.grid(row=1, column=1, columnspan=3, sticky=tk.EW, padx=5)
        
        self.btn_connect = tk.Button(grid_ftp, text=self.t("btn_reconnect") if self.is_connected else self.t("btn_connect"), command=self.connect_ftp, width=15)
        self.btn_connect.grid(row=0, column=4, padx=5, pady=5)
        
        self.tree_frame = tk.Frame(frame_ftp)
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.tree_dest = ttk.Treeview(self.tree_frame, selectmode='browse', show='tree', height=5)
        self.tree_dest.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.tree_scroll = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree_dest.yview)
        self.tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_dest.configure(yscrollcommand=self.tree_scroll.set)
        
        self.tree_dest.bind("<<TreeviewSelect>>", self.on_tree_select)
        self.tree_dest.bind("<<TreeviewOpen>>", self.on_tree_open)

        # --- Options ---
        frame_opts = tk.LabelFrame(self.root, text=self.t("section_proc"))
        frame_opts.pack(fill=tk.X, **pad)
        
        self.chk_root = tk.Checkbutton(frame_opts, text=self.t("create_root"), variable=self.create_root_var)
        self.chk_root.grid(row=0, column=0, columnspan=2, sticky=tk.W, padx=5, pady=5)
        
        self.chk_stream = tk.Checkbutton(frame_opts, text=self.t("streaming"), variable=self.streaming_var)
        self.chk_stream.grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        
        tk.Label(frame_opts, text=self.t("concurrent")).grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.spin_concurrent = tk.Spinbox(frame_opts, from_=1, to=20, textvariable=self.concurrent_var, width=5)
        self.spin_concurrent.grid(row=1, column=1, sticky=tk.W, padx=5)
        
        # --- Processing Controls ---
        ctrl_frame = tk.Frame(self.root)
        ctrl_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.btn_start = tk.Button(ctrl_frame, text=self.t("btn_start_init"), command=self.start_processing, bg="gray", fg="white", font=("Helvetica", 10, "bold"), state=tk.DISABLED)
        self.btn_start.pack(side=tk.TOP, fill=tk.X, expand=True)
        
        self.btn_cancel = tk.Button(ctrl_frame, text=self.t("btn_cancel"), command=self.cancel_processing, bg="#aa0000", fg="white", font=("Helvetica", 10, "bold"), state=tk.DISABLED)
        
        self.cancel_event = threading.Event()
        
        # --- Status Bar ---
        self.status_frame = tk.Frame(self.root, relief=tk.SUNKEN, bd=1)
        self.status_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        status_text = "🟢 " + self.t("status_connected", host=self.ftp_host_var.get()) if self.is_connected else self.t("status_disconnected")
        status_color = "green" if self.is_connected else "red"
        
        self.lbl_status_conn = tk.Label(self.status_frame, text=status_text, fg=status_color, font=("Helvetica", 9, "bold"))
        self.lbl_status_conn.pack(side=tk.LEFT, padx=5)
        self.lbl_status_speed = tk.Label(self.status_frame, text="", fg="blue", font=("Helvetica", 9))
        self.lbl_status_speed.pack(side=tk.RIGHT, padx=5)
        
        # --- Tabs ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True, **pad)
        
        self.tab_log = ttk.Frame(self.notebook)
        self.tab_queued = ttk.Frame(self.notebook)
        self.tab_success = ttk.Frame(self.notebook)
        self.tab_failed = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab_log, text=self.t("tab_log"))
        self.notebook.add(self.tab_queued, text=self.t("tab_queued", count=0))
        self.notebook.add(self.tab_success, text=self.t("tab_success", count=0))
        self.notebook.add(self.tab_failed, text=self.t("tab_failed", count=0))
        
        self.log_text = ScrolledText(self.tab_log, state='disabled', bg="black", fg="lightgreen", font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        cols = (self.t("col_local"), self.t("col_dest"), self.t("col_size"), self.t("col_status"))
        q_frame = tk.Frame(self.tab_queued)
        q_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.tree_queued = ttk.Treeview(q_frame, columns=cols, show='headings')
        for c in cols: self.tree_queued.heading(c, text=c)
        self.tree_queued.column(self.t("col_size"), width=100, anchor=tk.CENTER)
        self.tree_queued.column(self.t("col_status"), width=150, anchor=tk.W)
        self.tree_queued.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        q_scroll = ttk.Scrollbar(q_frame, orient="vertical", command=self.tree_queued.yview)
        q_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_queued.configure(yscrollcommand=q_scroll.set)
        
        cols_done = (self.t("col_local"), self.t("col_dest"), self.t("col_info"))
        s_frame = tk.Frame(self.tab_success)
        s_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.tree_success = ttk.Treeview(s_frame, columns=cols_done, show='headings')
        for c in cols_done: self.tree_success.heading(c, text=c)
        self.tree_success.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s_scroll = ttk.Scrollbar(s_frame, orient="vertical", command=self.tree_success.yview)
        s_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_success.configure(yscrollcommand=s_scroll.set)
        
        cols_failed = (self.t("col_local"), self.t("col_dest"), self.t("col_size"), self.t("col_info"))
        f_frame = tk.Frame(self.tab_failed)
        f_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.tree_failed = ttk.Treeview(f_frame, columns=cols_failed, show='headings')
        for c in cols_failed: self.tree_failed.heading(c, text=c)
        self.tree_failed.column(self.t("col_size"), width=100, anchor=tk.CENTER)
        self.tree_failed.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        f_scroll = ttk.Scrollbar(f_frame, orient="vertical", command=self.tree_failed.yview)
        f_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_failed.configure(yscrollcommand=f_scroll.set)
        self.tree_failed.bind("<Control-a>", self.select_all_failed)
        self.tree_failed.bind("<Button-3>", self.show_failed_menu)

        btn_failed_frame = tk.Frame(self.tab_failed)
        btn_failed_frame.pack(fill=tk.X, padx=5, pady=5)
        self.btn_retry_sel = tk.Button(btn_failed_frame, text=self.t("btn_retry_sel"), command=self.retry_selected)
        self.btn_retry_sel.pack(side=tk.LEFT, padx=5)
        self.btn_retry_all = tk.Button(btn_failed_frame, text=self.t("btn_retry_all"), command=self.retry_all_failed)
        self.btn_retry_all.pack(side=tk.LEFT, padx=5)
        self.btn_clear_failed = tk.Button(btn_failed_frame, text=self.t("btn_clear"), command=self.clear_failed)
        self.btn_clear_failed.pack(side=tk.RIGHT, padx=5)

        self.failed_data = {}
        
        self.failed_menu = tk.Menu(self.root, tearoff=0)
        self.failed_menu.add_command(label=self.t("menu_retry"), command=self.retry_selected)
        self.failed_menu.add_separator()
        self.failed_menu.add_command(label=self.t("menu_all"), command=self.select_all_failed)
        self.failed_menu.add_command(label=self.t("menu_clear"), command=self.clear_failed)

        # Restaurar estado dinámico tras reconstrucción
        if self.scanned_f > 0:
            self.check_can_start()

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r") as f:
                    data = json.load(f)
                    val_lang = data.get("lang", "es-ar")
                    # Migración de códigos vieos
                    if val_lang == "es": val_lang = "es-ar"
                    if val_lang == "en": val_lang = "en-us"
                    self.current_lang = val_lang
                    
                    if hasattr(self, "ftp_host_var"): self.ftp_host_var.set(data.get("host", ""))
                    if hasattr(self, "ftp_port_var"): self.ftp_port_var.set(str(data.get("port", "21")))
                    if hasattr(self, "ftp_dest_var"): self.ftp_dest_var.set(data.get("dest", "/"))
                    geometry = data.get("geometry", "")
                    if geometry: self.root.geometry(geometry)
                    if data.get("maximized", False):
                        try: self.root.state('zoomed')
                        except: pass
        except: pass

    def save_config(self):
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump({
                    "lang": self.current_lang,
                    "host": self.ftp_host_var.get().strip() if hasattr(self, "ftp_host_var") else "", 
                    "port": self.ftp_port_var.get().strip() if hasattr(self, "ftp_port_var") else "21",
                    "dest": self.ftp_dest_var.get().strip() if hasattr(self, "ftp_dest_var") else "/",
                    "geometry": self.root.geometry(),
                    "maximized": self.root.state() == 'zoomed'
                }, f)
        except: pass

    def update_tab_titles(self):
        self.notebook.tab(self.tab_queued, text=self.t("tab_queued", count=len(self.tree_queued.get_children())))
        self.notebook.tab(self.tab_success, text=self.t("tab_success", count=len(self.tree_success.get_children())))
        self.notebook.tab(self.tab_failed, text=self.t("tab_failed", count=len(self.tree_failed.get_children())))

    def browse_file(self):
        filepath = filedialog.askopenfilename(
            title=self.t("select_zip"),
            filetypes=[("Archives", "*.zip;*.rar"), ("All files", "*.*")]
        )
        if filepath:
            self.zip_path_var.set(filepath)
            self.log(f"🔍 {self.t('log_analyzing')} '{os.path.basename(filepath)}'...")
            self.btn_start.config(state=tk.DISABLED, text=self.t("log_analyzing"))
            threading.Thread(target=self._scan_archive_background, args=(filepath,), daemon=True).start()

    def _scan_archive_background(self, filepath):
        try:
            ArchiveClass = self.get_archive_class(filepath)
            f_count = 0
            d_count = 0
            with ArchiveClass(filepath, 'r') as zf:
                for zinfo in zf.infolist():
                    if zinfo.is_dir(): d_count += 1
                    else: f_count += 1
            
            self.scanned_f = f_count
            self.scanned_d = d_count
            self.root.after(0, self.check_can_start)
        except Exception as e:
            self.log(self.t("log_error", file=os.path.basename(filepath), msg=str(e)))
            self.scanned_f = 0
            self.scanned_d = 0
            self.root.after(0, self.check_can_start)

    def browse_file(self):
        filepath = filedialog.askopenfilename(
            title=self.t("select_zip"),
            filetypes=[("Archives", "*.zip;*.rar"), ("All files", "*.*")]
        )
        if filepath:
            self.zip_path_var.set(filepath)
            self.log(f"🔍 {self.t('log_analyzing')} '{os.path.basename(filepath)}'...")
            self.btn_start.config(state=tk.DISABLED, text=self.t("log_analyzing"))
            threading.Thread(target=self._scan_archive_background, args=(filepath,), daemon=True).start()

    def _scan_archive_background(self, filepath):
        try:
            ArchiveClass = self.get_archive_class(filepath)
            f_count = 0
            d_count = 0
            with ArchiveClass(filepath, 'r') as zf:
                for zinfo in zf.infolist():
                    if zinfo.is_dir(): d_count += 1
                    else: f_count += 1
            
            self.scanned_f = f_count
            self.scanned_d = d_count
            self.root.after(0, self.check_can_start)
        except Exception as e:
            self.log(f"⚠️ {self.t('log_error', file=os.path.basename(filepath), msg=str(e))}")
            self.scanned_f = 0
            self.scanned_d = 0
            self.root.after(0, self.check_can_start)

    def check_can_start(self):
        filepath = self.zip_path_var.get()
        if not filepath or not os.path.exists(filepath):
            self.btn_start.config(state=tk.DISABLED, bg="gray", text=self.t("btn_start_init"))
            return
            
        txt = self.t("btn_start_init")
        if self.scanned_f > 0:
            txt = self.t("btn_start_active", files=self.scanned_f, folders=self.scanned_d)
            
        if self.is_connected:
            self.btn_start.config(state=tk.NORMAL, bg="green", text=txt)
        else:
            txt_dis = f"{txt} - {self.t('host')}"
            self.btn_start.config(state=tk.DISABLED, bg="gray", text=txt_dis)

    def log(self, message):
        self.root.after(0, self._append_log, message)

    def _append_log(self, message):
        self.log_text.config(state='normal')
        try:
            last_line_start = self.log_text.index("end-2c linestart")
            last_line_text = self.log_text.get(last_line_start, "end-1c")
        except:
            last_line_text = ""
            last_line_start = tk.END
            
        prefix = self.t("prog_total")
        if last_line_text.startswith(prefix):
            self.log_text.insert(last_line_start, message + "\n")
        else:
            self.log_text.insert(tk.END, message + "\n")
            
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def progress_log(self, bar_str):
        self.root.after(0, self._update_progress_log, bar_str)

    def _update_progress_log(self, bar_str):
        self.log_text.config(state='normal')
        try:
            last_line_start = self.log_text.index("end-2c linestart")
            last_line_text = self.log_text.get(last_line_start, "end-1c")
        except:
            last_line_text = ""
            last_line_start = tk.END
            
        prefix = self.t("prog_total")
        if last_line_text.startswith(prefix):
            self.log_text.delete(last_line_start, "end-1c")
            self.log_text.insert(last_line_start, bar_str)
        else:
            self.log_text.insert(tk.END, bar_str + "\n")
            
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def connect_ftp(self):
        host = self.ftp_host_var.get().strip()
        try:
            port = int(self.ftp_port_var.get())
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", self.t("port") + " (1-65535)")
            return
            
        if not host:
            messagebox.showerror("Error", self.t("host") + " -> Error")
            return
            
        self.save_config()
        
        self.btn_connect.config(state=tk.DISABLED, text=self.t("msg_conectando"))
        self.btn_start.config(state=tk.DISABLED)
        # Mantener el texto actual si ya habia escaneado algo
        if self.scanned_f == 0:
            self.btn_start.config(bg="gray", text=self.t("status_connecting", host=host))
            
        self.entry_dest.config(state='disabled')
        self.is_connected = False
        self.lbl_status_conn.config(text="🟡 " + self.t("status_connecting", host="..."), fg="orange")
        self.log(self.t("status_connecting", host=f"{host}:{port}"))

        threading.Thread(target=self._connect_ftp_thread, args=(host, port), daemon=True).start()

    def _get_ftp_dirs(self, ftp, path):
        lines = []
        try:
            ftp.cwd(path)
            ftp.dir(lines.append)
        except Exception as e:
            return []
            
        dirs = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 9 and line.startswith('d'):
                dir_name = " ".join(parts[8:])
                if dir_name not in ('.', '..'):
                    dirs.append(dir_name)
        return dirs

    def _connect_ftp_thread(self, host, port):
        ftp = None
        try:
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=10)
            ftp.login('anonymous', '')
            self.log(f"✅ {self.t('status_connected', host=host)}")
            
            dirs = self._get_ftp_dirs(ftp, "/")
            
            def _success():
                self.entry_dest.config(state='normal')
                self.btn_connect.config(state=tk.NORMAL, text=self.t("btn_reconnect"))
                
                self.tree_dest.delete(*self.tree_dest.get_children())
                root_node = self.tree_dest.insert("", "end", text="/", open=True, values=("/",))
                for d in dirs:
                    node = self.tree_dest.insert(root_node, "end", text=d, values=(f"/{d}",))
                    self.tree_dest.insert(node, "end", text="dummy")
                
                self.is_connected = True
                self.lbl_status_conn.config(text="🟢 " + self.t("status_connected", host=host), fg="green")
                self.check_can_start()
            self.root.after(0, _success)
            
        except Exception as e:
            self.log(f"❌ {self.t('ftp_error')}: {e}")
            def _fail():
                self.lbl_status_conn.config(text="🔴 " + self.t("status_disconnected"), fg="red")
                self.btn_connect.config(state=tk.NORMAL, text=self.t("btn_connect"))
                messagebox.showerror(self.t("ftp_error"), f"{self.t('status_disconnected')}:\n{e}")
            self.root.after(0, _fail)
        finally:
            if ftp is not None:
                try: ftp.quit()
                except: ftp.close()

    def toggle_ui(self, running):
        self.is_running = running
        state = tk.DISABLED if running else tk.NORMAL
        def _update():
            if not running and self.is_connected:
                self.check_can_start()
            elif running:
                self.btn_start.config(state=tk.DISABLED, bg="gray", text=self.t("msg_procesando"))
                
            if running:
                self.btn_start.pack_forget()
                self.btn_cancel.pack(side=tk.TOP, fill=tk.X, expand=True)
                self.btn_cancel.config(state=tk.NORMAL)
            else:
                self.btn_cancel.pack_forget()
                self.btn_start.pack(side=tk.TOP, fill=tk.X, expand=True)
                
            self.btn_connect.config(state=state)
            if not running and self.is_connected:
                self.tree_dest.state(['!disabled'])
                self.entry_dest.config(state=tk.NORMAL)
            else:
                self.tree_dest.state(['disabled'])
                self.entry_dest.config(state=tk.DISABLED)
            
            # Bloquear opciones de procesamiento estructurales (pero dejar concurrencia libre)
            if hasattr(self, 'chk_root'):
                self.chk_root.config(state=state)
                self.chk_stream.config(state=state)
            if hasattr(self, 'lang_opt'):
                self.lang_opt.config(state=state)
            if hasattr(self, 'zip_path_entry'):
                self.zip_path_entry.config(state=state)
                self.btn_browse.config(state=state)
                self.host_entry.config(state=state)
                self.port_entry.config(state=state)
            
            # Botones de las pestañas
            if hasattr(self, 'btn_retry_sel'):
                self.btn_retry_sel.config(state=state)
                self.btn_retry_all.config(state=state)
                self.btn_clear_failed.config(state=state)
        self.root.after(0, _update)

    def on_tree_select(self, event):
        if self.is_running: return
        selection = self.tree_dest.selection()
        if not selection: return
        item_id = selection[0]
        path = self.tree_dest.item(item_id, 'values')[0]
        
        self.ftp_dest_var.set(path)
        
        children = self.tree_dest.get_children(item_id)
        if len(children) == 1 and self.tree_dest.item(children[0], 'text') == 'dummy':
            self.tree_dest.delete(children[0])
            threading.Thread(target=self._load_subdirs, args=(item_id, path), daemon=True).start()

    def on_tree_open(self, event):
        if self.is_running: return
        def check_open_nodes(node=""):
            for child in self.tree_dest.get_children(node):
                if self.tree_dest.item(child, 'open'):
                    subchildren = self.tree_dest.get_children(child)
                    if len(subchildren) == 1 and self.tree_dest.item(subchildren[0], 'text') == 'dummy':
                        path = self.tree_dest.item(child, 'values')[0]
                        self.tree_dest.delete(subchildren[0])
                        threading.Thread(target=self._load_subdirs, args=(child, path), daemon=True).start()
                    check_open_nodes(child)
        check_open_nodes()

    def _load_subdirs(self, item_id, path):
        host = self.ftp_host_var.get().strip()
        try: port = int(self.ftp_port_var.get())
        except: return
        
        ftp = None
        dirs = []
        try:
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=10)
            ftp.login('anonymous', '')
            dirs = self._get_ftp_dirs(ftp, path)
        except Exception as e:
            self.log(self.t("log_error", file=path, msg=str(e)))
            return
        finally:
            if ftp:
                try: ftp.quit()
                except: ftp.close()
                
        def _update_tree():
            if not self.tree_dest.exists(item_id): return
            children = self.tree_dest.get_children(item_id)
            for c in children:
                if self.tree_dest.item(c, 'text') == 'dummy':
                    self.tree_dest.delete(c)
            for d in dirs:
                full_path = f"{path}/{d}" if path != "/" else f"/{d}"
                node = self.tree_dest.insert(item_id, "end", text=d, values=(full_path,))
                self.tree_dest.insert(node, "end", text="dummy")
                
        self.root.after(0, _update_tree)

    def start_processing(self):
        if self.is_running or not self.is_connected:
            return
            
        zip_path = self.zip_path_var.get()
        host = self.ftp_host_var.get().strip()
        port = int(self.ftp_port_var.get())
        remote_dest = self.ftp_dest_var.get().strip()
        
        if not zip_path or not os.path.exists(zip_path):
            messagebox.showerror("Error", "¡Debes seleccionar un archivo válido!")
            return

        self.log_text.config(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state='disabled')
        
        for t in (self.tree_queued, self.tree_success, self.tree_failed):
            for row in t.get_children(): t.delete(row)
        self.update_tab_titles()
        self.notebook.select(self.tab_log)
        
        self.cancel_event.clear()
        self.toggle_ui(True)
        self.log(self.t("log_validating"))
        
        threading.Thread(target=self._validate_and_start_thread, args=(zip_path, host, port, remote_dest), daemon=True).start()

    def cancel_processing(self):
        self.log(f"⚠️ {self.t('log_cancel_request')}")
        self.cancel_event.set()
        self.btn_cancel.config(state=tk.DISABLED)

    def _validate_and_start_thread(self, zip_path, host, port, remote_dest):
        ftp = None
        valid = False
        try:
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=10)
            ftp.login('anonymous', '')
            try:
                ftp.cwd(remote_dest)
                valid = True
            except: pass
        except Exception as e:
            self.log(self.t("log_error", file="FTP", msg=str(e)))
        finally:
            if ftp:
                try: ftp.quit()
                except: ftp.close()
                
        if not valid:
            self.log(f"❌ {self.t('log_error', file=remote_dest, msg=self.t('remote_path') + ' -> Error')}")
            def _fail():
                messagebox.showerror("Error", self.t("log_error", file=remote_dest, msg="Path Error"))
                self.toggle_ui(False)
            self.root.after(0, _fail)
            return

        self.process_master(zip_path, host, port, self.create_root_var.get(), self.concurrent_var.get(), remote_dest)

    def process_master(self, zip_path, host, port, create_root, max_workers, remote_dest):
        try:
            self.total_bytes = 0
            self.uploaded_bytes = 0
            ArchiveClass = self.get_archive_class(zip_path)
            
            ftp_main = ftplib.FTP()
            ftp_main.connect(host, port)
            ftp_main.login('anonymous', '')

            base_dir = remote_dest
            if base_dir.endswith("/"): base_dir = base_dir[:-1]
                
            if create_root:
                basename = os.path.basename(zip_path)
                zip_root = os.path.splitext(basename)[0].replace(" ", "_")
                base_dir = f"{base_dir}/{zip_root}" if base_dir else f"/{zip_root}"

            self.log(self.t("log_root_info", path=(base_dir if base_dir else '/')))
            self.log(f"📦 {self.t('log_analyzing')}")
            
            directories_to_create = set()
            files_to_upload = []

            with ArchiveClass(zip_path, 'r') as zf:
                for zinfo in zf.infolist():
                    internal_path = zinfo.filename
                    dest_path = f"{base_dir}/{internal_path}" if base_dir else internal_path
                    dest_path = dest_path.replace("\\", "/").replace("//", "/")

                    if zinfo.is_dir():
                        directories_to_create.add(dest_path)
                    else:
                        files_to_upload.append((internal_path, dest_path))
                        parent_dir = os.path.dirname(dest_path)
                        if parent_dir:
                            directories_to_create.add(parent_dir)

            sorted_dirs = sorted(list(directories_to_create), key=lambda x: x.count('/'))
            if sorted_dirs:
                self.log(self.t("log_creating_folders", count=len(sorted_dirs)))
                created_cache = set()
                for d in sorted_dirs:
                    if d in ['/', '']: continue
                    parts = d.split('/')
                    current = ""
                    is_absolute = d.startswith('/')
                    for part in parts:
                        if not part: continue
                        current = ("/" + part if is_absolute else part) if current == "" else current + "/" + part
                        if current not in created_cache:
                            try:
                                ftp_main.mkd(current)
                                created_cache.add(current)
                            except: pass
                self.log(f"✅ {self.t('log_folders_ready')}")
            
            ftp_main.quit()
            
            self.current_queue = []
            with ArchiveClass(zip_path, 'r') as zf:
                for item in files_to_upload:
                    info = zf.getinfo(item[0])
                    self.total_bytes += info.file_size
                    self.current_queue.append((item[0], item[1], info.file_size))
            
            self.current_idx_ptr = 0
            def _populate():
                for i, (local, dest, size) in enumerate(self.current_queue):
                    size_mb = f"{(size / (1024*1024)):.2f} MB"
                    self.tree_queued.insert("", tk.END, iid=str(i), values=(local, dest, size_mb, self.t("status_pending")))
                self.update_tab_titles()
                self.log(f"🚀 {self.t('log_starting_upload', count=len(self.current_queue), size=f'{(self.total_bytes / (1024*1024)):.2f} MB')}")
            self.root.after(0, _populate)
            
            self.uploaded_bytes = 0
            self.start_time = time.time()
            self.show_progress = True
            self.overwrite_policy = "ASK"
            self.root.after(0, self._update_speed_ui)

            success_count = 0
            fail_count = 0
            futures_map = {}

            # Usamos un executor con un máximo de hilos físico holgado (p.ej. 50)
            # pero el bucle controlará cuántos se mandan realmente basándose en el Spinbox.
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                while (self.current_idx_ptr < len(self.current_queue) or futures_map) and not self.cancel_event.is_set():
                    # Leer límite dinámico desde la UI
                    current_max = self.concurrent_var.get()
                    
                    # Mandar nuevas tareas si hay cupo
                    while self.current_idx_ptr < len(self.current_queue) and len(futures_map) < current_max and not self.cancel_event.is_set():
                        item = self.current_queue[self.current_idx_ptr]
                        iid = str(self.current_idx_ptr)
                        f = executor.submit(self.upload_worker, zip_path, host, port, item[0], item[1], item[2], iid)
                        futures_map[f] = (item, iid)
                        self.current_idx_ptr += 1
                        
                    if not futures_map and self.current_idx_ptr >= len(self.current_queue):
                        break
                        
                    # Esperar un instante corto para permitir que el bucle revise el límite de la UI
                    done, not_done = concurrent.futures.wait(futures_map.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                    
                    for f in done:
                        item_orig, iid = futures_map.pop(f)
                        try:
                            success, dest_path, err_msg, file_path_local, size_in_bytes, _ = f.result()
                        except Exception as e:
                            success, dest_path, err_msg, file_path_local, size_in_bytes = False, "Error", str(e), "Error", 0
                            
                        if success:
                            success_count += 1
                            self.log(f"[OK] {dest_path}")
                            def _mv_ok(d=iid, lg=file_path_local, dst=dest_path, sz=size_in_bytes):
                                if self.tree_queued.exists(d): self.tree_queued.delete(d)
                                size_mb = f"{(sz / (1024*1024)):.2f} MB"
                                self.tree_success.insert("", tk.END, values=(lg, dst, size_mb))
                                self.update_tab_titles()
                            self.root.after(0, _mv_ok)
                        else:
                            fail_count += 1
                            self.log(self.t("log_error", file=file_path_local, msg=err_msg))
                            def _mv_err(d=iid, lg=file_path_local, dst=dest_path, e=err_msg, sz=size_in_bytes, it_o=item_orig):
                                if self.tree_queued.exists(d): self.tree_queued.delete(d)
                                sz_mb = f"{(sz / (1024*1024)):.2f} MB"
                                nid = self.tree_failed.insert("", tk.END, values=(lg, dst, sz_mb, e))
                                self.failed_data[nid] = (it_o[0], it_o[1], sz) 
                                self.update_tab_titles()
                            self.root.after(0, _mv_err)

                if self.cancel_event.is_set():
                    self.log(self.t("log_cancel_request"))
                    # Cancelar tareas en espera en el executor
                    for f in futures_map.keys(): f.cancel()
                    
                    def _final_cancel_sweep():
                        # Primero sumamos los que estaban en vuelo (en el futures_map)
                        # y luego barremos todo lo que quede físicamente en la tabla.
                        nonlocal fail_count
                        for d in self.tree_queued.get_children():
                            if not self.tree_queued.exists(d): continue
                            fail_count += 1
                            vals = list(self.tree_queued.item(d, 'values'))
                            local_path, dest_path, sz_str, _ = vals
                            self.tree_queued.delete(d)
                            
                            # Recuperar datos originales para permitir reintento posterior
                            it_data = None
                            try:
                                idx_lookup = int(d)
                                it_data = self.current_queue[idx_lookup]
                            except: pass
                            
                            nid = self.tree_failed.insert("", tk.END, values=(local_path, dest_path, sz_str, self.t("msg_interrumpido")))
                            if it_data: self.failed_data[nid] = (it_data[0], it_data[1], it_data[2])
                        self.update_tab_titles()
                    
                    self.root.after(0, _final_cancel_sweep)

            self.show_progress = False
            if self.cancel_event.is_set():
                self.progress_log(self.t("prog_abort"))
            else:
                self.progress_log(self.t("prog_done", bar="█"*30))
            
            self.log("-" * 40)
            self.log(self.t("log_finished"))
            self.log(self.t("log_summary", success=success_count, fail=fail_count, total=len(self.current_queue)))
            
        except Exception as e:
            self.log(self.t("log_error", file="MASTER", msg=str(e)))
        finally:
            self.toggle_ui(False)
            self.root.after(0, lambda: self.lbl_status_speed.config(text=""))
            self.root.after(0, lambda: self.btn_cancel.config(state=tk.DISABLED))

    def _update_speed_ui(self):
        if not self.is_running:
            self.lbl_status_speed.config(text="")
            return
            
        elapsed = time.time() - self.start_time
        speed = 0
        if elapsed > 1: # Esperar al menos 1 seg para promediar
            speed = self.uploaded_bytes / elapsed
            speed_mb = speed / (1024*1024)
            self.lbl_status_speed.config(text=self.t("status_speed", mb=speed_mb))
            
        if self.total_bytes > 0 and getattr(self, 'show_progress', False):
            # Usar max(1, uploaded_bytes) para evitar división por cero o estados extraños al inicio
            percent = (self.uploaded_bytes / self.total_bytes) * 100
            if percent > 100: percent = 100
            
            eta_str = "--"
            if speed > 1024: # Al menos 1KB/s para calcular ETA
                remaining_bytes = self.total_bytes - self.uploaded_bytes
                if remaining_bytes > 0:
                    eta_secs = remaining_bytes / speed
                    if eta_secs > 3600:
                        eta_str = f"ETA: {int(eta_secs // 3600)}h {int((eta_secs % 3600) // 60)}m"
                    elif eta_secs > 60:
                        eta_str = f"ETA: {int(eta_secs // 60)}m {int(eta_secs % 60)}s"
                    else:
                        eta_str = f"ETA: {int(eta_secs)}s"

            percent = min(100.0, (self.uploaded_bytes / self.total_bytes * 100)) if self.total_bytes > 0 else 0
            
            bar_length = 30
            filled = int(bar_length * percent / 100)
            bar = "█" * filled + "▓" * (bar_length - filled)
            spinner = self.spinner_chars[self.spinner_idx % len(self.spinner_chars)]
            self.spinner_idx += 1
            bar_str = f"{self.t('prog_total')} [{bar}] {percent:.1f}% {spinner} {eta_str}"
            self.progress_log(bar_str)
                    
        self.root.after(400, self._update_speed_ui)

    def upload_worker(self, zip_path, host, port, internal_path, dest_path, size, iid):
        ftp = None
        temp_dir = None
        try:
            with self.policy_lock: policy = self.overwrite_policy
            if policy == "CANCEL" or self.cancel_event.is_set(): 
                return False, dest_path, self.t("log_cancel_user"), internal_path, size, iid
                
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=15)
            ftp.login('anonymous', '')
            
            exists = False
            try:
                if ftp.size(dest_path) is not None:
                    exists = True
            except: pass
                
            if exists:
                if policy == "ASK":
                    policy = self._ask_overwrite_policy(dest_path)
                if policy == "CANCEL" or self.cancel_event.is_set():
                    return False, dest_path, self.t("log_cancel_user"), internal_path, size, iid
                elif policy == "SKIP":
                    with self.bytes_lock:
                        self.uploaded_bytes += size
                    return False, dest_path, self.t("msg_omitido"), internal_path, size, iid
            
            uploaded_for_this = 0
            start_time_file = time.time()
            
            def block_callback(data):
                if self.cancel_event.is_set():
                    raise Exception("Transferencia abortada por el usuario")
                    
                nonlocal uploaded_for_this
                chunk_len = len(data)
                with self.bytes_lock:
                    self.uploaded_bytes += chunk_len
                uploaded_for_this += chunk_len
                
                # Actualizar progreso individual cada 400ms o fin de archivo
                if time.time() - block_callback.last_update > 0.4:
                    block_callback.last_update = time.time()
                    if size > 0:
                        pct = (uploaded_for_this / size) * 100
                        elapsed = time.time() - start_time_file
                        eta_txt = "..."
                        if elapsed > 0.5 and uploaded_for_this > 0:
                            speed_file = uploaded_for_this / elapsed
                            eta_secs = (size - uploaded_for_this) / speed_file
                            if eta_secs > 60:
                                eta_txt = f"{int(eta_secs//60)}m {int(eta_secs%60)}s"
                            else:
                                eta_txt = f"{int(eta_secs)}s"
                        
                        status_str = self.t("msg_subiendo", prog=f"{pct:.1f}", eta=eta_txt)
                        def _update():
                            if self.tree_queued.exists(iid):
                                vals = list(self.tree_queued.item(iid, 'values'))
                                vals[3] = status_str
                                self.tree_queued.item(iid, values=vals)
                        self.root.after(0, _update)

            block_callback.last_update = 0
            
            is_rar = zip_path.lower().endswith('.rar')
            do_stream = self.streaming_var.get()
            
            # --- MODO 1: Streaming Directo (Eficacia Máxima, 0 Disco) ---
            if do_stream:
                if is_rar:
                    archive_abs = os.path.abspath(zip_path)
                    
                    if self.seven_zip_exe:
                        cmd = [self.seven_zip_exe, "x", "-so", "-y", archive_abs, internal_path]
                    elif self.unrar_exe:
                        cmd = [self.unrar_exe, "p", "-inul", "-y", archive_abs, internal_path]
                    else:
                        raise Exception("❌ No hay herramientas para streaming de RAR (instala 7-Zip)")
                    
                    self.log(f"🚀 {os.path.basename(internal_path)}: {self.t('msg_streaming')}")
                    
                    def _set_up_rar_stream():
                        if self.tree_queued.exists(iid):
                            vals = list(self.tree_queued.item(iid, 'values'))
                            vals[3] = self.t("msg_streaming")
                            self.tree_queued.item(iid, values=vals)
                    self.root.after(0, _set_up_rar_stream)

                    si = subprocess.STARTUPINFO()
                    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    si.wShowWindow = subprocess.SW_HIDE
                    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                            startupinfo=si, creationflags=subprocess.CREATE_NO_WINDOW)
                    try:
                        ftp.storbinary(f"STOR {dest_path}", proc.stdout, blocksize=1048576, callback=block_callback)
                        proc.wait(timeout=10)
                    except Exception as e:
                        proc.kill()
                        raise e
                else:
                    # Streaming ZIP
                    def _set_stream_zip():
                        if self.tree_queued.exists(iid):
                            vals = list(self.tree_queued.item(iid, 'values'))
                            vals[3] = self.t("msg_streaming")
                            self.tree_queued.item(iid, values=vals)
                    self.root.after(0, _set_stream_zip)

                    ArchiveClass = self.get_archive_class(zip_path)
                    with ArchiveClass(zip_path, 'r') as zf:
                        with zf.open(internal_path) as source_file:
                            ftp.storbinary(f"STOR {dest_path}", source_file, blocksize=1048576, callback=block_callback)
            
            # --- MODO 2: Extracción a Disco (Modo Clásico) ---
            else:
                temp_dir = tempfile.mkdtemp(prefix="pszip_classic_")
                archive_abs = os.path.abspath(zip_path)
                local_file = os.path.join(temp_dir, os.path.normpath(internal_path))
                
                try:
                    def _update_tree_status(text):
                        if self.tree_queued.exists(iid):
                            vals = list(self.tree_queued.item(iid, 'values'))
                            vals[3] = text
                            self.tree_queued.item(iid, values=vals)

                    self.log(f"📦 {os.path.basename(internal_path)}: {self.t('msg_extracting')}")
                    
                    if is_rar:
                        if self.seven_zip_exe:
                            cmd = [self.seven_zip_exe, "x", "-y", f"-o{temp_dir}", archive_abs, internal_path]
                        elif self.unrar_exe:
                            cmd = [self.unrar_exe, "x", "-inul", "-y", archive_abs, internal_path, temp_dir + os.sep]
                        else:
                            raise Exception("❌ No binary for RAR extraction.")
                        
                        si = subprocess.STARTUPINFO()
                        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                        si.wShowWindow = subprocess.SW_HIDE
                        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                                startupinfo=si, creationflags=subprocess.CREATE_NO_WINDOW)
                        while proc.poll() is None:
                            if self.cancel_event.is_set():
                                proc.kill()
                                raise Exception("Parado")
                            
                            # Polling de tamaño para progreso local
                            if size > 0 and os.path.exists(local_file):
                                cur_s = os.path.getsize(local_file)
                                pct = (cur_s / size) * 100
                                mb_cur = cur_s / (1024*1024)
                                mb_tot = size / (1024*1024)
                                pct = (cur_s / size) * 100
                                
                                msg = self.t("msg_extracting_prog", cur=f"{mb_cur:.2f}", tot=f"{mb_tot:.2f}", pct=f"{pct:.1f}")
                                self.root.after(0, lambda m=msg: _update_tree_status(m))
                            
                            time.sleep(0.5)
                            
                        if proc.returncode != 0:
                            raise Exception(f"Falla de extracción (Error {proc.returncode})")
                    else:
                        # ZIP classic extraction
                        self.root.after(0, lambda: _update_tree_status(self.t("msg_extracting")))
                        with zipfile.ZipFile(zip_path, 'r') as zf:
                            zf.extract(internal_path, temp_dir)
                    
                    self.root.after(0, lambda: _update_tree_status(self.t("msg_uploading_tree")))
                    with open(local_file, 'rb') as f:
                        ftp.storbinary(f"STOR {dest_path}", f, blocksize=1048576, callback=block_callback)
                finally:
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
            
            return True, dest_path, "", internal_path, size, iid
            
        except Exception as e:
            if "abortada" in str(e).lower() or self.cancel_event.is_set():
                return False, dest_path, self.t("log_cancel_user"), internal_path, size, iid
            return False, dest_path, f"Error: {str(e)}", internal_path, size, iid
        finally:
            if ftp is not None:
                try: ftp.quit()
                except: ftp.close()
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def select_all_failed(self, event=None):
        self.tree_failed.selection_set(self.tree_failed.get_children())
        return "break" # Prevent default event propagation

    def show_failed_menu(self, event):
        # Seleccionar el item bajo el cursor si no hay selección
        iid = self.tree_failed.identify_row(event.y)
        if iid and iid not in self.tree_failed.selection():
            self.tree_failed.selection_set(iid)
        self.failed_menu.post(event.x_root, event.y_root)

    def clear_failed(self):
        self.tree_failed.delete(*self.tree_failed.get_children())
        self.failed_data = {}
        self.update_tab_titles()

    def retry_selected(self):
        selection = self.tree_failed.selection()
        if not selection:
            messagebox.showinfo("INFO", self.t("msg_select_failed"))
            return
        
        items_to_retry = []
        for iid in selection:
            if iid in self.failed_data:
                items_to_retry.append(self.failed_data[iid])
                self.tree_failed.delete(iid)
                del self.failed_data[iid]
        
        if items_to_retry:
            self._start_retry_process(items_to_retry)

    def retry_all_failed(self):
        items_to_retry = list(self.failed_data.values())
        if not items_to_retry:
            return
            
        self.tree_failed.delete(*self.tree_failed.get_children())
        self.failed_data = {}
        self._start_retry_process(items_to_retry)

    def _start_retry_process(self, items):
        self.cancel_event.clear()
        self.overwrite_policy = "ASK" # Resetear política de sobreescritura por si se canceló antes
        if self.is_running:
            self.log(self.t("log_retrying", count=len(items)))
            zip_path = self.zip_path_var.get()
            host = self.ftp_host_var.get().strip()
            port = int(self.ftp_port_var.get())
            
            # Asegurar que el botón de cancelar sea clicable (por si se deshabilito tras un error previo)
            self.root.after(0, lambda: self.btn_cancel.config(state=tk.NORMAL))
            self.show_progress = True
            
            def _live_populate():
                start_at = len(self.current_queue)
                for i, (local, dest, size) in enumerate(items):
                    # Agregamos al final de la cola activa
                    self.current_queue.append((local, dest, size))
                    self.total_bytes += size
                    
                    real_idx = start_at + i
                    size_mb = f"{(size / (1024*1024)):.2f} MB"
                    self.tree_queued.insert("", tk.END, iid=str(real_idx), values=(local, dest, size_mb, self.t("msg_retrying_tree")))
                self.update_tab_titles()
            self.root.after(0, _live_populate)
            return

        self.process_retry(items)

    def process_retry(self, items):
        self.log(self.t("log_retrying", count=len(items)))
        for i, it in enumerate(items):
            self.log(self.t("log_retry_item", idx=i+1, name=os.path.basename(it[0])))
        
        # Reset state for master
        self.cancel_event.clear()
        self.toggle_ui(True)
        self.notebook.select(self.tab_log)
        
        zip_path = self.zip_path_var.get()
        host = self.ftp_host_var.get().strip()
        port = int(self.ftp_port_var.get())
        
        threading.Thread(target=self._retry_master_thread, args=(zip_path, host, port, items), daemon=True).start()

    def _retry_master_thread(self, zip_path, host, port, items):
        try:
            self.total_bytes = 0
            self.uploaded_bytes = 0
            # Sincronizar con class members
            self.current_queue = items
            self.current_idx_ptr = 0
            
            # Recalcular total_bytes para reintento
            for it in items:
                self.total_bytes += it[2] # size_in_bytes
            
            def _populate():
                for i, (local, dest, size) in enumerate(self.current_queue):
                    size_mb = f"{(size / (1024*1024)):.2f} MB"
                    self.tree_queued.insert("", tk.END, iid=str(i), values=(local, dest, size_mb, "Reintentando..."))
                self.update_tab_titles()
            self.root.after(0, _populate)
            
            self.total_bytes = sum(it[2] for it in items)
            self.uploaded_bytes = 0
            self.start_time = time.time()
            self.show_progress = True
            
            # Re-activar el monitor de UI para el proceso de reintento
            self.root.after(0, self._update_speed_ui)
            
            success_count = 0
            fail_count = 0
            futures_map = {}
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                while (self.current_idx_ptr < len(self.current_queue) or futures_map) and not self.cancel_event.is_set():
                    current_max = self.concurrent_var.get()
                    while self.current_idx_ptr < len(self.current_queue) and len(futures_map) < current_max and not self.cancel_event.is_set():
                        item = self.current_queue[self.current_idx_ptr]
                        iid = str(self.current_idx_ptr)
                        f = executor.submit(self.upload_worker, zip_path, host, port, item[0], item[1], item[2], iid)
                        futures_map[f] = (item, iid)
                        self.current_idx_ptr += 1
                        
                    if not futures_map and self.current_idx_ptr >= len(self.current_queue): break
                    done, _ = concurrent.futures.wait(futures_map.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                    
                    for f in done:
                        orig_it, iid = futures_map.pop(f)
                        try:
                            success, dest_path, err_msg, local_path, size, _ = f.result()
                        except Exception as e:
                            success, dest_path, err_msg, local_path, size = False, "Err", str(e), "Err", 0
                            
                        if success:
                            success_count += 1
                            self.log(self.t("log_ok_retry", path=dest_path))
                            def _mv_ok(d=iid, lg=local_path, dst=dest_path, sz=size):
                                if self.tree_queued.exists(d): self.tree_queued.delete(d)
                                self.tree_success.insert("", tk.END, values=(lg, dst, f"{(sz / (1024*1024)):.2f} MB"))
                                self.update_tab_titles()
                            self.root.after(0, _mv_ok)
                        else:
                            fail_count += 1
                            self.log(self.t("log_fail_retry", path=local_path, msg=err_msg))
                            def _mv_err(d=iid, lg=local_path, dst=dest_path, e=err_msg, sz=size, it_o=orig_it):
                                if self.tree_queued.exists(d): self.tree_queued.delete(d)
                                sz_mb = f"{(sz / (1024*1024)):.2f} MB"
                                nid = self.tree_failed.insert("", tk.END, values=(lg, dst, sz_mb, e))
                                self.failed_data[nid] = it_o
                                self.update_tab_titles()
                            self.root.after(0, _mv_err)

                # Si se canceló, mover pendientes a la lista de fallidos
                if self.cancel_event.is_set():
                    self.log(self.t("log_cancel_request"))
                    def _final_retry_cancel_sweep():
                        nonlocal fail_count
                        for d in self.tree_queued.get_children():
                            if not self.tree_queued.exists(d): continue
                            fail_count += 1
                            vals = list(self.tree_queued.item(d, 'values'))
                            self.tree_queued.delete(d)
                            
                            # Intentar recuperar data original
                            it_data = None
                            try:
                                idx = int(d)
                                it_data = self.current_queue[idx]
                            except: pass
                            
                            nid = self.tree_failed.insert("", tk.END, values=(vals[0], vals[1], vals[2], self.t("msg_interrumpido")))
                            if it_data: self.failed_data[nid] = it_data
                        self.update_tab_titles()
                    
                    self.root.after(0, _final_retry_cancel_sweep)

            self.show_progress = False
            if self.cancel_event.is_set():
                self.progress_log(self.t("prog_retry_abort"))
            else:
                self.progress_log(self.t("prog_retry_done", bar="█"*30))
            self.log(self.t("log_finished"))
            self.log(self.t("log_summary", success=success_count, fail=fail_count, total=len(items)))
        except Exception as e:
            self.log(f"Error: {e}")
        finally:
            self.toggle_ui(False)

    def _ask_overwrite_policy(self, filepath):
        with self.prompt_lock:
            if self.overwrite_policy != "ASK": return self.overwrite_policy
            self.policy_choice = None
            self.policy_event.clear()
            self.root.after(0, self._show_overwrite_dialog, filepath)
            self.policy_event.wait()
            return self.policy_choice

    def _show_overwrite_dialog(self, filepath):
        dlg = tk.Toplevel(self.root)
        dlg.title(self.t("dlg_overwrite_title"))
        dlg.geometry("500x180")
        dlg.transient(self.root)
        dlg.grab_set()
        
        self.root.update_idletasks()
        w_dlg, h_dlg = 500, 180
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (w_dlg // 2)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (h_dlg // 2)
        dlg.geometry(f"{w_dlg}x{h_dlg}+{x}+{y}")
        
        msg = self.t("dlg_overwrite_msg", path=filepath)
        tk.Label(dlg, text=msg, wraplength=450).pack(pady=10)
        btn_frame = tk.Frame(dlg)
        btn_frame.pack(pady=5)
        
        def set_pol(choice, global_pol=None):
            if global_pol: self.overwrite_policy = global_pol
            self.policy_choice = choice
            self.policy_event.set()
            dlg.destroy()
            
        tk.Button(btn_frame, text=self.t("btn_replace"), command=lambda: set_pol("OVERWRITE")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text=self.t("btn_replace_always"), command=lambda: set_pol("OVERWRITE", "OVERWRITE")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text=self.t("btn_skip"), command=lambda: set_pol("SKIP")).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text=self.t("btn_cancel_proc"), command=lambda: set_pol("CANCEL", "CANCEL")).pack(side=tk.LEFT, padx=5)
        
        dlg.protocol("WM_DELETE_WINDOW", lambda: set_pol("SKIP"))

if __name__ == "__main__":
    root = tk.Tk()
    app = ZipToFtpApp(root)
    root.mainloop()

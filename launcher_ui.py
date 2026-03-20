import tkinter as tk
from tkinter import messagebox, scrolledtext
import subprocess
import os
import sys
import webbrowser
import threading
import time


class AgentLauncherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Sliq Agent Launcher")
        self.root.geometry("560x460")
        self.root.configure(padx=20, pady=20, bg="#1e1e2e")
        self.root.resizable(False, False)
        self.root.protocol("WM_DELETE_WINDOW", self.on_window_close)
        self.process = None

        # ── Title ──────────────────────────────────────────────────
        tk.Label(
            root, text="Sliq — BSPL Agent Node Launcher",
            font=("Segoe UI", 13, "bold"),
            bg="#1e1e2e", fg="#cdd6f4"
        ).pack(pady=(0, 18))

        # ── Input frame ────────────────────────────────────────────
        frm = tk.Frame(root, bg="#1e1e2e")
        frm.pack(fill="x")

        def label(text, row):
            tk.Label(frm, text=text, bg="#1e1e2e", fg="#a6adc8",
                     font=("Segoe UI", 10), anchor="w").grid(
                row=row, column=0, sticky="w", pady=6, padx=(0, 12))

        label("Agent Name:", 0)
        self.agent_name_var = tk.StringVar(value="Alice")
        tk.Entry(frm, textvariable=self.agent_name_var, width=28,
                 font=("Segoe UI", 10), bg="#313244", fg="#cdd6f4",
                 insertbackground="white", relief="flat", bd=6).grid(
            row=0, column=1, sticky="w")

        label("UI Interface Port:", 1)
        self.ui_port_var = tk.StringVar(value="8001")
        tk.Entry(frm, textvariable=self.ui_port_var, width=10,
                 font=("Segoe UI", 10), bg="#313244", fg="#cdd6f4",
                 insertbackground="white", relief="flat", bd=6).grid(
            row=1, column=1, sticky="w")

        label("Public Host:", 2)
        self.public_host_var = tk.StringVar(value="127.0.0.1")
        tk.Entry(frm, textvariable=self.public_host_var, width=20,
                 font=("Segoe UI", 10), bg="#313244", fg="#cdd6f4",
                 insertbackground="white", relief="flat", bd=6).grid(
            row=2, column=1, sticky="w")

        # ── Buttons ────────────────────────────────────────────────
        btn_frm = tk.Frame(root, bg="#1e1e2e")
        btn_frm.pack(pady=16)

        self.launch_btn = tk.Button(
            btn_frm, text="▶  Launch Node", command=self.launch_agent,
            bg="#89b4fa", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
            width=14, relief="flat", bd=0, cursor="hand2"
        )
        self.launch_btn.pack(side="left", padx=8)

        self.stop_btn = tk.Button(
            btn_frm, text="■  Stop Node", command=self.stop_agent,
            state=tk.DISABLED,
            bg="#f38ba8", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
            width=14, relief="flat", bd=0, cursor="hand2"
        )
        self.stop_btn.pack(side="left", padx=8)

        # ── Status ─────────────────────────────────────────────────
        self.status_label = tk.Label(
            root, text="Status: Ready", fg="#6c7086",
            bg="#1e1e2e", font=("Segoe UI", 9, "italic")
        )
        self.status_label.pack()

        # ── Log panel ──────────────────────────────────────────────
        self.log_area = scrolledtext.ScrolledText(
            root, height=10, width=66, state=tk.DISABLED,
            bg="#181825", fg="#a6e3a1", font=("Consolas", 9),
            relief="flat", bd=4, insertbackground="white"
        )
        self.log_area.pack(pady=(10, 0))

    # ── Logging ────────────────────────────────────────────────────
    def log(self, message: str):
        self.log_area.config(state=tk.NORMAL)
        self.log_area.insert(tk.END, message)
        self.log_area.see(tk.END)
        self.log_area.config(state=tk.DISABLED)

    # ── Launch ─────────────────────────────────────────────────────
    def launch_agent(self):
        agent_name  = self.agent_name_var.get().strip()
        api_port    = self.ui_port_var.get().strip()
        public_host = self.public_host_var.get().strip()

        if not agent_name:
            messagebox.showerror("Error", "Agent Name is required.")
            return
        if not api_port.isdigit():
            messagebox.showerror("Error", "API Port must be a number.")
            return

        env = os.environ.copy()
        env["AGENT_NAME"]   = agent_name
        env["API_PORT"]     = api_port
        env["PUBLIC_HOST"]  = public_host or "127.0.0.1"

        self.log_area.config(state=tk.NORMAL)
        self.log_area.delete(1.0, tk.END)
        self.log_area.config(state=tk.DISABLED)

        self.status_label.config(text=f"Status: Starting '{agent_name}'...", fg="#f9e2af")
        self.launch_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)

        threading.Thread(
            target=self._run_process, args=(env, api_port, agent_name), daemon=True
        ).start()

    def _run_process(self, env, api_port, agent_name):
        cmd = [sys.executable, "agent_api.py"]
        self.process = subprocess.Popen(
            cmd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.root.after(0, lambda: self.status_label.config(
            text=f"Status: '{agent_name}' running on :{api_port}", fg="#a6e3a1"
        ))
        self.root.after(0, lambda: self.log(
            f"─── Node '{agent_name}' started on port {api_port} ───\n"
        ))
        threading.Thread(target=self._open_browser, args=(api_port,), daemon=True).start()

        for line in iter(self.process.stdout.readline, ""):
            if line:
                self.root.after(0, lambda l=line: self.log(l))

        self.process.stdout.close()
        rc = self.process.wait()
        self.root.after(0, lambda: self.log(f"\n[Process exited with code {rc}]\n"))
        if rc != 0 and self.process is not None:
            self.root.after(0, self._reset_error)

    def _reset_error(self):
        self.launch_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Status: Crashed — see log", fg="#f38ba8")

    def _open_browser(self, port):
        time.sleep(2.5)
        if self.process and self.process.poll() is None:
            webbrowser.open(f"http://127.0.0.1:{port}/")

    # ── Stop ───────────────────────────────────────────────────────
    def stop_agent(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None
        self.launch_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Status: Stopped", fg="#6c7086")
        self.log("\n─── Node stopped ───\n")

    def on_window_close(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app  = AgentLauncherApp(root)
    root.mainloop()
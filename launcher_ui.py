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
        self.root.geometry("600x550") 
        self.root.configure(padx=20, pady=20)
        self.root.resizable(False, False)
        self.root.protocol("WM_DELETE_WINDOW", self.on_window_close)
        self.process = None

        tk.Label(root, text="Sliq Node Configuration", font=("Helvetica", 12, "bold")).pack(pady=(0, 15))

        # Inputs Frame
        input_frame = tk.Frame(root)
        input_frame.pack(fill="x", pady=(0, 10))

        tk.Label(input_frame, text="Agent Name/Role:").grid(row=0, column=0, sticky="w", pady=5)
        self.agent_name_var = tk.StringVar(value="Merchant")
        tk.Entry(input_frame, textvariable=self.agent_name_var, width=30).grid(row=0, column=1, sticky="w", padx=10)

        tk.Label(input_frame, text="API Port:").grid(row=1, column=0, sticky="w", pady=5)
        self.api_port_var = tk.StringVar(value="8001")
        tk.Entry(input_frame, textvariable=self.api_port_var, width=15).grid(row=1, column=1, sticky="w", padx=10)

        tk.Label(input_frame, text="Receiver Port:").grid(row=2, column=0, sticky="w", pady=5)
        self.recv_port_var = tk.StringVar(value="9001")
        tk.Entry(input_frame, textvariable=self.recv_port_var, width=15).grid(row=2, column=1, sticky="w", padx=10)

        # The Universal Topology Injector
        tk.Label(input_frame, text="Network Topology (BASE_URLS):").grid(row=3, column=0, sticky="nw", pady=5)
        self.base_urls_text = tk.Text(input_frame, width=40, height=4, font=("Courier", 9))
        self.base_urls_text.grid(row=3, column=1, sticky="w", padx=10, pady=5)
        self.base_urls_text.insert("1.0", "Merchant=http://127.0.0.1:9001,\nLabeler=http://127.0.0.1:9002,\nWrapper=http://127.0.0.1:9003,\nPacker=http://127.0.0.1:9004")

        # Buttons Frame
        btn_frame = tk.Frame(root)
        btn_frame.pack(pady=15)

        self.launch_btn = tk.Button(btn_frame, text="Launch Agent", command=self.launch_agent, bg="#4CAF50", fg="white", font=("Helvetica", 10, "bold"), width=15)
        self.launch_btn.pack(side="left", padx=10)

        self.stop_btn = tk.Button(btn_frame, text="Stop Agent", command=self.stop_agent, state=tk.DISABLED, bg="#f44336", fg="white", font=("Helvetica", 10, "bold"), width=15)
        self.stop_btn.pack(side="left", padx=10)

        # Status and Logs
        self.status_label = tk.Label(root, text="Status: Ready", fg="gray", font=("Helvetica", 10, "italic"))
        self.status_label.pack()

        self.log_area = scrolledtext.ScrolledText(root, height=12, width=70, state=tk.DISABLED, bg="#1e1e1e", fg="#00ff00", font=("Consolas", 9))
        self.log_area.pack(pady=10)

    def log_message(self, message):
        self.log_area.config(state=tk.NORMAL)
        self.log_area.insert(tk.END, message)
        self.log_area.see(tk.END)
        self.log_area.config(state=tk.DISABLED)

    def launch_agent(self):
        agent_name = self.agent_name_var.get().strip()
        api_port = self.api_port_var.get().strip()
        recv_port = self.recv_port_var.get().strip()
        base_urls = self.base_urls_text.get("1.0", tk.END).strip().replace("\n", "")

        if not agent_name or not api_port or not recv_port:
            messagebox.showerror("Error", "Please fill in all standard fields.")
            return

        env = os.environ.copy()
        env["AGENT_NAME"] = agent_name
        env["API_PORT"] = api_port
        env["RECEIVER_PORT"] = recv_port
        env["BASE_URLS"] = base_urls

        self.log_area.config(state=tk.NORMAL)
        self.log_area.delete(1.0, tk.END)
        self.log_area.config(state=tk.DISABLED)

        self.status_label.config(text=f"Status: Starting {agent_name}...", fg="orange")
        self.launch_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)

        threading.Thread(target=self._run_process, args=(env, api_port), daemon=True).start()

    def _run_process(self, env, api_port):
        python_exec = sys.executable 
        cmd = [python_exec, "agent_api.py"]

        self.process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )

        self.root.after(0, lambda: self.status_label.config(text=f"Status: Running on Port {api_port}", fg="green"))
        self.root.after(0, lambda: self.log_message(f"--- Starting {env['AGENT_NAME']} API Server on port {api_port} ---\n"))
        
        threading.Thread(target=self.open_browser, args=(api_port,), daemon=True).start()

        for line in iter(self.process.stdout.readline, ''):
            if line:
                self.root.after(0, lambda l=line: self.log_message(l))
        
        self.process.stdout.close()
        return_code = self.process.wait()
        
        self.root.after(0, lambda: self.log_message(f"\n[Server Process Exited with Code {return_code}]\n"))
        
        if return_code != 0 and self.process is not None:
            self.root.after(0, self._reset_ui_error)

    def _reset_ui_error(self):
        self.launch_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Status: Crashed (See Logs)", fg="red")

    def open_browser(self, port):
        time.sleep(2.5) 
        if self.process and self.process.poll() is None:
            webbrowser.open(f"http://127.0.0.1:{port}/dashboard.html")

    def stop_agent(self):
        if self.process:
            self.process.terminate()
            self.process.wait() 
            self.process = None
        
        self.launch_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Status: Stopped", fg="gray")
        self.log_message("\n--- Agent Stopped ---\n")

    def on_window_close(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = AgentLauncherApp(root)
    root.mainloop()
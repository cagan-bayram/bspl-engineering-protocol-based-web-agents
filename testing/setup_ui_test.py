import os, subprocess, time, sys, requests

def start_node(name, port):
    env = os.environ.copy()
    env["AGENT_NAME"] = name
    env["API_PORT"] = str(port)
    env["PROTOCOL_DIR"] = "protocols/reference"
    return subprocess.Popen([sys.executable, "agent_api.py"], env=env)

p1 = start_node("Alice", 8091)
p2 = start_node("Bob", 8092)

time.sleep(3)
a = requests.get("http://127.0.0.1:8091/api/identity").json()["agent_id"]
b = requests.get("http://127.0.0.1:8092/api/identity").json()["agent_id"]

requests.post("http://127.0.0.1:8091/api/join", json={"mas_id":"ui-mas","protocol_name":"Purchase","role":"Buyer","topology":{"Seller":f"http://127.0.0.1:8092/{b}"}})
requests.post("http://127.0.0.1:8092/api/join", json={"mas_id":"ui-mas","protocol_name":"Purchase","role":"Seller","topology":{"Buyer":f"http://127.0.0.1:8091/{a}"}})

requests.post(f"http://127.0.0.1:8091/{a}/enactments?mas_id=ui-mas", json={"enactment_id":"order-test","bindings":{}})
requests.post(f"http://127.0.0.1:8091/{a}/enactments/order-test/send/rfq?mas_id=ui-mas", json={"payload":{"item":"Test Laptop", "ID":"42"}})

print("SETUP DONE")

try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    p1.terminate(); p2.terminate()

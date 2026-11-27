"""Poll PALINOR until something is addressed to me, then print it and exit (wakes Claude-2)."""
import json, subprocess, sys, time, os
D = os.path.dirname(os.path.abspath(__file__)); seq = int(sys.argv[1]) if len(sys.argv) > 1 else 0
env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
while True:
    out = subprocess.run([sys.executable, os.path.join(D, "pal.py"), "message_inbox", '{"unread_only":true}'],
                         capture_output=True, text=True, encoding="utf-8", env=env).stdout
    cmd = subprocess.run([sys.executable, os.path.join(D, "pal.py"), "command_inbox", "{}"],
                         capture_output=True, text=True, encoding="utf-8", env=env).stdout
    try:
        m = json.loads(out); c = json.loads(cmd)
    except Exception:
        time.sleep(15); continue
    msgs = m.get("messages", m) if isinstance(m, dict) else m
    cmds = c.get("commands", c) if isinstance(c, dict) else c
    pending = [x for x in (cmds or []) if isinstance(x, dict) and x.get("status") in ("PENDING", "DELIVERED", "SENT")]
    if msgs or pending:
        print("WAKE"); print(out[:3000]); print(cmd[:3000]); break
    time.sleep(15)

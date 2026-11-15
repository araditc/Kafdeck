"""Targeted regression: advertised unsupported attributes vs genuine getter failures."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
import argparse,json, secrets, subprocess, threading

parser=argparse.ArgumentParser()
parser.add_argument('--java',default='java')
parser.add_argument('--classes',type=Path,help='Already compiled Java 11 fixture classes')
args=parser.parse_args()

base=Path(__file__).resolve().parent
captures=Queue()
token=secrets.token_hex(32)
class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.headers['X-Kafdeck-Agent-Token']==token
        captures.put(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
        self.send_response(204); self.end_headers()
    def log_message(self,*args): pass

with ThreadingHTTPServer(('127.0.0.1',0),Receiver) as server, TemporaryDirectory() as tmp:
    threading.Thread(target=server.serve_forever,daemon=True).start()
    p=Path(tmp)
    (p/'token').write_text(token)
    config=p/'agent.properties'
    config.write_text(f'endpoint=http://127.0.0.1:{server.server_port}/monitoring/ingest\ntokenFile={(p/"token").as_posix()}\nintervalSeconds=10\nincludeTopicPartitions=false\n')
    if args.classes is None: subprocess.run(['javac','--release','11','-d',tmp,str(base/'AgentSmoke.java')],check=True)
    for failure in (False,True):
        process=subprocess.Popen([args.java,f'-Dprobe.failure={str(failure).lower()}',f'-javaagent:{base.parent}/kafdeck-kafka-agent.jar={config}','-cp',str(args.classes) if args.classes else tmp,'AgentSmoke'])
        try:
            batch=captures.get(timeout=20)
            diag=batch['diagnostics']
            assert batch['last'] and batch['partial']==failure,batch
            assert diag['unsupported']>=2 and not diag['limited'],diag
            expected='readFailed' if failure else 'unsupported'
            assert any(i['reason']==expected for i in diag['issues']),diag
            assert diag['failed']==int(failure),diag
            assert diag['collected']==len(batch['samples'])
            assert any(r['metric']=='kafka.test:Probe/Compatibility/Value' and r['value']==73 for r in batch['samples'])
            print(json.dumps({'failureFixture':failure,'partial':batch['partial'],'collected':diag['collected'],'unsupported':diag['unsupported'],'failed':diag['failed']}),flush=True)
        finally: process.terminate();process.wait(timeout=10)
    server.shutdown()

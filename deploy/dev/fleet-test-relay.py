"""Temporary acceptance transport: owned Docker pilot -> SSH -> local TLS fixture.

No TLS termination, credential logging, broker changes or persistent service.
The remote relay accepts only the explicitly selected pilot network.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import select
import shlex
import socket
import threading
import time

import paramiko


REMOTE = r'''
import ipaddress,json,select,socket,sys,threading,time
settings=SETTINGS
allowed=ipaddress.ip_network(settings['subnet'])
stopped=threading.Event()
def done():
    sys.stdin.buffer.read()
    stopped.set()
threading.Thread(target=done,daemon=True).start()
def forward(incoming):
    try:
        with incoming, socket.create_connection(('127.0.0.1',settings['reversePort']),5) as outgoing:
            while not stopped.is_set():
                for source in select.select([incoming,outgoing],[],[],1)[0]:
                    value=source.recv(65536)
                    if not value:return
                    (outgoing if source is incoming else incoming).sendall(value)
    except OSError:pass
with socket.socket() as server:
    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server.bind((settings['address'],settings['port']))
    server.listen(8)
    server.settimeout(1)
    print('READY',flush=True)
    deadline=time.monotonic()+1800
    while not stopped.is_set() and time.monotonic()<deadline:
        try: connection,peer=server.accept()
        except socket.timeout:continue
        if ipaddress.ip_address(peer[0]) not in allowed:
            connection.close()
            continue
        threading.Thread(target=forward,args=(connection,),daemon=True).start()
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--known-hosts', required=True)
    parser.add_argument('--network', required=True)
    parser.add_argument('--stop-file', required=True)
    arguments = parser.parse_args()
    if not arguments.network.startswith('kafdeck-fleet-ui-pilot-') or not arguments.network.endswith('_default'):
        raise ValueError('Use only an owned Fleet acceptance network.')
    client = paramiko.SSHClient()
    client.load_host_keys(arguments.known_hosts)
    client.connect(arguments.host, username=arguments.user, password=os.environ['KAFDECK_RELAY_PASSWORD'],
                   timeout=10, allow_agent=False, look_for_keys=False)
    stopped = threading.Event()
    try:
        _, output, _ = client.exec_command('/usr/bin/docker network inspect ' + shlex.quote(arguments.network), timeout=10)
        network = json.loads(output.read())
        if output.channel.recv_exit_status() != 0 or len(network) != 1:
            raise ValueError('Owned pilot network could not be verified.')
        transport = client.get_transport()
        transport.request_port_forward('127.0.0.1', 28590)
        settings = dict(subnet=network[0]['IPAM']['Config'][0]['Subnet'], address=arguments.host,
                        port=28090, reversePort=28590)
        script = REMOTE.replace('SETTINGS', repr(settings))
        remote_input, remote_output, _ = client.exec_command('/usr/bin/python3 -u -c ' + shlex.quote(script))
        if remote_output.readline().strip() != 'READY':
            raise RuntimeError('The temporary relay could not start.')

        def forward(channel):
            try:
                with channel, socket.create_connection(('127.0.0.1', 18090), 5) as target:
                    while not stopped.is_set():
                        for source in select.select([channel, target], [], [], 1)[0]:
                            value = source.recv(65536)
                            if not value:
                                return
                            (target if source is channel else channel).sendall(value)
            except OSError:
                pass

        print('READY: pilot-only raw TLS forwarding; no persistent host service.', flush=True)
        deadline = time.monotonic() + 1800
        while transport.is_active() and not Path(arguments.stop_file).exists() and time.monotonic() < deadline:
            channel = transport.accept(1)
            if channel is not None:
                threading.Thread(target=forward, args=(channel,), daemon=True).start()
        stopped.set()
        remote_input.close()
        transport.cancel_port_forward('127.0.0.1', 28590)
    finally:
        stopped.set()
        client.close()


if __name__ == '__main__':
    main()

"""Offline recalculation in a disposable source copy; no collection/publication."""
import json
import socket
import sys
from pathlib import Path

def no_network(*args, **kwargs):
    raise RuntimeError('network forbidden during derived-input repair assessment')

socket.socket.connect = no_network
socket.create_connection = no_network
from qwl import config, paths
from qwl.datasources.controlled_benchmarks import write_controlled

if __name__ == '__main__':
    try:
        result = {'passed': True, **write_controlled(config.load_extended_universe(), sys.argv[1], Path(sys.argv[2]), paths.ROOT)}
    except Exception as exc:
        result = {'passed': False, 'issues': [{'domain':'benchmarks', 'blocking': True,
                  **getattr(exc, 'evidence', {'reason':'derived_worker_failed', 'details':str(exc)})}]}
    print(json.dumps(result, ensure_ascii=False, default=str))

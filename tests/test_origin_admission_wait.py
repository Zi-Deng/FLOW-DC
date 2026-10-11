"""A pre-header admission wait must fit the declared origin idle timeout."""
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
import unittest
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'bin')]
from benchmark.core.controlled_origin import ControlledOrigin, scenario
from benchmark.known_truth import image_payloads
from flowdc_experiment_research import origin_plan

class OriginAdmissionWaitTests(unittest.TestCase):
    def test_header_wait_obeys_explicit_timeout_and_preserves_service_work(self):
        for timeout, expected in ((.05, False), (.5, True)):
            with tempfile.TemporaryDirectory() as folder:
                plan = scenario('steady', image_payloads(), rows=1)
                plan['connection_idle_timeout_s'] = timeout
                with ControlledOrigin(Path(folder), plan) as origin:
                    address = urlsplit(origin.base_url)
                    with socket.create_connection((address.hostname,address.port),timeout=2) as connection:
                        time.sleep(.15)
                        path = plan['assignments'][0]['path']
                        connection.sendall(f'GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n'.encode())
                        response = connection.recv(4096)
                        self.assertEqual(b'200 OK' in response, expected)
                        if expected:
                            while connection.recv(4096):pass
                self.assertEqual(origin.snapshot()['requests'], int(expected))

    def test_invalid_idle_timeout_fails_before_origin_output(self):
        for timeout in (0,36,True,float('nan'),float('inf'),'35'):
            with tempfile.TemporaryDirectory() as folder:
                plan = scenario('steady',image_payloads(),rows=1)
                plan['connection_idle_timeout_s'] = timeout
                with self.assertRaises(ValueError):ControlledOrigin(Path(folder),plan)
                self.assertEqual(list(Path(folder).iterdir()), [])

    def test_guest_plan_accepts_bounded_explicit_timeout(self):
        import base64
        payload = image_payloads()['PNG']
        plan={'schema':'flowdc-guest-origin-v3','research_workload':'bounded-research-v2',
            'name':'steady','schedule':[[0,4]],'queue_bound':16,'assignments':['/image'],
            'objects':{'/image':{'payload_base64':base64.b64encode(payload).decode(),
                'service_s':.04,'responses':[{'status':200}]}},'connection_idle_timeout_s':35}
        self.assertEqual(origin_plan(json.dumps(plan))['connection_idle_timeout_s'],35)
        for timeout in (36,0,True):
            with self.assertRaises(ValueError):origin_plan(json.dumps({**plan,'connection_idle_timeout_s':timeout}))

if __name__=='__main__':unittest.main()

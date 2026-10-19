"""External-study qualification uses loopback only, never the public host."""
import asyncio
import datetime as dt
import io
import json
import socket
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import patch

import aiohttp

from benchmark.real_server_study import analyze,cloud,common,design,prepare,runner,storage,telemetry

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bin"))
from flowdc_research_profile import EXTERNAL,RESEARCH,LEGACY,from_record,workload


def events(start=100, cap=40, status=200):
    out=[]
    for t in range(10,30):
        out.append(dict(event="sample",monotonic_s=start+t,backlog=500,inflight=cap,cap=cap))
        for n in range(20):
            token=f"{t}-{n}"
            out.extend([dict(event="dispatch",monotonic_s=start+t+.01,token=token,cap=cap),
                        dict(event="headers",monotonic_s=start+t+.02,token=token,status=status),
                        dict(event="network_end",monotonic_s=start+t+.03,token=token,ttfb_s=.02,failure_kind=None)])
    return out


class DesignTests(unittest.TestCase):
    def test_profiles_preserve_previous_meanings(self):
        self.assertEqual(RESEARCH.max_payload_bytes,512*2**20)
        self.assertEqual(LEGACY.max_rows,256)
        self.assertEqual(workload(EXTERNAL.name),EXTERNAL)
        self.assertEqual(from_record(EXTERNAL.record()),EXTERNAL)
        changed=EXTERNAL.record();changed["max_rows"]+=1
        with self.assertRaises(ValueError):from_record(changed)

    def test_schedule_no_fixed_until_all_adaptive(self):
        runs=design.schedule()
        self.assertEqual(len(runs),28)
        self.assertTrue(all(r["phase"]!="fixed" for r in runs[:16]))
        for phase in ("calibration","adaptive","fixed"):
            first=[r["arm"] for r in runs if r["phase"]==phase and r["position"]==1]
            self.assertEqual(first.count(first[0]),len(first)//2)
        self.assertEqual(runs,design.schedule())

    def test_no_hidden_four_or_sixteen_setting(self):
        r=design.schedule()[0]
        cfg=design.config(r,Path('/study'),Path('/study/run'))
        self.assertEqual((cfg['C_min'],cfg['C_init'],cfg['C_max'],cfg['concurrent_downloads']),(2,4,256,256))
        self.assertEqual(cfg['max_retry_attempts'],1)
        for r in design.schedule()[16:]:
            cfg=design.config(r,Path('/study'),Path('/study/run'),{'fixed-low':30,'fixed-high':90})
            self.assertEqual(cfg['C_min'],cfg['C_max'])
            self.assertEqual(cfg['C_max'],30 if r['arm']=='fixed-low' else 90)

    def test_calibration_selects_equal_bounded_work(self):
        rows=[{'complete':True,'verification':{'credited_rows':8192},'acquisition_s':80}]*4
        p=design.slice_size(rows,150000,600000)
        self.assertEqual(p['rows_per_main_run']%256,0)
        self.assertGreaterEqual(p['predicted_fastest_acquisition_s'],60)
        with self.assertRaises(ValueError):design.slice_size(rows,150000,500)
        rows[0]={'complete':False}
        with self.assertRaises(ValueError):design.slice_size(rows,150000,600000)

    def test_fixed_selection_with_and_without_429(self):
        records=[{'phase':'calibration' if i<4 else 'adaptive','started_monotonic':100} for i in range(16)]
        selected=design.select_fixed(records,[events() for _ in records],.02)
        self.assertEqual((selected['fixed-low'],selected['fixed-high']),(32,60))
        self.assertIsNone(selected['S'])
        observed=[events() for _ in records]
        observed[0].extend([dict(event='dispatch',monotonic_s=131,token='limit',cap=35),
                            dict(event='headers',monotonic_s=132,token='limit',status=429)])
        selected=design.select_fixed(records,observed,.02)
        self.assertEqual((selected['fixed-low'],selected['fixed-high']),(28,35))
        with self.assertRaises(ValueError):design.select_fixed(records,[[e for e in events() if e['event']!='sample']]*16,.02)
        with self.assertRaises(ValueError):design.select_fixed(records[:-1],observed[:-1],.02)

    def test_503_is_not_explicit_rate_limit(self):
        records=[dict(phase='calibration' if i<4 else 'adaptive',started_monotonic=100) for i in range(16)]
        audits=[events() for _ in records]
        audits[0].append(dict(event='headers',monotonic_s=132,token='x',status=503))
        self.assertIsNone(design.select_fixed(records,audits,.02)['S'])

    def test_refusal_access_and_retry_after(self):
        p=telemetry.StopPolicy(.02)
        for i in range(5):p.header(i,429,'1200')
        self.assertEqual(p.reason,'refusal_threshold')
        self.assertGreater(p.cooldown_until,time.time()+1190)
        p=telemetry.StopPolicy(.02);p.header(1,403)
        self.assertTrue(p.fatal)
        self.assertEqual(p.reason,'access_refusal')
        p=telemetry.StopPolicy(.02)
        for i in range(200):p.header(i/100,429 if i<2 else 200)
        self.assertEqual(p.reason,'refusal_threshold')

    def test_persistent_delay_and_no_local_timeout_as_refusal(self):
        p=telemetry.StopPolicy(.02)
        p.tick(0)
        for bucket in range(3):
            for i in range(100):p.complete(bucket*10+i/100,.07)
            p.tick((bucket+1)*10)
        self.assertEqual(p.reason,'sustained_latency')
        p=telemetry.StopPolicy(.02)
        for i in range(500):p.header(i/100,408)
        self.assertIsNone(p.reason)

    def test_url_tls_and_robots(self):
        for url in ['http://moth.tbn.org.tw/images/a','https://elsewhere.test/images/a',common.PREFIX+'a?b=1']:
            with self.assertRaises(ValueError):common.checked_url(url)
        self.assertTrue(common.tls_context(True).check_hostname)
        self.assertEqual(common.tls_context().verify_flags & 0x200000,0)
        prepare.robots_permit('User-agent: *\nDisallow: /other/',[common.PREFIX+'a'])
        with self.assertRaises(ValueError):prepare.robots_permit('User-agent: *\nDisallow: /images/',[common.PREFIX+'a'])

    def test_known_statistics_empty_ties_and_randomization(self):
        self.assertIsNone(analyze.summary([])['mean'])
        self.assertEqual(analyze.summary([0]*6)['p'],1)
        self.assertAlmostEqual(analyze.summary([1,2,3])['mean'],2)
        self.assertEqual(analyze.randomization([1]*6),2/64)
        self.assertEqual(analyze.randomization([0]*6),1)

    def test_source_change_refused_and_duplicate_json(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'p';p.write_text('{"a":1,"a":2}')
            with self.assertRaises(ValueError):common.read_json(p)
            common.write_new(Path(d)/'one',{})
            with self.assertRaises(FileExistsError):common.write_new(Path(d)/'one',{})
            common.write_new(Path(d)/'protocol.json',{'schema':'flowdc-external-concurrency-v1','source':{'head':'old'},'files':{}})
            with patch.object(common,'source_identity',return_value={'head':'new'}),self.assertRaises(ValueError):
                common.protocol_check(d,'/unused')

    def test_true_exposure_quantile_not_sample_count(self):
        self.assertEqual(design.weighted_quantile([(4,99),(256,1)],.95),4)
        self.assertEqual(design.weighted_quantile([(4,1),(256,99)],.95),256)

    def test_allocation_gate_and_embedded_guest_programs(self):
        valid={'allocation':{'cpu_su':2000,'observed_at':dt.datetime.now(dt.UTC).isoformat(),'expiry_date':'2027-04-17'}}
        cloud.allocation_gate(valid)
        valid['allocation']['cpu_su']=None
        with self.assertRaises(ValueError):cloud.allocation_gate(valid)
        compile(storage.MOUNT,'mount-fixture','exec');compile(storage.STAGE,'stage-fixture','exec')

    def test_first_attempt_and_order_refusals(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);runs=design.schedule();common.write_new(root/'schedule.json',{'runs':runs})
            (root/'runs'/runs[0]['run_id']).mkdir(parents=True)
            with self.assertRaises(ValueError):runner.records(root)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);common.write_new(root/'schedule.json',{'runs':design.schedule()})
            (root/'runs'/design.schedule()[3]['run_id']).mkdir(parents=True)
            with self.assertRaises(ValueError):runner.records(root)

    def test_guest_requires_observed_cloud_uuid(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            common.write_new(root/'execution.json',dict(executor='jetstream2',vm_id='expected',campaign_id='campaign',
                hostname=socket.gethostname(),deadline_utc=time.time()+60))
            client=__import__('unittest.mock',fromlist=['Mock']).Mock()
            client.open.return_value=io.BytesIO(b'{"uuid":"elsewhere"}')
            with patch.object(prepare.urllib.request,'build_opener',return_value=client):
                with self.assertRaises(ValueError):prepare.guest_check(root)
            client.open.return_value=io.BytesIO(b'{"uuid":"expected"}')
            with patch.object(prepare.urllib.request,'build_opener',return_value=client):
                prepare.guest_check(root)
            self.assertEqual(client.open.call_args.args[0],'http://169.254.169.254/openstack/latest/meta_data.json')


class NetworkTests(unittest.TestCase):
    def setUp(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def do_GET(self):
                self.send_response(200);self.send_header('Content-Length','32');self.end_headers()
                self.wfile.write(b'x');self.wfile.flush();time.sleep(.25)
                self.wfile.write(b'x'*31)
            def log_message(self,*args):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=True
        thread=threading.Thread(target=self.server.serve_forever,daemon=True);thread.start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)

    def test_actual_native_three_methods_and_independent_archive_verifier(self):
        import download_batch
        import polars as pl
        from benchmark.real_server_study.verify import verify
        async def run():
            for method in ('fixed-v1','ratio-v1','gradient-candidate-v1'):
                with tempfile.TemporaryDirectory() as d:
                    directory=Path(d);native=directory/'native'
                    urls=[f'http://127.0.0.1:{self.server.server_port}/{i}' for i in range(48)]
                    pl.DataFrame({'url':urls}).write_parquet(directory/'input.parquet')
                    r=dict(phase='adaptive',method=method,slice='fixture')
                    cfg=design.config(r,directory,directory)
                    cfg.update(input=str(directory/'input.parquet'),C_init=32)
                    if method=='fixed-v1':cfg.update(C_min=32,C_max=32)
                    (directory/'flowdc-config.json').write_text(json.dumps(cfg))
                    with patch.object(sys,'argv',['download_batch.py','--config',str(directory/'flowdc-config.json')]):
                        config=download_batch.parse_args()
                    with (directory/'request-audit.jsonl').open('x') as audit:
                        g=telemetry.Guard(urls,audit,1)
                        g.emit('start',run_id='fixture',rows=48)
                        with patch.object(telemetry,'checked_url',side_effect=lambda u:u):
                            restore=g.install();started=time.monotonic()
                            try:
                                download_batch.shutdown_flag=False
                                await download_batch.run_acquisition(config)
                            finally:
                                if g.sampler:g.sampler.cancel();await asyncio.gather(g.sampler,return_exceptions=True)
                                restore()
                            ended=time.monotonic()
                        g.emit('closed',active=len(g.active),stop_reason=None)
                    result=verify(directory,directory/'input.parquet',Path(__file__).resolve().parents[1],started,ended)
                    self.assertTrue(result['native_run_complete'])
                    # The fixture bodies intentionally are not images: native integrity succeeds,
                    # independent image credit rejects every retained body.
                    self.assertEqual(result['decode_failures'],48)
                    self.assertEqual(result['credited_rows'],0)
                    self.assertTrue((native/'outcome-index.json').exists())
                    runner.compact_outputs(directory)
                    self.assertTrue((native/'.flowdc/owner.json').exists())
                    self.assertEqual(verify(directory,directory/'input.parquet',Path(__file__).resolve().parents[1],started,ended)['decode_failures'],48)
                    rows=prepare.read_lines(directory/'request-audit.jsonl')
                    self.assertGreater(max(e['inflight'] for e in rows if e['event']=='dispatch'),16)
                    (directory/'native.tar').write_bytes(b'corrupt')
                    with self.assertRaises(ValueError):verify(directory,directory/'input.parquet',Path(__file__).resolve().parents[1],started,ended)
        asyncio.run(run())

    def test_body_complete_concurrency_exceeds_sixteen_and_replay_refused(self):
        import single_download
        async def run():
            urls=[f'http://127.0.0.1:{self.server.server_port}/{i}' for i in range(40)]
            stream=io.StringIO();g=telemetry.Guard(urls,stream,1)
            with patch.object(telemetry,'checked_url',side_effect=lambda u:u if u in urls else common.require(False,'unplanned')):
                restore=g.install()
                try:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
                        async def fetch(url):
                            measurement={};token=single_download.HTTP_TRACE_CTX.set(measurement)
                            try:return await single_download.download_via_http_get(session,url,10)
                            finally:single_download.HTTP_TRACE_CTX.reset(token)
                        result=await asyncio.gather(*(fetch(u) for u in urls))
                        self.assertTrue(all(r[1]==200 and len(r[0])==32 for r in result))
                        with self.assertRaises(ValueError):await fetch(urls[0])
                finally:
                    if g.sampler:g.sampler.cancel();await asyncio.gather(g.sampler,return_exceptions=True)
                    restore()
            records=[json.loads(line) for line in stream.getvalue().splitlines()]
            self.assertGreater(max(e['inflight'] for e in records if e['event']=='dispatch'),16)
            self.assertEqual(len([e for e in records if e['event']=='network_end']),40)
            self.assertFalse(g.active)
            begins={e['token']:e['monotonic_s'] for e in records if e['event']=='dispatch'}
            self.assertTrue(all(e['monotonic_s']-begins[e['token']]>=.20 for e in records if e['event']=='network_end'))
        asyncio.run(run())


if __name__=='__main__':unittest.main()

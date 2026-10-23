"""Offline regressions for long public studies; never scientific image requests."""

import ast
import io
import json
import multiprocessing
import os
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

import polars as pl

from benchmark.real_server_study import minimum_study as design
from benchmark.real_server_study.common import write_new
from benchmark.real_server_study.minimum_native import pack, unpack
from benchmark.real_server_study.public_guard import SharedBodyBudget
from benchmark.real_server_study.public_launch import merge_audits
from benchmark.real_server_study.public_metrics import transport


def reserve(path):
    return SharedBodyBudget(path, 10).update(reserve=6)


def native_shard(arguments):
    import email.message
    from PIL import Image

    wrapped, row = arguments
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), "green").save(buf, format="JPEG")
    raw = buf.getvalue()

    class Response(io.BytesIO):
        code = 200
        headers = email.message.Message()

    with patch("urllib.request.OpenerDirector.open", side_effect=lambda *args, **kwargs: Response(raw)):
        return wrapped(row)


class MinimumStudyTests(unittest.TestCase):
    def test_embedded_cloud_programs_compile(self):
        path = Path(__file__).resolve().parents[1] / "benchmark/real_server_study/minimum_cloud.py"
        programs = [
            node.args[2].value
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "json_command"
            and len(node.args) > 2
            and isinstance(node.args[2], ast.Constant)
            and isinstance(node.args[2].value, str)
        ]
        self.assertTrue(programs)
        for index, program in enumerate(programs):
            with self.subTest(program=index):
                compile(program, f"{path}:embedded-{index}", "exec")


    def test_exhausted_cloud_envelope_records_blocker_without_activation(self):
        from types import SimpleNamespace
        from unittest.mock import Mock

        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bin"))
        import flowdc_ops
        from benchmark.real_server_study import minimum_cloud

        for exhausted_windows in (False, True):
            with (
                self.subTest(exhausted_windows=exhausted_windows),
                tempfile.TemporaryDirectory() as directory,
            ):
                packet = Path(directory)
                write_new(packet / "operator.json", {})
                write_new(packet / "deployment.json", dict(limits=design.LIMITS))
                if exhausted_windows:
                    for number in range(design.LIMITS["max_windows"]):
                        write_new(packet / f"window-{number:02}.json", dict(cleaned=True))
                journal = SimpleNamespace(read=lambda: dict(profile_path="test", vms={}))
                controller = Mock()
                with (
                    patch.object(
                        minimum_cloud.public_cloud,
                        "authorize",
                        return_value=(journal, dict(clock=dict(utc=time.time() - design.LIMITS["wall_seconds"]))),
                    ),
                    patch.object(flowdc_ops, "load_profile", return_value={}),
                    patch.object(minimum_cloud, "exposure", return_value=0),
                    patch.object(minimum_cloud.cloud, "controller_for", return_value=controller),
                ):
                    result = minimum_cloud.run(packet)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse(result["collected"])
                self.assertTrue((packet / "complete.json").exists())
                controller.call.assert_not_called()






    def make_protocol(self, study, unqualified=()):
        (study / "gates").mkdir()
        (study / "runs").mkdir()
        write_new(study / "protocol.json", dict(
            initial_rows={s: 1000 for s in design.SOURCES},
            partitions={s: dict(capacity=100000) for s in design.SOURCES},
            provenance={s: dict(media_rights_qualified=s not in unqualified,
                                access_qualified=True) for s in design.SOURCES}))

    def test_queue_freezes_global_selection_before_targets_and_never_replays(self):
        from benchmark.real_server_study.minimum_queue import next_cell
        with tempfile.TemporaryDirectory() as directory:
            study = Path(directory)
            self.make_protocol(study, [design.SOURCES[2]])
            count = 0
            while True:
                cell = next_cell(study, design.LIMITS["wall_seconds"])
                if cell["phase"] == "evaluation":
                    break
                if cell["source"] != design.SOURCES[0]:
                    self.assertTrue((study / "gates/development-selection.json").exists())
                rows = cell["rows"]
                path = study / "runs" / cell["run_id"]
                path.mkdir()
                write_new(path / "record.json", dict(**cell, status="complete", original_rows=rows,
                    http_span_s=900, elapsed_s=1000, native_seconds=950,
                    body_bytes=rows*1000, coverage=1, goodput_Bps=1000,
                    mean_http_concurrency=cell["candidate"]+10))
                count += 1
                self.assertLess(count, 30)
            self.assertEqual(count, 18)  # 2 sizing + 12 development + 4 target
            frozen = json.loads((study / "freeze.json").read_text())
            self.assertEqual(len(frozen["cells"]), 18)  # identical tool settings deduplicated
            self.assertEqual(frozen["sources"], [design.SOURCES[1]])
            path = study / "runs" / cell["run_id"]
            path.mkdir()
            with self.assertRaisesRegex(ValueError, "unfinished first attempt"):
                next_cell(study, design.LIMITS["wall_seconds"])
            write_new(path / "record.json", dict(**cell, status="stopped"))
            self.assertIsNone(next_cell(study, design.LIMITS["wall_seconds"]))
            self.assertEqual(json.loads((study / "gates/inaturalist-open-disposition.json").read_text())["status"], "suspended")

    def test_current_profiles_transfer_mapping_and_matched_resources(self):
        frame = pl.DataFrame(dict(url=[str(i) for i in range(100)]))
        for source in design.SOURCES:
            for method in design.METHODS:
                for candidate in range(4):
                    cell = dict(source=source, workers=1, method=method, profile=candidate,
                                tool_setting=design.tool_settings(source)[candidate])
                    cfg = design.configuration("/tmp/example", cell, frame)
                    if method == design.METHODS[2]:
                        self.assertLessEqual(cfg["processes_count"]*cfg["thread_count"], design.ceiling(source))
                        self.assertTrue(cfg["disable_all_reencoding"])
                        self.assertEqual((cfg["retries"],cfg["max_shard_retry"]), (0,0))
                    else:
                        self.assertEqual(cfg["C_max"], design.ceiling(source))
                        self.assertEqual(cfg["method_options"]["target_delay_ratio"], design.PROFILES[candidate][0])
                        self.assertEqual(cfg["research_workload"], design.PROFILE)
        with self.assertRaises(ValueError):
            design.configuration("/tmp/example", dict(source=design.SOURCES[0], workers=2, method=design.METHODS[0]), frame)

    def test_selection_excludes_short_incomplete_low_coverage_and_breaks_ties(self):
        rows = [dict(candidate=i, run_id=str(i), method=design.METHODS[0], status="complete",
                     http_span_s=900, coverage=1, goodput_Bps=100-i, mean_http_concurrency=50-i) for i in range(4)]
        self.assertEqual(design.select_configuration(rows, design.METHODS[0])["candidate"], 2)
        rows[2]["http_span_s"] = 599
        self.assertEqual(design.select_configuration(rows, design.METHODS[0])["candidate"], 1)
        rows[1]["coverage"] = .98
        self.assertEqual(design.select_configuration(rows, design.METHODS[0])["candidate"], 0)
        rows[0]["status"] = "stopped"
        self.assertEqual(design.select_configuration(rows, design.METHODS[0])["candidate"], 3)
        with self.assertRaises(ValueError):
            design.select_configuration(rows[:3], design.METHODS[0])

    def test_balanced_schedule_deduplicates_only_identical_tool_candidates(self):
        from collections import Counter
        selected = dict(development={m: dict(candidate=0) for m in design.METHODS})
        selected.update({s: dict(candidate=1) for s in design.TARGETS})
        sizes = {s:1000 for s in design.TARGETS}
        cells = design.schedule(selected, sizes, design.TARGETS)
        self.assertEqual(len(cells), 48)
        self.assertEqual(len({c["run_id"] for c in cells}),48)
        for source in design.TARGETS:
            for arm in ("ratio", "gradient", "transferred", "target_calibrated"):
                positions = Counter(c["position"] for c in cells if c["source"] == source and c["arm"] == arm)
                self.assertLessEqual(max(positions.values())-min(positions.values()),1)
        selected[design.TARGETS[0]]["candidate"] = 0
        cells = design.schedule(selected, sizes, design.TARGETS)
        self.assertEqual(len(cells), 42)
        aliases = [c for c in cells if c["reference_aliases"]]
        self.assertEqual(len(aliases),6)

    def test_manifest_partitions_are_disjoint_and_block_inputs_match(self):
        with tempfile.TemporaryDirectory() as directory:
            study = Path(directory)
            (study / "pools").mkdir()
            source=design.SOURCES[0]
            pl.DataFrame(dict(study_row_id=list(range(300)),url=[str(i) for i in range(300)])).write_parquet(study / "pools" / (source+".parquet"))
            write_new(study / "protocol.json", dict(partitions={source:dict(sizing=0,calibration=100,evaluation=200,capacity=100)}))
            parts=[design.manifest(study,source,phase,100) for phase in ("sizing","calibration","evaluation")]
            self.assertEqual(pl.concat(parts)["study_row_id"].n_unique(),300)
            self.assertTrue(design.manifest(study,source,"evaluation",100,3).equals(design.manifest(study,source,"evaluation",100,3)))
            self.assertFalse(parts[2].equals(design.manifest(study,source,"evaluation",100,3)))

    def test_forecasts_preserve_ordered_clocks_and_finite_capacity(self):
        r=dict(status="complete",phase="sizing",rows=1000,http_span_s=100,native_seconds=400,elapsed_s=600,body_bytes=1000000)
        prediction=design.forecast(r,9000)
        self.assertEqual(prediction,dict(http=900,native=3600,total=5400,body=9000000))
        self.assertEqual(design.sizing(r,10000),9000)
        with self.assertRaises(ValueError): design.sizing(r,8000)
        with self.assertRaises(ValueError): design.forecast({**r,"phase":"evaluation"},9000)
        with self.assertRaises(ValueError): design.sizing({**r,"body_bytes":100*2**30},10000)

    def test_partial_classification_does_not_call_403_rate_limiting(self):
        from benchmark.real_server_study.public_metrics import partial_transport
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); p=root/'request-audit.jsonl'
            p.write_text(json.dumps(dict(event='headers',status=403))+'\n')
            value=partial_transport(root,1000)
            self.assertEqual(value['classification'],'access_refusal_cause_unknown')
            self.assertIsNone(value['mean_http_concurrency'])
            self.assertEqual(value['body_bytes'],1000)
            p.write_text(json.dumps(dict(event='headers',status=429))+'\n')
            self.assertEqual(partial_transport(root,1000)['classification'],'confirmed_rate_limit')

    def test_analysis_retains_missing_changed_and_partial_pairs(self):
        from benchmark.real_server_study.minimum_analysis import summarize
        with tempfile.TemporaryDirectory() as directory:
            study=Path(directory); (study/'gates').mkdir()
            selected=dict(development={m:dict(candidate=0) for m in design.METHODS})
            source=design.TARGETS[0]; selected[source]=dict(candidate=0)
            cells=design.schedule(selected,{source:1},[source])
            write_new(study/'freeze.json',dict(cells=cells))
            for cell in cells:
                if cell['arm']=='ratio' and cell['block']==5: continue
                run=study/'runs'/cell['run_id']; (run/'worker').mkdir(parents=True)
                (run/'input.parquet').write_bytes(b'matched')
                seconds=10 if cell['arm']=='transferred' else 5
                stopped=cell['arm']=='ratio' and cell['block']==4
                write_new(run/'record.json',dict(**cell,status='stopped' if stopped else 'complete',
                    original_rows=1,credited_bytes=100,coverage=1,elapsed_s=seconds,
                    goodput_Bps=None if stopped else 100/seconds))
                digest='changed' if cell['arm']=='ratio' and cell['block']==3 else 'original'
                write_new(run/'worker/verification.json',dict(original_rows=1,credited_rows=1,
                    credited_bytes=100,payloads={'row':dict(bytes=100,sha256=digest)}))
            result=summarize(study)
            self.assertEqual(result['complete_jobs'],16)
            ratio=result['comparisons'][source+':'+design.METHODS[0]+':transferred']
            self.assertEqual(ratio['blocks'],3)
            self.assertFalse(ratio['practical_speed_claim_supported'])
            grad=result['comparisons'][source+':'+design.METHODS[1]+':transferred']
            self.assertEqual(grad['blocks'],6)
            self.assertTrue(grad['practical_speed_claim_supported'])
            self.assertEqual(grad['p_holm'],0)
            self.assertEqual(ratio['changed_content_blocks'],[dict(block=3,rows=1)])

    def test_preselection_stop_retains_declared_scope(self):
        from benchmark.real_server_study.minimum_analysis import summarize
        with tempfile.TemporaryDirectory() as directory:
            study=Path(directory)
            self.make_protocol(study)
            run=study/'runs/sizing-openimages-cvdf-0'
            run.mkdir()
            write_new(run/'record.json',dict(source=design.SOURCES[0], phase='sizing',
                method=design.METHODS[2], status='stopped', run_id=run.name, rows=210000))
            result=summarize(study)
            self.assertEqual(result['scientific_status'],'not_frozen')
            self.assertEqual(result['attempted_jobs'],1)
            self.assertEqual(result['planned_jobs'],0)
            self.assertEqual(sum(s['maximum_jobs'] for s in result['declared_scope'] if s['phase']!='sizing'),68)
            self.assertEqual(sum(s['stopped_or_failed'] for s in result['declared_scope']),1)
            self.assertFalse(any(c.get('practical_speed_claim_supported') for c in result['comparisons'].values()))

    def test_partial_tool_sealed_rows_and_truncated_shards(self):
        from PIL import Image
        from benchmark.real_server_study.public_verify import verify_partial
        from benchmark.real_server_study.common import sha256
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); (root/'native').mkdir()
            repo=Path(__file__).resolve().parents[1]
            frame=pl.DataFrame(dict(study_row_id=['a','b'],url=['https://test/a','https://test/b']))
            frame.write_parquet(root/'input.parquet')
            write_new(root/'attempt.json',dict(workload=design.PROFILE))
            buf=io.BytesIO(); Image.new('RGB',(8,8),'red').save(buf,format='JPEG'); raw=buf.getvalue()
            row=dict(study_row_id='a',url='https://test/a',key='0000',status='success',sha256=sha256(raw))
            pl.DataFrame([row]).write_parquet(root/'native/0000.parquet')
            def archive(payload):
                with tarfile.open(root/'native/0000.tar','w') as tar:
                    for name,body in [('0000.json',json.dumps(row).encode()),('0000.jpg',payload)]:
                        member=tarfile.TarInfo(name); member.size=len(body); tar.addfile(member,io.BytesIO(body))
            archive(raw)
            result=verify_partial(root,repo,design.METHODS[2])
            self.assertEqual((result['credited_rows'],result['original_rows'],result['coverage']),(1,2,.5))
            self.assertFalse(result['full_success'])
            # A damaged body in a sealed archive is a failed integrity check.
            archive(b'bad')
            with self.assertRaisesRegex(ValueError,'digest mismatch'): verify_partial(root,repo,design.METHODS[2])
            # A publication interrupted before closure earns no partial credit.
            (root/'native/0000.tar').write_bytes(b'unfinished')
            result=verify_partial(root,repo,design.METHODS[2])
            self.assertEqual(result['credited_rows'],0)
            self.assertEqual(result['incomplete_artifacts'],['native/0000.tar'])

    def test_progress_uses_verified_urls_and_reports_retry_after(self):
        from benchmark.real_server_study.public_metrics import progress
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            events=[dict(event='http_start',monotonic_s=10,token='a'),
                    dict(event='http_first_byte',monotonic_s=11,token='a'),
                    dict(event='network_end',monotonic_s=12,token='a'),
                    dict(event='http_start',monotonic_s=13,token='b'),
                    dict(event='headers',monotonic_s=14,token='b',status=429,retry_after='60')]
            (root/'request-audit.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
            value=progress(root,{'row':dict(url='a',bytes=100)})
            self.assertEqual(value['points'][-1]['credited_bytes'],100)
            self.assertEqual(value['points'][-1]['ttfb_p95_s'],1)
            self.assertTrue(value['complete_credit'])
            self.assertEqual(value['refusal_events'],[dict(second=4,status='429',retry_after='60')])

    def isolated_spawn(self):
        if os.environ.get("FLOWDC_MINIMUM_TEST_CHILD") == "1":
            return False
        # multiprocessing's persistent resource tracker otherwise leaks into
        # later subreaper/owned-worker tests in unittest discovery.
        env = dict(
            os.environ, FLOWDC_MINIMUM_TEST_CHILD="1", PYTHONPATH=str(Path(__file__).resolve().parents[1])
        )
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "MinimumStudyTests." + self._testMethodName],
            env=env,
            capture_output=True,
            text=True,
            timeout=45,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return True

    def test_shared_reservations_across_spawned_processes(self):
        if self.isolated_spawn():
            return
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "counter"
            counter = SharedBodyBudget.create(path, 10)
            with multiprocessing.get_context("spawn").Pool(2) as pool:
                values = pool.map(reserve, [path, path])
            self.assertEqual(sum(not value[2] for value in values), 1)
            self.assertEqual(counter.update(release=6, received=3), (3, 0, "body_budget"))
            with self.assertRaises(ValueError):
                counter.update(release=1)




    def test_pack_keeps_journals_and_unpack_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run"
            (run / "native/.flowdc").mkdir(parents=True)
            (run / "native.tar").write_bytes(b"retained archive")
            (run / "native/photo").write_bytes(b"loose duplicate")
            (run / "native/photo.json").write_text("{}")
            (run / "native/.flowdc/measurement.json").write_text("{}")
            (run / "native/.flowdc/exports").mkdir()
            (run / "native/.flowdc/attempts").mkdir()
            os.link(run / "native.tar", run / "native/.flowdc/exports/archive")
            os.link(run / "native/photo", run / "native/.flowdc/attempts/payload")
            os.link(run / "native/photo.json", run / "native/.flowdc/attempts/metadata")
            os.link(run / "native/.flowdc/measurement.json", run / "native/.flowdc/measurement-alias.json")
            write_new(
                run / "native/outcome-index.json",
                dict(rows=[dict(disposition="verified", payload="photo", metadata="photo.json")]),
            )
            archive = root / "return.tar"
            pack(run, archive)
            unpack(archive, root / "returned", time.monotonic() + 10)
            self.assertTrue((root / "returned/native/.flowdc/measurement.json").exists())
            self.assertFalse((root / "returned/native/photo").exists())
            self.assertFalse((root / "returned/native/.flowdc/exports/archive").exists())
            self.assertFalse((root / "returned/native/.flowdc/attempts/payload").exists())
            self.assertTrue((root / "returned/native/.flowdc/measurement-alias.json").exists())
            with tarfile.open(archive) as stored:
                self.assertTrue(all(member.isfile() for member in stored))
            self.assertTrue((run / "native/photo").exists())  # no premature deletion
            with tarfile.open(root / "bad.tar", "w") as tar:
                info = tarfile.TarInfo("../escape")
                info.size = 1
                tar.addfile(info, io.BytesIO(b"x"))
            with self.assertRaises(ValueError):
                unpack(root / "bad.tar", root / "bad-return", time.monotonic() + 10)

    def test_merged_common_boundary_and_duplicate_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for i in range(2):
                events = [dict(event="start", monotonic_s=1)]
                events += [
                    dict(event="attempt", monotonic_s=2 + i, token=str(i), url=str(i)),
                    dict(event="http_start", monotonic_s=2 + i, token=str(i)),
                    dict(event="dispatch", monotonic_s=2 + i, token=str(i), inflight=1),
                    dict(event="http_first_byte", monotonic_s=3 + i, token=str(i)),
                    dict(event="network_end", monotonic_s=5 + i, token=str(i), inflight=0, dispatched=True),
                    dict(
                        event="closed",
                        monotonic_s=7,
                        active=0,
                        body_bytes=5,
                        logical_attempts=1,
                        stop_reason=None,
                    ),
                ]
                path = root / str(i)
                path.write_text("".join(json.dumps(e) + "\n" for e in events))
                paths.append(path)
            output = root / "merged"
            with output.open("w") as stream:
                merge_audits(paths, stream, 2)
            metrics = transport(output, 2)
            self.assertEqual(
                (metrics["attempts"], metrics["common_peak"], metrics["request_seconds"]), (2, 2, 6)
            )
            self.assertEqual(metrics["mean_http_concurrency"], 1.5)
            with (root / "bad").open("w") as stream, self.assertRaises(ValueError):
                merge_audits([paths[0], paths[0]], stream, 2)

    def test_actual_spawned_tool_shards_share_budget_and_verify_original_rows(self):
        try:
            import importlib

            native = importlib.import_module("img2dataset.main")
            from img2dataset.reader import Reader
        except ImportError:
            self.skipTest("optional pinned tool runtime")
        if self.isolated_spawn():
            return
        from benchmark.real_server_study.public_launch import GuardedDownloader
        from benchmark.real_server_study.public_verify import verify

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "native").mkdir()
            (root / "temp").mkdir()
            frame = pl.DataFrame(
                dict(
                    url=[f"https://open-images-dataset.s3.amazonaws.com/train/{i}.jpg" for i in range(24)],
                    study_row_id=[str(i) for i in range(24)],
                    author=["test"] * 24,
                    license=["CC-BY"] * 24,
                    landing_url=["test"] * 24,
                    record_id=[str(i) for i in range(24)],
                )
            )
            frame.write_parquet(root / "input.parquet")
            reader = Reader(
                str(root / "input.parquet"),
                "parquet",
                "url",
                None,
                None,
                None,
                [c for c in frame.columns if c != "url"],
                6,
                set(),
                str(root / "temp"),
            )
            resizer = native.Resizer(
                image_size=256,
                resize_mode="no",
                resize_only_if_bigger=False,
                upscale_interpolation="lanczos",
                downscale_interpolation="area",
                encode_quality=95,
                encode_format="jpg",
                skip_reencode=False,
                disable_all_reencoding=True,
                min_image_size=0,
                max_image_area=float("inf"),
                max_aspect_ratio=float("inf"),
                blurrer=None,
            )
            downloader = native.Downloader(
                sample_writer_class=native.WebDatasetSampleWriter,
                resizer=resizer,
                thread_count=2,
                save_caption=False,
                extract_exif=False,
                output_folder=str(root / "native"),
                column_list=reader.column_list,
                timeout=30,
                number_sample_per_shard=6,
                oom_shard_count=5,
                compute_hash="sha256",
                verify_hash_type=None,
                encode_format="jpg",
                retries=0,
                user_agent_token="flowdc-public-study",
                disallowed_header_directives=[],
            )
            counter = SharedBodyBudget.create(root / "budget", 2**20)
            wrapped = GuardedDownloader(
                downloader,
                "openimages-cvdf",
                frame["url"].to_list(),
                str(root / "audit"),
                dict(baseline_s=10, ignore_time=False),
                2,
                2**20,
                6,
                str(root / "budget"),
            )
            with multiprocessing.get_context("spawn").Pool(2) as pool:
                results = pool.map(native_shard, [(wrapped, row) for row in reader])
            self.assertTrue(all(r[0] for r in results))
            with (root / "request-audit.jsonl").open("w") as out:
                merge_audits(sorted(root.glob("tool-request-*.jsonl")), out, 4)
            observed = transport(root / "request-audit.jsonl", 4)
            self.assertEqual(observed["attempts"], 24)
            self.assertEqual(counter.update()[0], observed["body_bytes"])
            verified = verify(root, Path(__file__).resolve().parents[1], "img2dataset-1.47.0", 0, 1)
            self.assertEqual(verified["coverage"], 1)
            self.assertEqual(verified["credited_bytes"], observed["body_bytes"])

    def test_flow_guard_complete_archive_and_common_timing(self):
        import contextlib
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from PIL import Image
        from benchmark.real_server_study import public_launch, public_verify

        repo = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo / "bin"))
        raw = io.BytesIO()
        Image.new("RGB", (8, 8), "blue").save(raw, format="JPEG")
        body = raw.getvalue()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                study = Path(directory)
                run = study / "runs/fixture"
                run.mkdir(parents=True)
                (study / "gates").mkdir()
                frame = pl.DataFrame(
                    dict(
                        url=[f"http://127.0.0.1:{server.server_port}/{i}.jpg" for i in range(16)],
                        study_row_id=[str(i) for i in range(16)],
                        author=["test"] * 16,
                        license=["CC-BY"] * 16,
                        landing_url=["test"] * 16,
                        record_id=[str(i) for i in range(16)],
                    )
                )
                frame.write_parquet(run / "input.parquet")
                cell = dict(
                    run_id="fixture",
                    workers=1,
                    method="ratio-v2",
                    source="openimages-cvdf",
                    workload=design.PROFILE,
                    profile=0,
                )
                cfg = design.configuration(run, cell, frame)
                write_new(run / "config.json", cfg)
                write_new(run / "flowdc-config.json", cfg)
                write_new(run / "attempt.json", cell)
                write_new(study / "protocol.json", {})
                write_new(study / "execution.json", dict(source_root=str(repo)))
                write_new(study / "gates/openimages-cvdf.json", dict(baseline_s=10, ignore_time=False))
                before = time.monotonic()
                with (
                    patch.object(
                        sys, "argv", ["public_launch", "--study", str(study), "--run-id", "fixture"]
                    ),
                    patch.object(public_launch, "guest_check"),
                    patch("benchmark.real_server_study.public_guard.checked_url", side_effect=lambda u, s: u),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    public_launch.main()
                after = time.monotonic()
                observed = transport(run / "request-audit.jsonl", 192)
                self.assertEqual(observed["attempts"], 16)
                verified = public_verify.verify(run, repo, "ratio-v2", before, after)
                self.assertEqual(verified["coverage"], 1)
                self.assertEqual(verified["credited_bytes"], 16 * len(body))
                self.assertGreater(observed["common_ttfb_p50_s"], 0)
                partial = public_verify.verify_partial(run, repo, "ratio-v2")
                self.assertEqual(partial["credited_rows"],16)
                commits=sorted((run/'native/.flowdc/commits').glob('*.json'))
                commits[0].unlink()
                partial=public_verify.verify_partial(run,repo,"ratio-v2")
                self.assertEqual(partial['credited_rows'],15)
                self.assertFalse(partial['full_success'])
                commit=json.loads(commits[1].read_text())
                payload=run/'native/.flowdc/attempts'/commit['row_id']/str(commit['attempt'])/'payload'
                payload.write_bytes(b'tampered')
                with self.assertRaisesRegex(ValueError,'digest mismatch'):
                    public_verify.verify_partial(run,repo,'ratio-v2')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()

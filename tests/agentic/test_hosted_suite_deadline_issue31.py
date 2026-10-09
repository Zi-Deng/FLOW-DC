"""Closed hosted profile and current-authority regression fixtures; no services."""

import base64
import copy
import hashlib
import json
import os
import subprocess
import sys
import unittest
import zipfile
import zlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import check_runner as runner
import ci_diagnostics as diagnostics
import reporting_activation_v6 as authority
import review_batch_windows_v1 as windows
import review_packet
import review_public_catalog_v1 as public
import test_check_runner as legacy
import test_ci_diagnostics as old_diagnostics
import test_review_public_catalog_v1 as old_public
from tasks import digest
from workflow import WorkflowError


class HostedRunnerTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    module = legacy.RunnerTests.module
    good = legacy.RunnerTests.good
    evidence = legacy.RunnerTests.evidence

    def launch(self, jobs, profile, seconds=None):
        if seconds is None:
            argv = [sys.executable, "-B", str(self.root / "scripts/agentic/check.py"), "--jobs", str(jobs)]
            if profile is not None:
                argv += ["--suite-profile", profile]
        else:
            code = f"import sys;from pathlib import Path;sys.path.insert(0,{str(self.root / 'scripts/agentic')!r});import check_runner as r;sys.exit(r.run(Path.cwd(),{jobs},suite_profile={profile!r},seconds={seconds!r}))"
            argv = [sys.executable, "-B", "-c", code]
        return subprocess.run(
            argv, cwd=self.root, env={**os.environ, "TMPDIR": str(self.root)}, capture_output=True, timeout=15
        )

    def test_actual_cli_all_profiles_one_and_two_workers(self):
        self.good()
        self.module("test_b.py", "import unittest\nclass B(unittest.TestCase):\n def test_ok(self): pass\n")
        policies = {}
        for jobs in (1, 2):
            for profile, version, cap in (
                (None, 2, None),
                (runner.SUITE_PROFILE, 3, 1800),
                (runner.HOSTED_SUITE_PROFILE, 4, 3600),
            ):
                result = self.launch(jobs, profile)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                directory, summary = self.evidence(result)
                request = runner.strict((directory / "request.json").read_bytes())
                self.assertEqual(request["version"], version)
                self.assertEqual(summary["occurrences"], 2)
                self.assertEqual(summary["process_exits"], [0] * jobs)
                if cap:
                    self.assertEqual(request["execution_limits"]["seconds"], cap)
                else:
                    self.assertNotIn("execution_limits", request)
                for worker in range(jobs):
                    self.assertTrue(
                        runner.reconcile(request, worker, directory / f"worker-{worker}.jsonl", 0)[
                            "successful"
                        ]
                    )
                if profile == runner.HOSTED_SUITE_PROFILE:
                    self.assertEqual(request, windows.hosted_runner_request(self.root, jobs))
                    windows.hosted_suite_summary(directory, request, jobs)
                policies[jobs, profile] = (request["assignment_policy"], request["assignments"])
            self.assertEqual(
                policies[jobs, runner.SUITE_PROFILE], policies[jobs, runner.HOSTED_SUITE_PROFILE]
            )
            self.assertNotEqual(
                policies[jobs, None][0]["seed_digest"],
                policies[jobs, runner.HOSTED_SUITE_PROFILE][0]["seed_digest"],
            )
        self.assertEqual(
            runner._SCOPED_SEED_DIGEST, "dd2f9ec560213889e3c1a27b6096c4a648b1032d44a60177ce7036e2ad4ea5ce"
        )
        self.assertEqual(len(runner._SCHEDULING_SEED["entries"]), 71)
        self.assertEqual(len(runner._SCOPED_SCHEDULING_SEED["entries"]), 80)

    def test_closed_bounds_and_schema_pairs(self):
        limits = runner.execution_limits(
            runner.HOSTED_SUITE_PROFILE, 3600, runner.TEXT_BYTES, runner.EVIDENCE_BYTES
        )
        request = dict(version=4, execution_limits=limits, evidence_limit=runner.EVIDENCE_BYTES)
        self.assertEqual(runner.request_limits(request), {"execution_limits": limits})
        for key, values in {
            "version": (True, 3, 2, 5),
            "evidence_limit": (True, runner.EVIDENCE_BYTES + 1),
        }.items():
            for value in values:
                bad = copy.deepcopy(request)
                bad[key] = value
                with self.assertRaises(runner.RunnerError):
                    runner.request_limits(bad)
        for key, values in {
            "profile": (True, "unknown", runner.SUITE_PROFILE),
            "seconds": (True, 0, 3601, float("nan"), float("inf")),
            "cap_seconds": (True, 1800, 3601),
            "text_limit": (True, runner.TEXT_BYTES + 1),
            "evidence_limit": (True, runner.EVIDENCE_BYTES + 1),
            "schema_version": (True, 2),
        }.items():
            for value in values:
                bad = copy.deepcopy(request)
                bad["execution_limits"][key] = value
                with self.assertRaises(runner.RunnerError):
                    runner.request_limits(bad)
        with self.assertRaises(runner.RunnerError):
            runner.strict(b'{"version":4,"version":4}')
        with self.assertRaises(runner.RunnerError):
            runner.strict(b'{"seconds":NaN}')
        with self.assertRaises(runner.RunnerError):
            runner.execution_limits(runner.SUITE_PROFILE, 1801, runner.TEXT_BYTES, runner.EVIDENCE_BYTES)

    def test_owned_deadline_child_cleanup_and_no_reset(self):
        self.module(
            "test_a.py",
            "import unittest,subprocess,sys,time\nfrom pathlib import Path\nclass A(unittest.TestCase):\n def test_child(self):\n  p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n  Path('child.pid').write_text(str(p.pid))\n  time.sleep(30)\n",
        )
        result = self.launch(2, runner.HOSTED_SUITE_PROFILE, 1)
        self.assertEqual(result.returncode, 1)
        directory, summary = self.evidence(result)
        self.assertEqual(summary["execution_limits"]["seconds"], 1)
        self.assertIn("deadline", summary["error"].lower())
        self.assertLess(summary["elapsed_seconds"], 5)
        self.assertTrue(all(type(v) is int for v in summary["process_exits"]))
        status = Path("/proc") / (self.root / "child.pid").read_text() / "stat"
        self.assertTrue(not status.exists() or status.read_text().split()[2] == "Z")
        with self.assertRaises(WorkflowError):
            windows.hosted_suite_summary(
                directory, runner.strict((directory / "request.json").read_bytes()), 2
            )

    def test_hosted_worker_refuses_cross_profile_request(self):
        self.good()
        result = self.launch(1, runner.HOSTED_SUITE_PROFILE)
        self.assertEqual(result.returncode, 0)
        directory, _ = self.evidence(result)
        request = runner.strict((directory / "request.json").read_bytes())
        request["version"] = 3
        path = directory / "bad.json"
        path.write_text(json.dumps(request))
        output = directory / "rejected.jsonl"
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(self.root / "scripts/agentic/check_runner.py"),
                "--worker",
                "0",
                str(self.root),
                str(path),
                str(output),
            ],
            capture_output=True,
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(output.exists())

    def test_complete_hosted_reader_matches_original_records_and_refuses_mutation(self):
        self.good()

        def git(*args):
            return (
                subprocess.run(["git", *args], cwd=self.root, capture_output=True, check=True, timeout=10)
                .stdout.decode()
                .strip()
            )

        git("init", "-q")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.invalid")
        git("add", ".")
        git("commit", "-qm", "base")
        base = git("rev-parse", "HEAD")
        git("commit", "--allow-empty", "-qm", "head")
        head = git("rev-parse", "HEAD")
        merge = git("commit-tree", "HEAD^{tree}", "-p", base, "-p", head, "-m", "merge")
        git("checkout", "-q", "--detach", merge)
        result = self.launch(2, runner.HOSTED_SUITE_PROFILE)
        self.assertEqual(result.returncode, 0, result.stderr)
        directory, _ = self.evidence(result)
        git("checkout", "-q", "--detach", head)
        hosted = self.root / "retained"
        hosted.mkdir()
        (hosted / "hosted").symlink_to(directory, target_is_directory=True)
        # The real reader must reject a symlink, even when its target is valid.
        with self.assertRaises(WorkflowError):
            windows.hosted_records_g21(hosted / "hosted")
        (hosted / "hosted").unlink()
        import shutil

        shutil.copytree(directory, hosted / "hosted")
        raw = {n: (directory / n).read_bytes() for n in diagnostics.LIMITS}
        manifest = dict(
            schema_version=1,
            repository="Zi-Deng/FLOW-DC",
            check="agentic-quality",
            event="pull_request",
            pr_head_sha=head,
            pr_base_sha=base,
            tested_checkout_sha=merge,
            run_id=1,
            run_attempt=1,
            profile=runner.HOSTED_SUITE_PROFILE,
            test_status="success",
            clean_status="success",
            utc_start="2026-10-09T00:00:00+00:00",
            utc_end="2026-10-09T00:00:01+00:00",
            elapsed_seconds=1,
            cap_seconds=60,
            aggregate_limit_bytes=diagnostics.AGGREGATE_LIMIT,
            collection_errors=[],
            runner_name=directory.name,
            request_binding="matched",
            files=[
                dict(
                    name=n,
                    state="copied",
                    bytes=len(raw[n]),
                    sha256=hashlib.sha256(raw[n]).hexdigest(),
                    reason="exact",
                )
                for n in diagnostics.LIMITS
            ],
        )
        raw["manifest.json"] = json.dumps(manifest).encode()
        (hosted / "hosted/manifest.json").write_bytes(raw["manifest.json"])
        archive = hosted / "hosted-diagnostics.zip"
        with zipfile.ZipFile(archive, "w") as z:
            for n, b in raw.items():
                z.writestr(n, b)
        artifact = dict(
            name=f"diagnostic-agentic-quality-{head}-1-1",
            expired=False,
            digest="sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest(),
        )
        repo = SimpleNamespace(
            root=self.root, git=git, api=lambda path, **kw: [artifact] if "/artifacts?" in path else []
        )
        receipts = [
            dict(
                check=n,
                state="observed",
                run_id=1,
                run_attempt=1,
                test_status="success",
                clean_status="success",
                pr_head_sha=head,
                pr_base_sha=base,
                tested_checkout_sha=merge,
            )
            for n in ("agentic-quality", "flowdc-tests")
        ]
        meta = dict(head_sha=head, base_sha=base)
        with patch("ci_evidence.collect", return_value=receipts):
            self.assertIn("request", windows.hosted_checks_g21(repo, hosted, meta))
            (hosted / "hosted/worker-0.jsonl").write_bytes(raw["worker-0.jsonl"] + b"\n")
            with self.assertRaises(WorkflowError):
                windows.hosted_checks_g21(repo, hosted, meta)
            (hosted / "hosted/worker-0.jsonl").write_bytes(raw["worker-0.jsonl"])
            receipts[0]["run_attempt"] = True
            with self.assertRaises(WorkflowError):
                windows.hosted_checks_g21(repo, hosted, meta)


class HostedDiagnosticTests(unittest.TestCase):
    setUp = old_diagnostics.DiagnosticsTests.setUp
    files = old_diagnostics.DiagnosticsTests.files

    def test_exact_failed_raw_bytes_and_no_readiness(self):
        self.identity["profile"] = runner.HOSTED_SUITE_PROFILE
        actual = self.files()
        self.assertEqual(diagnostics.collect(self.parent, self.source, self.identity), 1)
        manifest = runner.strict((self.parent / "diagnostics/manifest.json").read_bytes())
        self.assertEqual(manifest["profile"], runner.HOSTED_SUITE_PROFILE)
        self.assertEqual(manifest["test_status"], "failure")
        self.assertNotIn("successful", manifest)
        for name in diagnostics.LIMITS:
            self.assertEqual((actual / name).read_bytes(), (self.parent / "diagnostics" / name).read_bytes())

    def test_v4_binding_strict_and_old_v3_kept(self):
        source = {"checkout": "a" * 40, "files": {"a.py": "b" * 64}}
        value = dict(
            version=4,
            jobs=2,
            source=source,
            evidence_limit=runner.EVIDENCE_BYTES,
            execution_limits=runner.execution_limits(
                runner.HOSTED_SUITE_PROFILE, 3600, runner.TEXT_BYTES, runner.EVIDENCE_BYTES
            ),
        )
        raw = json.dumps(value).encode()
        self.assertEqual(diagnostics.hosted_request_binding(raw, source), "matched")
        self.assertEqual(diagnostics.request_binding(raw, source), "mismatch")
        for key, vals in {
            "version": (True, 3, 5),
            "jobs": (True, 1, 3),
            "source": ({},),
            "evidence_limit": (True, runner.EVIDENCE_BYTES + 1),
        }.items():
            for v in vals:
                bad = copy.deepcopy(value)
                bad[key] = v
                self.assertNotEqual(
                    diagnostics.hosted_request_binding(json.dumps(bad).encode(), source), "matched"
                )
        for key, v in [
            ("profile", runner.SUITE_PROFILE),
            ("cap_seconds", 3601),
            ("seconds", True),
            ("text_limit", runner.TEXT_BYTES + 1),
        ]:
            bad = copy.deepcopy(value)
            bad["execution_limits"][key] = v
            self.assertNotEqual(
                diagnostics.hosted_request_binding(json.dumps(bad).encode(), source), "matched"
            )
        self.assertEqual(diagnostics.hosted_request_binding(b'{"version":4,"version":4}', source), "invalid")
        self.assertEqual(diagnostics.hosted_request_binding(b'{"x":Infinity}', source), "invalid")


class HostedPublicTests(unittest.TestCase):
    setUp = old_public.PublicCatalogTests.setUp
    context = old_public.PublicCatalogTests.context
    write = old_public.PublicCatalogTests.write

    def test_closed_g21_context_and_metadata(self):
        value = self.context()
        value["designated_plan_comment"]["id"] = 6076545397
        self.assertEqual(public.read_context_g21(self.write(value)), value)
        with self.assertRaises(WorkflowError):
            public.read_context(self.write(value))
        meta = dict(
            issue=31,
            plan_comment=6076545397,
            public_catalog_profile=public.PROFILE,
            repository="Zi-Deng/FLOW-DC",
            schema_version=7,
        )
        public.verify_metadata(meta)
        for key, v in [
            ("issue", True),
            ("plan_comment", True),
            ("public_catalog_profile", "unknown"),
            ("schema_version", True),
        ]:
            with self.assertRaises(WorkflowError):
                public.verify_metadata({**meta, key: v})

    def test_whole_g21_and_both_scope_closure(self):
        from test_installed_qualification_v1 import PUBLIC_U
        from test_reporting_qualification_v6 import PUBLIC_SCOPED_SEED, PUBLIC_V, PUBLIC_W, PUBLIC_X
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        g19, g20, scope20 = [zlib.decompress(base64.b85decode(v)) for v in old_public.PUBLIC_G20_BODIES]
        numbers = [
            6074133818,
            6076704991,
            6072111969,
            6074219127,
            6068705967,
            6072002965,
            6068144159,
            6064513854,
            6062530466,
            6061320190,
            6045434332,
        ]
        g21, scope21 = [zlib.decompress(base64.b85decode(v)) for v in PUBLIC_G21_BODIES]
        bodies = [g20, scope21, g19, scope20] + [
            zlib.decompress(base64.b85decode(v))
            for v in (PUBLIC_X, PUBLIC_SCOPED_SEED, PUBLIC_W, PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)
        ]

        def record(n, b):
            return dict(
                id=n,
                body=b.decode(),
                user=dict(login="Zi-Deng", id=29555112),
                issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
            )

        context = dict(
            designated_plan_comment=record(6076545397, g21),
            issue_comments=[record(n, b) for n, b in zip(numbers, bodies, strict=True)],
        )
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC")
        self.assertEqual(
            review_packet.g21_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(len(numbers)):
            bad = copy.deepcopy(context)
            bad["issue_comments"].pop(index)
            with self.assertRaises(WorkflowError):
                review_packet.g21_predecessors(repo, bad)
            bad = copy.deepcopy(context)
            bad["issue_comments"].append(copy.deepcopy(bad["issue_comments"][index]))
            with self.assertRaises(WorkflowError):
                review_packet.g21_predecessors(repo, bad)
        bad = copy.deepcopy(context)
        bad["designated_plan_comment"]["body"] += "x"
        with self.assertRaises(WorkflowError):
            review_packet.g21_predecessors(repo, bad)

    def test_g21_binding_and_legacy_pair_separation(self):
        binding = old_public.PublicCatalogTests.binding(self)
        public.validate_binding(binding)
        binding["contract"] = authority.G21_CONTRACT_DIGEST
        with self.assertRaises(WorkflowError):
            public.validate_binding(binding)
        binding["authorization"] = digest(
            dict(contract_digest=authority.G21_CONTRACT_DIGEST, approval_digest=authority.G21_APPROVAL_DIGEST)
        )
        public.validate_binding(binding)
        with self.assertRaises(WorkflowError):
            public.validate_binding_g21({**binding, "contract": authority.G20_CONTRACT_DIGEST})


PUBLIC_G21_BODIES = (
    "c$|$}%aYqhmfhD^<b-EohX8`lBCBXSLY8EA#po5XY|qSw4FX9LZ4h81fhy6f{)bu3@Aa3=xsObMRn^_IP^d@(iOl;r_uL1*GI!2(b~Z^qews~hjmzqj&CQ{9t<7WC6|J3JkH@C5S$%Jtv&rhJZPKj0y85yBVx9SFHo3ZbRo6{kRB2l`ac*6)$Ddf`)YPZirKRbr+%~2?SaVE^s>MIs{QYnL>x}(!DvPXWjcd~iE9_0$wTHSX{z%)RuFS4(Oj?$vvYB-*#q)PXg*_SU0KYq|+0@<sz-ynH;vUb%7G;sRvG;|2FsC&8J>A=pxogc9=Qv{9#qm_yqs3nMIr`N0Z0f2!n{?M=zxE5BVou!_Thz;{+_>&^svA09dcpzkaXb=El@(>dSB=d3+8nxLTA81U_D|imzp!v7b<Wb=Yg5%Y)|VFBJ<>*!yx3dUCWpd7jAy#GsP6HD%}3_9LtR?)({vn(hOWoU`E+tKnJ#%xmQ_<!2a7XcXAgY&sj)fB4MJ=zNuES>gcL{S6SmysB*rc+Wgq`U2K56JmTc?%Z1ALFr}9|SBXFF1-kRU}<VN=N#bzBov&9q@ED4W&30>~eqU>;Ak{!|pM<(5$u|YPAd~|j7)mP?2Wm&2unH&dFt;1aKc#_uD)yq6LAKt$)Sy?+=;p$!ckTy2v%|qV10wVZxA=Vu~Ht6_%^5+PT_-hB5L#7AFXC4{;$&os9)7CBNwySV<liM^eu|GW;P6}h4P2Uwim=?lICfD@mU+S)@aMQMi#k&!XfxU<b|GS7^+iIWu`1ZqZ@#|NH+|6v8w8{>KHyLA>d>^(ch8uS{v{PTi+FRpnX)}CPv}^CF^z{R|NLd_<cDXo0)UCy{-{>YyY4>S%Ud_kJuT$Q>mkA4FvSMK*zs{1f&an4AT*!HQ>9D=D+LH;wsPS_NSybH-TFCJ6WC#?RB8PMRoF47eJk-tawsA01d?eF`n?Z4T;RHV-sPaq8>WBTosNR13tQYT^`VU*J#yF+T6Y>~aK|D4hPib+wY9Bw?Vt;_k@TS~4=-CT~V8Cm~ONf@^?Zb9r*`Fb^ZAa!+*EBFDv+E$hW1V-U)svI+<;8vBifw5}41wgl3Taq%+vR(UomjX%p7l_)2XlatG<AK9%=-_YA!-BLg1_PXaD#o9LffsiW}DIifRoHt@J;%AOJ2l2<=_CXfedv^8!C2Z*q3Wv0sw;>WeI)Sh<$7xu*dqrZSE(d)0v);R_FfsbY&uNgLM;?`^f0(^@(Kq2f+jm&TwjE-K4RnGCd~*2nm5F&bfoc4i&IOBnjmdq&?t#0q+mqp^CYMwLLU|J=;XH@i@5|kLAh;<|eC0jO!Ue6wZ~FF4>cKTmo-u(w86a##kIkjpvh2Tnmd6$Y;1<r{cX-K=@bq9*CFVHWCAHB_}KZb~o6OvjQr(3eST!9NZMEvbRx8As8qck{K-)V$ww@y1I&CP)%_f=mMyb{k{=|fCg^)cG`6apx!<KC=}svufi33D;}IuVC1kO5JaAGN?QVjN+A>v-|*!wQM}TR5rI>p7ENnF*$BM^kDF7CGXQWudVrSor^(D5ZCc^1j&BjCV;^xze?OG7r&w?}!m%Mv>^N-f*a0zZh0vT?1-IWTL<sMU3H;oVHJ#F;S*twi5-tOKW@r(lN0hP+MJ_!31mA#S4z`5JK*ik-F$T8!hOs7X{e!wHy|0Trg;Vw$vN^#dMK=o7?trB|OIe#-tleQ_RSPuE%CtC|?YXr+cu8olcj9lw@TfcBpE~6~&_1g@%}JRYcnE;V3Q9n012ld0_6|-_<1nIS#wkPlJrz~O=tRK=P?p<W+LbL|M$(eIoLV}b|1SOAl6-G39`N#~H}5~+z1sZz%iZTUn~$G9{CM~FjRqF@7=Q)TLP9`7SWjWKIAPrY?aV~dW?MjFa0bNWj!b~hJlewsx-A>ot_0Tj)5m`WZ=jHVK9;wF-vM~w;I%7=3M7^Y45($Ik&Zwx4`@y2@E(F=``|?}jj07&1!33ccOPHheM+*rq#K1(K<NwzL=*NE0a<%U;ZikLh}6hrW6Bj1nAQ=okN?J97TSc30^@c?<JvfFTYJP;4Wa-!4x^&+Cm)2u=WL<a<VCu#;Ppl3)F|MPo}COqLHRU$a&p8z^R!LPwh;cSg8EwVAb~90G!g3;xBn*UX_uEdYt}2>6<=bOX_G#XAZUwq6<v>&*nm>SZ5`u36C@geO1_v7hmnV{8CG>n2vrd7Lup{Nd^+<p@6Ck0Ga}@&lMW8(mCBX{2{Bt3vW9|ym~r{3C^fci$pJ4T9Lu!efJ1~e?g=Z!@0k73XxLG2Q(RG;rEG#6^!=OPJge%pW(tl(SgqJGUSq%D4gX>7$p|wNpx=1a$)X~ug5VvZ%;Pktz*<+}R6qpM3#irFKhB$->oOwdI0xZhQSHE@ETi$!3=PS=f_%$*-$PO`cc(PnGx!I58020-I3V3!PSK3QtsD!R-dYI53Usbju64z^48=l9-ryNf9!w$gar050^Q`hHH-Z^#$ms-HH}}_2US#^;d@{MwjG@1RRWFo-70yTWQ1@)I;<Jt<Q!Xdq;<Gi$gdm;RhXkJkv1@`5fsw)q>IZVZ0AXyj5*SW|3k|Zm4`uGR;zE`df@yI)b}hvv5)0)f&(Jn8f(`TTjps}orQ<m7nleRlO9t0n$U1|a^O}6F4<FxrdinY8{ZH{vzudikGdkuhD@06A1!a2$S#8e%ff^uz0X*`^CpQ%Rc~zx?Np5Ky7P<yc7v>fa6GE&UX>!296d93+k`rajcYEa6*3d!9%=Ytv(pn<YnJ*dOHYE&$EY2fHB*8ml9pHBlCPNT$gDp^4Ym2g99UJkiK%S}zNwDc)7Ic1Z;B>7RTQ7U5Vf4wxvjvNhx!WmK@xF!&$o(o}SfCh<mAnKn-PR|`8#r)rB!ZJy7rrUX^_^H5U=CZ{Lx0ZO_dN_HSAzP<6DtK9@Oz7BNb)AslO%&u7YL2nx@O{Qg&{boO^KW^P=0#zdc=A7x8Kdi4$#t4_^b-XN^&S4xhdjebIE{Chb^-IkXzmonQgMdSua_J>qIS3=<ucM=JrUenE@O>W@$yXAY6;iGXOgx5*lg!uD%X5zN`9@jsA)d`f#ADgc#A|ID$&h?L(bUgVoa6df|epHz=kPWDMSHVVCK)AYvqzi?vRcao>w%WPWU{{X=RQ8SP26kbrpICqzsg$IaqMdrstx_#JY~I7GoDsP3_=vrZ38q2$}|Z~~`Q8)fwx(a4D<<++7TRHUG^oFr4t92kzA+BZds_JAEKWl@1SF;sUTzh{>r;*f{L54niw*{37;#6s{67uN&}nu8in{Uak51q~)8*X^JZP!ht5?nM4UDWB>j@p`4C0#74MNxF&?k0fhq=X!gze-SH+NF6UG2^VMrG)ZknYYu}F!65r_<N$0CFbtw=ks8W-fE4ysKuB=;J@#HMCv>x~QS_yS*FCYgpkd;*0(kSl`j{{SWr4PM5dS5tDlk3as1yLa!AQzCa3=C~VPTn6=e&GaSg_S2F$k#{IiIB^kdaykHo5B<Pe0RRkh&z4a|H6Q>|NlkN}C2>u>jYPWAd=_NMVr=&cSOfxsB%`iDo@+SZSgnAzdZWPF#=$%$0+T<V7N|c^})b=O0NxSf=+Ya*!Y_LR;rofej&^0eisg>JuF5XWC<y3^PK4JvbCs0V8A6?C|Y(*W+8ZXv%V7_H6<?fl!k{#)A}c$C?SD=8zIrB*-qU=M5Z~Lvoq))dAM8#A#n4v-xDan3_5hf|HScUiN%VDM9YjAz}e*u3->xl{FJzcPMsP9fbW*=i+^R@$0heM<Ikve{EjAeGA8E3j9bp6&&Jv%x+FZx18Lp@G0<K%GPk(i|~b9J&428<#_zvY5|an0x>84%3jAlNRNuY_Ya7_?@9Id!+0(b*VKU5VKJIn0H1{uEH3G>j@JTuzV0YDoiCD44PnS^#1TB1C@PJwmVuFslH_#B&tN_;c}vPbWA&ziT%&(LNC-}7A5=J$#9v~xa(ni9<MUFblAI+c%G4%fe7Idq$Wmc&TMqvNBOVlhf;|%Yhx`+6NMslg0eIK)wM&q3xIwm!Xjtr+&W4JV&t%}xK9|wVf%qQQlD#u7;rI$uaQVIM!Ac5H76~JQdtH%N%x_llsq9=#3?sl2J4ho%%8~g~pGNoIou1u*&(yT%j?dXr(OENJJWG}gOd}G!=HhAB)<=SY%M{i8{EV>@xs^^s55{SEc7^k>aiFSN%i!|S*NjQk(p3nA0cKT;8O_Eo$boS83-Xc|q*VI!-DEz$8HX|vvKQ%z>1~0dNH--{hy%C)Nnd#tUXRID5dqnH)~0XlA=;9?Q5%8c7gIk7q&p=Vh?Bm8V75>i6<0mO1Oy^+?Kt6u#LpTVPYu_7ObTq?LG{AW`vQ2qgLzQqz>#h-Oa6T^nPbnE@lE0yxf_HyVBDIANJ&7$cpo<SuCCf~14UT#8z1tS+CvmuB$iiKKQO&clqnHRs8Jl#lfrbQLGDSG83i1;d+n0Y%IM>e)*YBRX__@M?>KYUr;xA1AkP&}$7KhPa?~vx#&ngj_)L$OpcVkIdqE_h^k$HTW`^>Z*r#wr7v$t-NN`fJMdEbihtP0WG5!U#kdC4!IaC_8rDpJn101x)9~ae=MB1c=dxS({sl~YMn9zlsOKTZSB|~C<nSu2^OzN9ZL|99r1OvOTOOS>{^LB1M5iMax4Gh`g;Bir%I!O}x90>*!gh87D>LPTL?d~<l*G92=QYe9li7<jwXi%5+c!ykoC_eLMpAAxbwjggoaJa~ZeG3?t`AIGDRt}ct_tfR$s=L~ZXE<zIJ<z5l6(ZOR06c>jD%)pRf=sYXNyLYR63(Kh(B=;{R4B(JOYT$L)CGgOtUwSZ_yRhyCb~_`Q&_UNlI)mL;DF8Wq?VJW(6}}H>5z78+-&aBbx~o(BpRXv!puP<3sz(i0!;3^IekREFrkomUzV%j_&)E&S`2OyqLaRDkY~&BYPq<{$2W_~cA1Xh$oVqM7dMl9IiAjTD|<a3FSCf}7*^Be<a%;_vm4K|={&zlXUp*fEN7YTrs>TrpYL|F@nVvP@Yh$BrsHICb5W%Uvjp{n)2d_ezL%5*k0;p9ZaIk;%dw4@b32Z=%k4Ct&g}Iy9(TQ%U273zk9E0RA{AjK@GWvW5XTE>x#SmG(Vs0>H;egvxtOk~_&k~5REz8Ra<ZCogvf~vlG;aO@Y3mIK3S}N;mF?$Azj?NSS)yJ%nUTH!20dUflmTEb4fWo+AUX$NjjNk`Q&;vvy1C-wwf;%<MCv6Jx%Ak*=&(!+tngX=uk=mZGM4~$RR)_0_F+@_ibJR_(!~hIJRV0ekP#rj%iEUX!<lVzj2N`4#<|zR;y$_8#A)aCmkU=l^b3VzvDtEIEf_2lm4db>6A(eU<O30sRzmy+vFjngM;YBBlDKoBw?IVbrBqgWG#(^%7oXi-p!;*^U=a7_vwEIzn1G|03h}R+_0%4R3Oy0)6wGZfBT>5XtmY@CE=8OG7_=RH%;D&l@I#nI>4dra$g+jH~4(3oQxS3cV_+|8k0c)PJ4X8n5Z{)91Xh32H+ukhz$3YY1v=jX)DMlQxqC}3@fPAMww^e8xqlCAGL&VP{K+_88R>C3WK2&KVK1QAD1i9w1pHkCP*j6LSfwFG3QGaRV(IW{p>1ml0)52ruxkkKQZ_4-*6Z#0Uz9XBPzt<*CkWFz;JbD-N(K83b(F!33;eqelxgu5E}ipY3aj%xDTEq-{=Yx`Zngys=w0jQ7if~4@0#(U(zS!AH<)YzW7pwS~6!{p+UpMIVYZH-h_k%(r;_zEH}Pb<N1aPJJGV0*0W<O)n&YzZ*>cQ3KjjQBQU;tO~%_d6G7?W_}E*ZEWU@vRd5Zo_+rRvLOdPuB$LnP!<<_b#t<mfODqh5jjF^>#lgt8Jzpf4ih5v{C&_Yduj0^LVO!j&Y<y$QV?yK$%GO^Y3<`(wr7F(eA}`#{U~~n1SUO0qE8B2INQ-@tP6pjN-`0xyX5uiXA6NvjbFy)ypeHRtN(qUpAwxDKg-gmGd&|<ng2X>>{j`NjBW$6C+K($FkIW+-7N^K0?4SkL6A>Ne8&3ZD(j0MX7|ns_7L_&!v+y$%n%+D!h9wgd-4=_r)GSZ2@rgzb^4yxg8PiJ0vm~$O1_c4bu(8En&Lv*}gG>Y-Pqd;?^UzOY@h813wLQVYkQqVF75Mn-ai=n^R8gZ~dNiXXO?@5_DRyV=^hwWtQ2pn`y4YP#bBPLltPA~6xrQr}1NpC?^b(CEBg3|Eu$5uC(q|XGSA3yd9@LD4U$c}pBAtyI=im%-f7<#fQb<HQbaz!&cDa2n1A6)XwR}zUos-#YHCeGPFTU5gPX-14MdTmGYJQsBB>$-;U0e?gp59z9lHZbFlV6h0$$m1sI56V2c0||S)k6F4Y;3;wrRd^f-@kl%YeLHIM9aRv+oy%n*;V}4R=G!U(Yp4tFZ3RgmhJgczyF}cy=JduZWK~-4cCvs{z+?R;96=j{@RV)tJ8H<-H?Vm&vpC@ypgl13HLP$_4Y6oCbqmKDNrwm3s-V21r>ssP$Z7}88Rxn7Q}M=lr(7(HjwTqhriIo<1Tyv`b|LNi~ePz1SHvn&IH-|lhgY6?c3lQHR|S;=9<ih_iz8pd(L2fI>4c?Y=`@`ru%4)`d>{9Z~`s;n;~>W!#vuiO<7PD-vghI@I5p=H*L}nn^3&kNF{T#pH5=3P8@>3aX4*qQD%^4q@Oz0**=*Vi7sIh3k3NLh}Xt*C}GZ<OEt-XH^Uhzh1Lys(LBIPOHHyun!4<9lWmzN@CNWXFyMh(665i_WKbgVkuXdaO8&qjFMUzVC$TXX>~&_w7uo~O<IL=`<`Bxs!#dZnSHYI~!-vm*GC$sZ`uVe`idvqrPDi!rY^@}h8p@=iM!91mO-q+Zi!Kye28AtZ6yEpCnq8UhdB&7W_CV0AP^v;<^l2(qw3=4{36SXtb>mhs8MMc^VlZXnJyVh-dQm~%kRZGtnI8tj0g=uY_$Hv|hmdMhd@*0+i(Rqrd?JKB?K%09vWIqRLrFgx4nqx!zO0*2%aXB<eWaM4c$PP)s51}`2dk(i=Y|6&BrkS3j*EWYSO~*6;pGHxmzLfDP{D<l9YT54bG5V17=~`zu$gwzGULgnT1gZ|0~qtx9@2ZNtOK~IPOduf!@Bg205uRVW-~a(57gEZ@Fy>XFlkd{A41N>*i!@k)egVOO6b?}H}dDEN5r5u{`pj#xU?l7{B$-CaQ8{EpVsc1I5H(14FZ0m88T4ZMPVLDc>$SyU$Wx)a^KMxvP>s^@GVKUaTp+R5=68A*94TXN*;o;!m$TOzQ6~K0MgQQVRGu3xu|`I&;-*RY2M2GB{b<|@PykIjKr2&zJXFH54q&RG=M=>#LHKRTuDQ|)IX=u8EOMX+~3n#jOWHRBHs;q+I^-EF<IH8IU;T^nXl#Lgj~G-2uhPX>P~+nkIZVqb*XlT$z%z!R5co4_<-S3xt4jhz-Z_?x_({=Kyu*sxYA1F3Ee=QoX-~>R^m!UMN=5Wlz5DTvRE1(p;;fsa2b{$QTw?dKMlg83wY33XeWW7dd>hns6n0&@@#6>!3p~zQ+`v$zny|3hvIKPqB9&+lA-oTBTCv9mQo<Z!w&z?=87Vnr1GOW)H%@+p~vGqBlAl?s?Z-S@pf`Cw;(MO!9y;#=y<NqGRPEzyj;a3m=JpyD!E|NH`@Q&2_OWk7*dxfjrHh}`ODiQWiA!SFnK`)n1kXY0}@7ty@oLonY^IF39OIDaOl3!H%LoY`{u#v!ce_QR@z;o7UQ$c1K%}}(e}{S&1>SK#F4LFUND^=tHlsHCr=Emt5YbbN)LlmAu)b3cM7DT%HO8$cQ+EMYc~|&z8;N6uoVy=K=F5<o6dgY4?zUvLKJ<izVF46-X}MMR9IOv^$LUr>7yxE3?+w}-<=9g1phuB$9;LNrjMorSeHJs7Q3`;%j4Fn{l4|gjcNY^gDEc3guXbn4qG5*7zIKeYPSuIY|@xMz3b<@lesp!v)ad!$`;ZOZus@7io>k7dv8EF2^aMxqt$$@=I>uZ!lR9U?`SYzJsnd~dlrS+iN(y{;<zc@r?|=ddJ&#L)mPXmzj&9-R|gr8iOjq7i%*kt7*V<>hFRn>zthC0qBoTOuuYv*wZ63UBoV#rWSdugQJV{A+W+obobp1e_TU&6g>WU0+?JtWJsdfgw2cSoY4DtE5I0aKF$Se8C{uED2rUu8Yrug|Ed^6jHzbCP;MNc7@gR%%$)mZN%bB-O0Pv;8T=MtlW|mz-#b<d_FpNdX%SvH#EHJ@!@2^jaR?n1{2LQPq&a6DiBN%)K5Lx!1F-UNH^JeW^OMWk*w#Q-QnI`qxyKez|;L64;W_v?R4I^d$s(DQd4Bn`E+`N`F*1r|irzw_RxJro&I!J|dMg+O<Wrt~QtR-D*dd-0vG0c-CK6AnL?1%H97bZ}S-`D08l;6qeb$uSZD$Pk*6yD+1ceYJdG+9^nUFbn~y>BVY0KcO(2z%I=wW-^0QL`#o@wfjL$9|eB4vnJQo9ktot`^gHv0N?U`EoalZ*HcGc)VCFx3incc)VSiIR5_X{{ZN<7?S",
    "c$}SAU60!~7JZ*z!9|LuMMsuoS+*zXE-=VUlL696kPO=Wz+ys^l+8vKwInq&?g!BSuuuJa{Utq@q-<vj?881JQ7rND^4xRIy?nx6PD|#5=@qj|m`><cbFWSJjB(-honjl~ypq|yu^*e(+%xsiE8)X8?&Rbf{h%CsQk5qsHyiCj5Gx(?8^tJ`RbH{)s!U1kA8mpE{%6ouNw(UB1yVV^?l`t%|DwFo)^5;VRr8{th~6Tve}A#@zIV^^e69UvSWSf4=Ks{$h3eM%H?QA(pIyAjwR3}Vc~w3MPxxf3I-kvp#k`)?)!E|93A@3rTdq5gKg$08>u-)pWF$Ouo_A7rYi910wOT612fr~^|ES{WIwh2I-0oPTJ0ytYEUm=Bp0m#2{XOIIPCH|BVd7$(P-sU-$Ma^`@{YaK{?)Kzyzi~K<1GV*x@)kdk`s0={D9vGixC?v&c}~)AnuLbO$&x(5mT>qhwslFP+|`0(brTtZxt1oj>?8kY<Rb(_}7LxGgzT=ubj`{U0&pk5rabkLcNfsdQf5r)!J~!j1U8*h8zP3*uL-a-ySbjS(mREca$d~Jt0aQ6`v3EH<0YSZZsD0R8OfzWL(ILFsIOoeu0~ky?s|r*`9;mEb6W924o#h9tvEQ*<-?FcO?_N>x`!~oU#b$?+p$G;AkUct8Ab$YR^n76TPt`@CAJ$MbrbbIS@s%twK3zUq-;WoUiFrBEWbXw!f8%%N8HsrsO95j=iTh7wN?ft(=4vTmDfsx>dJ0%&h>Fw?f}?i+@^!Vw|!Sexo%^ibb(p)Mv6dtIO4b7fMOF5VAfi<)WBY%~H*0#X>Op+LiV6_h0{8E{h5Keq&m7h(p`5_pi>U^?WMIsi^8@Dd%M)Rjqg_tHo-*oX(nRDg|#AO*xxabv?%q<$T5^Ux-;zRn1II7mEqI@&sF}h4$?(VH!o!3~kFgZY_$ouO6xnCJ?2lb`j@OMn(EzKvq7I#;HyyW*X20Y$tR;sPj>S?DFsDFK%A{Bb77OECcVUs%}`Jygty@MXGYo-dta%x;;B7C+sy~C>PbC;2%Ew=_mGq_3hve8aYZfy!gOA|D4Tg_VdrItl2NWd|)eb7M0#Cj^0d8rsFR2|7Vw{BdY_SP(-5hQ;PqzRckJGPyf9C)BmA_KP?5G-zFXhQTpBKksxyD&T`lbYGYMzoF=Qu)g3;~-@eONmLvT`=BMmiu$aa8Ii;Hq-N(+{cZ?W~D2TV!!IPqD564G=<sf`nL{T4QF`8CTAPf<7G9ZXel|<A=Kls5WBUtUo5<<nHO(M;s0B=Vd*qdcEKw9&XObS8Jv!coyu3Lo8qjRY{f=Q_7fuYfQwLpZRCBz+CY8x6US_KD(0u0j-!qKO3vkAK}f$wpr5RbgaDNnBCMt4mXcw~F<pl)dJJJKRy0H{;6hE{z78vMc<j%3+-;Fdswk)wt!7o(S<^YRuIXZL8J2e}0|BhLeR>OrVJ^=Y&*;2ONY^9M4o)P~<_1i#-zsOdPtzzGtl5&hV2NVigk6G5$6@Sj6)KlY%}qGOMf+6B(hCt{<-N2GPtiFQCIrbt6C!=Nxaaq36d9Emj6{D{+!y$tlCUd?!GfG+iNYR(Q}w|th9X$YSslL-t+2hCE;2TTc=Ype^8Y)e+<RL`Li1=qurpqpKy|8iF3-%U@)=G7Y1J}Bb5>Jjr}rVcKiOVw~tD7BpX;Mdgo$Dt>zym@u?o?TwwT)n$I8rv+IsU-mQmIe~47ZTkfGpl)f8V?>zJ;p8aupHCSY+9a`(?tjmb%*sy0H}xzV~(-U6LI=s1*2z>wX$)<%C9c+7&WRdAhS}jYBXhpDIhG%1oy~g&8V*tg|{620^BfNyNjCTapZ{s;|WK13S-rHC^Wb&L>X~})J+S8q>*DkmF!24{ZK+^x`F(Xlo_#8B>6q_jx2x})24F=56Cf>f$ia(FrzSrLAu~G4iUIY6Wq3M73~$n1!6$_F?LH_pvSr_98JTRmA}B;RTXja0!u(hd?CL2ba1MQ;*=>4$BnP4$t7NY{nvT%B@nPd0%Sy6bu<m8IdH-*xotJ9^N@tgfuAM(h(6JgnxT_nc>o2L`67uSsniAdb0U7*sx}8qo}MJ8T!4u!VRp-US&%hbPyt%YHkf(RIFEzJadqE0I2k6FSs+33TztXXozo8I9xXfw{Ghm4)Z>$;L43_adETHbF@v}<<KC-n?*rKammqJmO<>YOR2>~o@E&%8n+v)YS=YYh?5|3v(D%K?6b=a>zs%UPqM+IQPEq3vPK#cOzTA^D&zAHAmUP~Ig%MJ%ZS=VD{*&!VZHE?}vaY-VrsEc$)#WU7lwi?9;r;|0qkj89iIOG4QXFQ`Ts^=l&fi`IfP1cG7Jfl3&|lJRA*XI`$-K}^aCk%#twLYv!>V{K*(%Utyr2=8A9v#G0N*ATWDy91<m12)Mu7-MG9o~S6Ju)d(TxP(BDHl?!#$=5MH!d@W<Dg*go_8l>7ZG%g>mppmqq-P`1t9aa>Cw2{4B_LcdW`VdC+VaEM=u27Zol!ad03d>(pBI_g2^QE}WE_^GHmj2FmyGrib3!(Adku)wks<v|_n0JlON~U$Ts)+d;Mm=$TxWXDd-RSv6HvHd|F^+3Kt+vPw#t)@m-xCCjqEo%{h>cAGT",
)


class HostedPlanTests(unittest.TestCase):
    setUp = old_public.PublicCatalogTests.setUp
    catalog = old_public.PublicCatalogTests.catalog

    def binding(self):
        return {
            **dict.fromkeys(
                ("local", "hosted", "source", "context", "identity", "policy", "inventory"), "0" * 64
            ),
            "profile": digest(public.PROFILE),
            "contract": authority.G21_CONTRACT_DIGEST,
            "authorization": digest(
                dict(
                    contract_digest=authority.G21_CONTRACT_DIGEST,
                    approval_digest=authority.G21_APPROVAL_DIGEST,
                )
            ),
        }

    def test_coherent_parts_and_schema_dispatch(self):
        catalog = self.catalog()
        self.assertEqual(len(catalog["components"]), 2)
        self.assertEqual(sum(len(u["required_ids"]) for u in catalog["components"]), 129)
        plan = public.plan_catalog(catalog)
        self.assertEqual(windows.validate_current_plan(plan), plan)
        with self.assertRaises(WorkflowError):
            windows.validate_plan(plan)
        changed = copy.deepcopy(plan)
        changed["profile"] = True
        with self.assertRaises(WorkflowError):
            windows.validate_current_plan(changed)
        changed = copy.deepcopy(catalog)
        changed["components"][0]["required_ids"].pop()
        with self.assertRaises(WorkflowError):
            public.validate_catalog(changed)

    def test_native_fixture_complete_over_legacy_catalog(self):
        import review_capacity_native_v1 as capacity

        components = []
        for n in range(18):
            rows = [
                dict(id=f"{n * 100 + i + 1:024x}", artifact=f"{n}.txt", start_line=i + 1, end_line=i + 1)
                for i in range(100)
            ]
            components.append(dict(id=f"unit-{n:02}", items=rows, files={f"{n}.txt": b"x\n" * 100}))
        additional = [dict(id="f" * 24, artifact="guide.txt", start_line=1, end_line=1)]
        dependencies = dict(
            **dict.fromkeys(
                ("local", "hosted", "source", "context", "identity", "policy", "inventory", "assignments"),
                "0" * 64,
            ),
            profile=digest(public.PROFILE),
            contract=authority.G21_CONTRACT_DIGEST,
            authorization=digest(
                dict(
                    contract_digest=authority.G21_CONTRACT_DIGEST,
                    approval_digest=authority.G21_APPROVAL_DIGEST,
                )
            ),
        )
        with self.assertRaises(WorkflowError):
            capacity.largest_fixture(components, additional, {"guide.txt": b"g\n"}, dependencies)
        result = capacity.largest_fixture_public_catalog_g21(
            components, additional, {"guide.txt": b"g\n"}, dependencies
        )
        self.assertIn("catalog_sha256", result["manifest"]["dependencies"])
        dependencies["profile"] = True
        with self.assertRaises(WorkflowError):
            capacity.largest_fixture_public_catalog_g21(
                components, additional, {"guide.txt": b"g\n"}, dependencies
            )

    def test_original_family_and_all_cross_part_edges(self):
        catalog = self.catalog()
        self.assertEqual(
            {row["source_family"] for row in catalog["items"] if row["kind"] != "cross-boundary"},
            {windows.FAMILIES["agentic:review"]},
        )
        rows = [dict(id=row["id"], links=[]) for row in catalog["items"]]
        source = catalog["components"][0]["required_ids"][0]
        target = catalog["components"][1]["required_ids"][0]
        next(row for row in rows if row["id"] == source)["links"] = [target]
        public.add_relations(self.root, rows, catalog)
        relation = json.loads((self.root / "public-catalog-relations.json").read_text())
        positions = {key: n for n, key in enumerate(relation["ids"])}
        self.assertEqual(relation["cross_part_edges"], [[positions[source], positions[target]]])
        self.assertEqual(set(relation["families"]), {row["id"] for row in catalog["items"]})


def complete_g21_context():
    from test_installed_qualification_v1 import PUBLIC_U
    from test_reporting_qualification_v6 import PUBLIC_SCOPED_SEED, PUBLIC_V, PUBLIC_W, PUBLIC_X
    from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

    g19, g20, scope20 = [zlib.decompress(base64.b85decode(v)) for v in old_public.PUBLIC_G20_BODIES]
    numbers = [
        6074133818,
        6076704991,
        6072111969,
        6074219127,
        6068705967,
        6072002965,
        6068144159,
        6064513854,
        6062530466,
        6061320190,
        6045434332,
    ]
    g21, scope21 = [zlib.decompress(base64.b85decode(v)) for v in PUBLIC_G21_BODIES]
    bodies = [g20, scope21, g19, scope20] + [
        zlib.decompress(base64.b85decode(v))
        for v in (PUBLIC_X, PUBLIC_SCOPED_SEED, PUBLIC_W, PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)
    ]

    def record(n, b):
        return dict(
            id=n,
            body=b.decode(),
            user=dict(login="Zi-Deng", id=29555112),
            issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
        )

    context = dict(
        designated_plan_comment=record(6076545397, g21),
        issue_comments=[record(n, b) for n, b in zip(numbers, bodies, strict=True)],
    )
    return context


class HostedBuildTests(unittest.TestCase):
    setUp = old_public.PublicCatalogTests.setUp

    def test_actual_g21_build_restores_whole_git_line_mapping(self):
        import review

        gitroot = self.root / "git"
        gitroot.mkdir()

        def git(*args):
            return subprocess.check_output(["git", "-C", str(gitroot), *args], stderr=subprocess.PIPE)

        git("init", "-q")
        git("config", "user.email", "fixture@example.invalid")
        git("config", "user.name", "Fixture")
        source = "scripts/agentic/example.py"
        test = "tests/agentic/test_example.py"
        deleted = "scripts/agentic/deleted.py"
        original = ("# " + "é" * 50 + "\n").encode() * 2500
        current = original + b"# appended\n"
        test_raw = b"# complete test fixture\nassert True\n"
        for name, raw in ((source, original), (test, test_raw), (deleted, b"# deleted\n")):
            path = gitroot / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        git("add", ".")
        git("commit", "-qm", "base")
        base = git("rev-parse", "HEAD").decode().strip()
        (gitroot / source).write_bytes(current)
        (gitroot / deleted).unlink()
        git("add", ".")
        git("commit", "-qm", "head")
        head = git("rev-parse", "HEAD").decode().strip()
        repo = SimpleNamespace(root=gitroot, name="Zi-Deng/FLOW-DC")
        limits = dict(max_source_file_bytes=250000, max_snapshot_bytes=12000000)
        legacy_index = review.snapshot(repo, head, self.root / "legacy", limits)
        self.assertEqual(
            next(r for r in legacy_index if r["path"] == source)["omitted"],
            "file exceeds configured size limit",
        )
        packet = self.root / "packet"
        packet.mkdir()
        indices = {}
        for revision, commit, directory, index_name in (
            ("head", head, "source", "source-index.json"),
            ("base", base, "base-source", "base-source-index.json"),
        ):
            indices[revision] = public.snapshot(repo, commit, packet / directory, limits)
            (packet / index_name).write_text(json.dumps(indices[revision]))
        # Working tree text cannot substitute for immutable source blobs.
        (gitroot / source).write_bytes(b"# dirty working tree\n")
        for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
            (packet / name).write_text("policy\n")
        context = old_public.PublicCatalogTests.context(self)
        context.update(complete_g21_context())
        context["pull_request"]["head"]["sha"] = head
        context["pull_request"]["base"]["sha"] = base
        review_packet.build(
            repo,
            packet,
            head,
            base,
            indices["head"],
            indices["base"],
            context,
            dict(required_checks=[], max_snapshot_bytes=12000000),
        )
        mapping_path = packet / "public-source-mapping.json"
        self.assertTrue(mapping_path.exists(), "actual G21 build omitted whole-source mapping")
        inventory = json.loads((packet / "required-material.json").read_bytes())["required"]
        expected = []
        for revision, raw in (("base", original), ("head", current)):
            rows = [r for r in inventory if r["path"] == source and r["revision"] == revision]
            self.assertEqual({r["kind"] for r in rows}, {"changed-source"})
            self.assertEqual(
                {line for r in rows for line in range(r["start_line"], r["end_line"] + 1)},
                set(range(1, len(raw.decode().splitlines()) + 1)),
            )
            artifact = next(r["snapshot"] for r in indices[revision] if r["path"] == source)
            self.assertEqual((packet / artifact).read_bytes(), raw)
            expected.append(
                dict(
                    old_omitted_id=hashlib.sha256(
                        json.dumps(
                            ("changed-source", revision, source, "file exceeds configured size limit"),
                            ensure_ascii=False,
                        ).encode()
                    ).hexdigest()[:24],
                    path=source,
                    revision=revision,
                    blob=hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
                    sha256=hashlib.sha256(raw).hexdigest(),
                    range_ids=sorted(r["id"] for r in rows),
                    lines=len(raw.decode().splitlines()),
                )
            )
        requirements = [r for r in inventory if r["kind"] == "acceptance"]
        self.assertEqual(len(requirements), 1)
        self.assertEqual((packet / requirements[0]["artifact"]).read_bytes(), b"fixture")
        requirement_ids = [r["id"] for r in requirements]
        linked = [r for r in inventory if r["kind"] in ("changed-source", "test")]
        self.assertEqual(
            {(r["path"], r["revision"]) for r in linked},
            {(source, "base"), (source, "head"), (deleted, "base"), (test, "head")},
        )
        self.assertTrue(all(r["links"] == requirement_ids for r in linked))
        mapping = json.loads((packet / "test-map.json").read_bytes())
        self.assertEqual(
            [(r["changed_path"], r["candidates"]) for r in mapping], [(deleted, [test]), (source, [test])]
        )
        # Assemble adjacency independently from the known source/test-to-criterion fixture.
        rows = [dict(id=r["id"], links=r["links"]) for r in linked] + [dict(id=requirement_ids[0], links=[])]
        public.add_relations(packet, rows)
        relation = json.loads((packet / "public-catalog-relations.json").read_bytes())
        ids = sorted([r["id"] for r in linked] + requirement_ids)
        self.assertEqual(relation["ids"], ids)
        self.assertEqual(
            relation["edges"], sorted([[ids.index(r["id"]), ids.index(requirement_ids[0])] for r in linked])
        )
        actual = json.loads(mapping_path.read_bytes())
        self.assertEqual(actual["profile"], public.PROFILE)
        self.assertEqual(sorted(actual["mappings"], key=lambda r: r["revision"]), expected)
        test_rows = [r for r in inventory if r["path"] == test and r["revision"] == "head"]
        self.assertEqual(
            {line for r in test_rows for line in range(r["start_line"], r["end_line"] + 1)}, {1, 2}
        )
        deleted_rows = [r for r in inventory if r["path"] == deleted and r["revision"] == "base"]
        self.assertEqual(
            {line for r in deleted_rows for line in range(r["start_line"], r["end_line"] + 1)}, {1}
        )


class HostedConsumerTests(unittest.TestCase):
    setUp = HostedPlanTests.setUp
    catalog = HostedPlanTests.catalog
    binding = HostedPlanTests.binding

    def test_actual_g21_consumer_dispatch_mutations(self):
        from unittest.mock import patch

        import review
        import review_report_material_v1 as material

        plan = public.plan_catalog(self.catalog())
        self.assertEqual(plan["catalog"]["binding"]["contract"], authority.G21_CONTRACT_DIGEST)
        public.validate_binding_g21(plan["catalog"]["binding"])
        applied = windows.application(plan, 100, 200, qualification_finished=150)
        names = [u["id"] for u in plan["catalog"]["components"]]
        import review_capacity_native_v1 as capacity

        raw = capacity.reports()[0][1]
        reports = {n: raw for n in names}
        dependencies = {
            n: {
                **dict.fromkeys(material.DEPENDENCY_FIELDS, "0" * 64),
                "review_sha256": hashlib.sha256(raw).hexdigest(),
            }
            for n in names
        }
        result = windows.integration_reports(plan, reports, dependencies)
        self.assertEqual(len(result["items"]), len(names))
        policy = {}
        funding = dict(
            name="fixture",
            plan_digest=digest(plan),
            policy_digest=digest(policy),
            funding=plan["schedule"],
            expires_at=200 + plan["schedule"]["wall_seconds"],
        )
        windows.finite_authorization(funding, plan, policy, 200)
        for field, bad in (
            ("schema_version", True),
            ("schema_version", 9),
            ("profile", True),
            ("profile", "unknown"),
            ("catalog_digest", "f" * 64),
        ):
            changed = copy.deepcopy(plan)
            changed[field] = bad
            (self.root / "windows-plan.json").write_text(json.dumps(changed))
            (self.root / "windows-application.json").write_text(json.dumps(applied))
            calls = [
                lambda changed=changed: windows.application(changed, 100, 200, qualification_finished=150),
                lambda changed=changed: windows.integration_reports(changed, reports, dependencies),
                lambda changed=changed: windows.finite_authorization(funding, changed, policy, 200),
                lambda: windows.load(self.root),
            ]
            for call in calls:
                with self.subTest(field=field, consumer=call), self.assertRaises(WorkflowError):
                    call()
            batch = dict(
                schema_version=9,
                binding={},
                contract_digest=authority.G21_CONTRACT_DIGEST,
                plan=changed,
                authorization=funding,
                unit_policy=policy,
                admission={},
                application=applied,
            )
            (self.root / "batch.json").write_text(json.dumps(batch))
            with patch.object(review, "verify_packet", return_value={}), self.assertRaises(WorkflowError):
                windows.replay_prefix(None, self.root, changed, 0)
            # Admission reaches the same stored-plan validator with an inert owned handle;
            # no credential function is run and no successful live admission is claimed.
            import reporting_admission_v6 as admission

            with (
                patch.object(admission.claude_owned_auth, "require", return_value=object()),
                patch.object(review, "verify_packet", return_value={}),
                self.assertRaises(WorkflowError),
            ):
                admission.check_batch(None, self.root, owned_auth=None)
            # Only catalog collection is replaced, by an actual validated fixture plan mutated above.
            with (
                patch.object(windows, "catalog", return_value={"plan": changed, "metadata": {}}),
                self.assertRaises(WorkflowError),
            ):
                windows.select_preparation(None, self.root / "absent", funding, owned_auth=None)
        (self.root / "windows-plan.json").write_text(json.dumps(plan))
        self.assertEqual(windows.load(self.root), (plan, applied))
        stale = copy.deepcopy(plan)
        stale["catalog"]["binding"]["source"] = "f" * 64
        stale["catalog_digest"] = digest(stale["catalog"])
        with (
            patch.object(
                windows, "catalog", return_value={"dependencies": {"assignments": plan["catalog_digest"]}}
            ),
            self.assertRaises(WorkflowError),
        ):
            windows.current_plan(None, stale)

    def test_independent_g21_authority_binding_refusals(self):
        catalog = self.catalog()
        for field, value in (
            ("contract", authority.G20_CONTRACT_DIGEST),
            (
                "authorization",
                digest(
                    dict(
                        contract_digest=authority.G20_CONTRACT_DIGEST,
                        approval_digest=authority.G20_APPROVAL_DIGEST,
                    )
                ),
            ),
            ("authorization", "f" * 64),
        ):
            changed = copy.deepcopy(catalog)
            changed["binding"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(WorkflowError):
                public.validate_catalog(changed)

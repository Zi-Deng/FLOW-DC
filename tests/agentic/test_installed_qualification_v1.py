"""Installed adoption uses real isolated fixtures; no provider or readiness mocks."""

import copy
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import install
import review_batch_windows_v1 as windows
from workflow import WorkflowError

ROOT = Path(__file__).resolve().parents[2]


class InstalledAdoptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="adoption-v1-test-")
        cls.base = Path(cls.temporary.name)
        cls.source = cls.base / "source"
        cls.source.mkdir()
        # Tiny source fixture: support files retain exact bytes under its runtime;
        # only the explicit load_tests suite is discovered. No original imports.
        for relative in install.payload(ROOT):
            target = cls.source / (
                Path("scripts/agentic") / relative.name
                if relative.parts[:2] == ("tests", "agentic")
                else relative
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        for relative in windows.ADOPTION_INTEGRATION:
            target = cls.source / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        tests = cls.source / "tests/agentic"
        tests.mkdir(parents=True)
        (tests / "test_selected.py").write_text(
            "import unittest\n"
            "import test_review_batch_providers as providers\n"
            "import test_suite_deadline_issue31 as deadline\n"
            "def load_tests(loader, tests, pattern):\n"
            " return unittest.TestSuite([providers.ProviderBatchTests('test_current_harness_binding_cannot_name_arbitrary_subset'),deadline.SuiteDeadlineTests('test_makefile_and_case_ci_are_closed')])\n"
        )
        env = windows.adoption_environment()
        template = cls.base / "source-template"
        template.mkdir()
        for argv in (
            ["git", "init", "-q", f"--template={template}", str(cls.source)],
            ["git", "-C", str(cls.source), "add", "."],
            [
                "git",
                "-C",
                str(cls.source),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "fixture source",
            ],
        ):
            subprocess.run(argv, env=env, check=True, capture_output=True, timeout=10)
        cls.directory = cls.base / "evidence"
        cls.directory.mkdir()
        cls.prep = windows.prepare_installed_adoption(cls.source, cls.directory)
        cls.execution = cls.directory / "installed-execution-root"
        start = time.monotonic()
        utc = datetime.datetime.now(datetime.UTC).isoformat()
        argv = [
            "/usr/bin/python3",
            "-B",
            str(cls.execution / "scripts/agentic/check.py"),
            "--jobs",
            "1",
            "--suite-profile",
            "issue31-suite1800-v1",
        ]
        child_env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        result = subprocess.run(argv, cwd=cls.execution, env=child_env, capture_output=True, timeout=30)
        if result.returncode:
            raise AssertionError(result.stdout.decode() + result.stderr.decode())
        cls.run_record = {
            "argv": argv,
            "cwd": str(cls.execution),
            "safe_environment": {"PYTHONPATH": None, "git_keys": []},
            "utc_start": utc,
            "utc_end": datetime.datetime.now(datetime.UTC).isoformat(),
            "elapsed_seconds": time.monotonic() - start,
            "cap_seconds": 1860,
            "exit_status": result.returncode,
        }
        original = Path(
            next(
                line for line in result.stdout.decode().splitlines() if line.startswith("Workflow evidence: ")
            ).split(": ", 1)[1]
        )
        cls.installed = cls.directory / "installed"
        cls.installed.mkdir()
        for f in original.iterdir():
            if f.is_file():
                shutil.copyfile(f, cls.installed / f.name)
        cls.request = json.loads((cls.installed / "request.json").read_text())
        with patch.dict(os.environ, child_env, clear=True):
            observed = windows.adoption_descriptor(cls.execution)
        import check_runner

        cls.receipt = {
            "schema_version": 1,
            "profile": windows.ADOPTION_PROFILE,
            "source_head": cls.prep["source_head"],
            "source_files": check_runner.source(cls.source),
            **{
                key: cls.prep[key]
                for key in (
                    "payload_files",
                    "integration_files",
                    "pristine_manifest",
                    "execution_worktree_manifest",
                    "git_manifest_before",
                    "fixture_commit",
                    "fixture_tree",
                )
            },
            "git_manifest_after": cls.prep["git_manifest_before"],
            "preparation": cls.prep,
            "execution": cls.run_record,
            "request_digest": hashlib.sha256((cls.installed / "request.json").read_bytes()).hexdigest(),
            "summary_digest": hashlib.sha256((cls.installed / "summary.json").read_bytes()).hexdigest(),
            "journal_digest": hashlib.sha256((cls.installed / "worker-0.jsonl").read_bytes()).hexdigest(),
            "origins": observed["origins"],
        }
        cls.write_receipt(cls.receipt)
        cls.original_runner = original
        with patch.dict(os.environ, child_env, clear=True):
            windows.validate_installed_adoption(cls.source, cls.directory, cls.request["rows"])

    @classmethod
    def tearDownClass(cls):
        # Retain full fixture evidence for this bounded qualification.
        cls.temporary._finalizer.detach()

    @classmethod
    def write_receipt(cls, value):
        (cls.directory / "installed-adoption.json").write_text(json.dumps(value))

    def validate(self):
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        with patch.dict(os.environ, env, clear=True):
            return windows.validate_installed_adoption(self.source, self.directory, self.request["rows"])

    def test_canonical_origin_refuses_before_object_construction(self):
        real = windows.adoption_objects
        for root in (self.directory / "installed-root", self.execution):
            path = root / ".agentic/template-origin.json"
            raw = path.read_bytes()
            mode = path.stat().st_mode & 0o7777
            variants = (
                (json.dumps(json.loads(raw), separators=(",", ":")).encode(), mode),
                (json.dumps(json.loads(raw), sort_keys=True, indent=2).encode() + b"\n", mode),
                (raw, 0o644),
            )
            for altered, altered_mode in variants:
                self.assertNotEqual((altered, altered_mode), (raw, mode))
                try:
                    path.write_bytes(altered)
                    path.chmod(altered_mode)
                    with patch.object(windows, "adoption_objects", wraps=real) as observed:
                        with self.assertRaisesRegex(WorkflowError, "canonical origin bytes or mode"):
                            self.validate()
                        observed.assert_not_called()
                finally:
                    path.write_bytes(raw)
                    path.chmod(mode)
        self.assertTrue(self.validate())

    def test_real_dual_root_original_cases_and_closed_records(self):
        self.assertEqual(len(self.request["rows"]), 2)
        self.assertNotEqual(self.prep["source_head"], self.prep["fixture_commit"])
        self.assertTrue(self.validate())
        self.assertEqual(self.request["execution_limits"]["seconds"], 1800)
        self.assertEqual(self.request["source"]["checkout"], self.prep["fixture_commit"])
        self.assertTrue(
            all(str(ROOT) not in row["path"] for row in self.receipt["origins"]["modules"].values())
        )

    def test_receipt_every_field_and_nested_execution_mutations(self):
        for key in self.receipt:
            bad = copy.deepcopy(self.receipt)
            bad.pop(key)
            self.write_receipt(bad)
            with self.subTest(missing=key), self.assertRaises(WorkflowError):
                self.validate()
        mutations = [
            ("schema_version", True),
            ("profile", "legacy"),
            ("source_head", "0" * 40),
            ("fixture_commit", "0" * 40),
            ("fixture_tree", "0" * 40),
            ("git_manifest_before", {}),
            ("git_manifest_after", {}),
            ("source_files", {}),
            ("payload_files", {}),
            ("integration_files", {}),
            ("pristine_manifest", {}),
            ("execution_worktree_manifest", {}),
            ("origins", {}),
            ("request_digest", "0" * 64),
            ("summary_digest", "0" * 64),
            ("journal_digest", "0" * 64),
        ]
        for key, value in mutations:
            bad = copy.deepcopy(self.receipt)
            bad[key] = value
            self.write_receipt(bad)
            with self.subTest(key=key), self.assertRaises(WorkflowError):
                self.validate()
        for key, value in (
            ("argv", []),
            ("cwd", str(ROOT)),
            ("exit_status", True),
            ("exit_status", 1),
            ("cap_seconds", 1800),
            ("elapsed_seconds", float("nan")),
            ("elapsed_seconds", -1),
            ("utc_end", "wrong"),
            ("safe_environment", {"PYTHONPATH": str(ROOT), "git_keys": []}),
        ):
            bad = copy.deepcopy(self.receipt)
            bad["execution"][key] = value
            self.write_receipt(bad)
            with self.subTest(execution=key), self.assertRaises(WorkflowError):
                self.validate()
        self.write_receipt(self.receipt)

    def test_fixture_tamper_private_missing_extra_index_objects(self):
        self.write_receipt(self.receipt)
        fixed = [
            self.execution / "Makefile",
            self.execution / ".git/config",
            self.execution / ".git/index",
            self.execution / ".git/HEAD",
            self.execution / ".git/refs/heads/installed-fixture",
            self.directory / "installed-root/.agentic/template-origin.json",
            self.installed / "worker-0.jsonl",
            self.installed / "summary.json",
        ]
        fixed.append(next(f for f in (self.execution / ".git/objects").rglob("*") if f.is_file()))
        for path in fixed:
            raw = path.read_bytes()
            original_mode = path.stat().st_mode & 0o7777
            try:
                path.chmod(original_mode | 0o200)
                path.write_bytes(raw + b"altered")
                path.chmod(original_mode)
                with self.subTest(path=path), self.assertRaises((WorkflowError, ValueError)):
                    self.validate()
            finally:
                path.chmod(original_mode | 0o200)
                path.write_bytes(raw)
                path.chmod(original_mode)
        for name in (
            "memory/private",
            ".git/objects/info/alternates",
            ".git/refs/heads/copied",
            ".git/hooks/pre-commit",
            "untracked",
        ):
            path = self.execution / name
            parents = []
            ancestor = path.parent
            while not ancestor.exists():
                parents.append(ancestor)
                ancestor = ancestor.parent
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("forbidden")
            try:
                with self.subTest(extra=name), self.assertRaises(WorkflowError):
                    self.validate()
            finally:
                path.unlink()
                for parent in parents:
                    parent.rmdir()
        path = self.execution / "Makefile"
        raw = path.read_bytes()
        path.unlink()
        try:
            with self.assertRaises(WorkflowError):
                self.validate()
            path.symlink_to(ROOT / "Makefile")
            with self.assertRaises(WorkflowError):
                self.validate()
            path.unlink()
        finally:
            path.write_bytes(raw)
            path.chmod(0o644)

    def test_source_rows_preparation_and_copy_refuse(self):
        self.write_receipt(self.receipt)
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        for rows in ([], self.request["rows"] * 2, list(reversed(self.request["rows"]))):
            with patch.dict(os.environ, env, clear=True), self.assertRaises(WorkflowError):
                windows.validate_installed_adoption(self.source, self.directory, rows)
        path = self.source / "Makefile"
        raw = path.read_bytes()
        try:
            path.write_bytes(raw + b"changed")
            with self.assertRaises(WorkflowError):
                self.validate()
        finally:
            path.write_bytes(raw)
        path = self.directory / "adoption-preparation.json"
        raw = path.read_bytes()
        for key, value in (
            ("commands", []),
            ("exit_status", True),
            ("elapsed_seconds", float("inf")),
            ("cap_seconds", 181),
            ("argv", []),
            ("safe_environment", {}),
            ("utc_end", "wrong"),
        ):
            bad = copy.deepcopy(self.receipt)
            bad["preparation"][key] = value
            path.write_text(json.dumps(bad["preparation"]))
            self.write_receipt(bad)
            with self.subTest(preparation=key), self.assertRaises(WorkflowError):
                self.validate()
        path.write_bytes(raw)
        self.write_receipt(self.receipt)
        bad = copy.deepcopy(self.receipt)
        bad["extra"] = True
        self.write_receipt(bad)
        with self.assertRaises(WorkflowError):
            self.validate()
        self.write_receipt(self.receipt)

    def test_guard_and_object_encoding_refusals(self):
        for name in ("GIT_ASKPASS", "GIT_PAGER", "GIT_DIR", "GIT_CONFIG_COUNT", "GIT_OBJECT_DIRECTORY"):
            with patch.dict(os.environ, {name: "untrusted"}), self.assertRaises(WorkflowError):
                windows.adoption_environment()
        for name in ("../escape", "/absolute", ".git/config", "a//b", "a/./b"):
            with self.subTest(name=name), self.assertRaises(WorkflowError):
                windows.adoption_objects({name: (b"x", 0o644)})
        for mode in (True, 0o120000, 0o777):
            with self.subTest(mode=mode), self.assertRaises(WorkflowError):
                windows.adoption_objects({"a": (b"x", mode)})
        with self.assertRaises(WorkflowError):
            windows.adoption_objects({"a": (b"x", 0o644), "a/b": (b"y", 0o644)})


PUBLIC_U = "c$}qL+j87Ua((AlltYeKDW=iYX!Lcsb_AECMvRq6nwMFx9gdD_pwPq!jp{`KBzvCrAM7vOFY(E&Dgc!9;0RC5Xvl7$sxtHB$&*=nC$6k*brK6HZts$*_{ZP>M?C&44yJC@w$$Rq)L(TSrB6kfi6>Q8+FDWU>3e%}^09c;R=i8%lat%Ko7<cF%L{RH{rRtBaZ?w&qEe+evrVmp#cPGBCb636d>Kdc<wQrznVv-3<#rlP6TMidb+VWzi;<}HuCR@+btVdbIyy#EYc036s?w*Cu#KvlqS}eH)T)XX@qD$4v+Xw9P8QQ?u4dE4Y`tEnc&@W)8n5wnwpxzGxiNKygEgiW`_?w%&~D4ZK8fb3u;S&(z-G1ARfhi>d{~=yM_+2uVqaD5bhen_pJ;n*wAi((&PL){*F}C5Ds5U=Y+fo|OFp?w&7KyLTaz8-UXd?_sxpyjTkLQO%}eW7yecaF+6Z+x)aDthY;|sGO|C9AEuk9tZ!E5jD5@O3sZuRq10O$)be?N0anSX?U{8zOl%;vG&MvHZFjkf0laqJv#A7@b6gjxJdcx5gMT@y(?M+1iYw%^Rin6Ws$;nlfni>(O<DTDqx&3_kaOpP|4{{gJ#GzD`_|L^;5hv40yq^5m`=_Qk*q^1`70pw-9m5auKZ@u=S3CLf^UYtPi*x58E0g#g|Kwd2^{ads#qlaxK0bV0k+<#0$LIdj*>t`TX*!)w^?Z`fb-tL#agrqYR4*pe=^~q~CrP@>=6SNv$z-17ah%TAdW~flK*=PFlXxUjQ^8jFE!*mNIh`+)S(eY#JXufmHpPcatemgbt8|I2W|MTCtTh6&TFsZM$#RwI>0HkyYIVxqD;g_u_G{;*U7W!AI@2i<0O5j<X7gDxOOomT&qraE52N{dy`Hguq<73(<*;G4n&rTMx>_Z(rCx8B`5Mq&tkY~ZO|lH;<=J+X!!7IOdOAs_i%GgzPbTYG@0iI{FIP&(2|T)3#Bkkoy-eZlg-YPDe331*o*{LoSY(pngVkcKvRtpHt8B3b&d$m*pbUQbH+d&-k+^@7_wqp)AZW+9w!C9ewUi5~J~ZO-4;DWj0I{M8B&4h7qBa$0rZsJyhNlVd6adbk5<AsMAk{Wc2HR5J;gf=LipfLU0X-ox3qn@NJ4^J#?8Ynl9I*IsuZpTCfm2Z#M7DUQq^Q{l1!!(5M0mX092s}}q^w@7g$Xo;=d1N{DomQTHD|INt(UQ|ZAvgfAWfxpU7LC&@)i#KH4(qTUhE(dmZH@7!jX-BEt+_Qd`ZTW<$O6?<D_jx{KKCm%f%#)tuSpv@8HOpR#{n8;2n5ir|YqJ7;>BVOHpO!<w4uVjsdA=<M!FORX6?^b_3^ZayaH`1D|X9Y?Brnm8k=M6SPATPX#|A4o5hnic;aK2rRJJ>5IwQQeT_qqiL(`lFa=XPw4ck->joR!~$ifVZH+ImnDAkj}%qWAhN~pIvb1Ix<HBnMn}{e3b;I|njSAHEgQs<UbD{6$VqzTuMeMYu5ZsCK8Y8Fux%~Cbt<yLrUtD3{)2FifeWkhNIXgrib|6LJN9VD2h}`{#pNps7FDCL4_Br3K(N6&jv5+GWmDBhL6z)OFjM25?`;E!>daD&JBFvg*n|Yz0^N+t072yzfniOt*HK<H^2yMG(l3bR6e=Fo?G#K@G%hJo7El8V*s+>TzQcL=-4~4MSbTx2_b|Fs21pZCxN4S`h9U}|9k^WJr@A;aR;nG`S)_6p*74y;r+xnc7;b#|^WJlLE^e_*C6MjJ3aCbHO6i4~ubW}Oa7=m|n-<vIVQWYcjd6t)al<0-ZxN~Yn@tYaZ8jgqFwT_E`uzul5INY;aY|F|Y?0}=^*$Wy;mhsC)!kS;TK9m3PVi}nE%d8S+h!07fCzzvk0A`~TRQ<WQz9k9%B7;s#^S;tq^L#!@3Z!m8>dDZnLV#S87XRG8XOjnJ?j##s$JF~$3fhLpnB+rk$VYN;Z)6WB=#o#3T!mBu?NkbB^}WbRzP2O8Ze`-vH^+rAs7ij8U*WTtciMy@mpO`*3@qM@J&JF`)92Wf{<xoQ$1LD7`?pEC30X$xUiQ*TSJs<?ASleDu_eaC8TqBVLe92;@(vYd)$MWzRHl^M+qIqP{kUo<+=o5<bWTTy8b|hkZOcw=}RVU0}kDK8H{+B#Q4a1au>D)v~U{OLyQ8LRN2<N^7;xNYvj}e9S)8y$w$Cxsdp+paxR%e6JdXV$fE-eyK2N<eZ`}WPaV_|LS17o{Aq&^J<I|UgI;$ebrVeRwUfE91(^Ns-z4p6(6gv=1X~^T40Znbs(-!z_?s0*K4hFMZjXReS9bcyGimn=1scsX{H#fMBEvGu3i2pFOj#gapqzee^C>=7O}7xGwkG<1Mj}{WyV)wPu)(yBAtAGw<0Gbh2oVZ2KyrTNKB3U4nhMw0*#uS_i7yBe@e2p?{0h4g_fkr!)OQ$LdWVAG^a^anSyGefP-uV-&W~R&@9wW|uBY<;^8Dud;vOtL3gCcKN#iFnUQH%qfMHs+W0_P{rXqsJ{(pmtY$T$AUgQa9f<W*}LfUq}rvr6(@`|j>Jc<=gj(=ScBeubl)*DfFDBJxOTaO%Zz`hqxCum1%$S`st>4l106q5@gN*s&WiCU19)EspqWAP`$#7B7dc>oqKxSVxW!S@5+Jg3;m0Jz8<RZ?}_@!_P0f&}_Vqq(plxjuD;ke1d2U{`tHSj%NjRb&@bVv!LCh5A6@W5`&nNus7ksX_tX0U!qn>*3|lM`JNiGB<w^bFjXaK~(VLK{<>_vmM%|cTTDf<UV!@XZ2Rq;L~~loEw5>#l6NL&;9pr#T8ZI+_>DNO4~c?)C*w5k;ES<Tm)<LykLm;-%%WhLpM%I9NiN>2G50l*=$vlK5brDQQFOO9QL%DbQ$M#g&LX#qWLAB|3JO_<+~!YBiCD9y4rObHan>Ft8T&;t_^$miTW>sM?u4cVIvlLg0VPv>x%o!hsWFT-i7++_VVuR;p+OA=$FT<i%a@=4@mf0NKWe-^G+Z{TzWFjzSMQ=XIaMe&&$V7?@-T43sDA;$Yxo$3EF()ryjwb{7CQkzrj~U4#4(tA?=HzQ$sZn;~skwpm(J!RFn+5rk6r42p(eKv}K#IE<t*^Ze=M{1|lRPV41l`n-sR}PVGJaCcsWyy24IWh<*zAZV)V;&F8Yh3UtU<Dw38}U0{9mCz&Sq0`1(A*{?`<+q0@XzmkD#zbVs?===`6#{m8v)dD{U%dlYXNQ<&?(-bFe6P(!r&`n@ECB!*zD9-Rw0L;nBSxzlZD2_Y^;pJo)>abZTf!g(jK(&XU{T}*+`KHAdByY<+>PHd<a<*{|U^f(UA9|Xnr<h!H7SAG5Tt~0!@qcBx&n2zO(BlzR(dU}aGZ)=MsZtFp#adSGTIxZ;*eyc@+qT+<p<E3L$c{LAk-kYMoSBM}Dmw|vMoF%Jd8u|xUHE2;G##LIhb9r_?cwh1{9(k?lIzQV=bvV9&CE3bP)#_%hN2V52EXEw@Ebd%Z)hmsH8hCWgGzyG80iz&8&9iuJ`FvB;R?jD?Mfd{Z>Xspj0bjcEOBf}hvj#uxOWipEX8r9SmMkQ@PPi<NtOBGoMg?xSnin&9{vqKfhr*Gb|e+8><SVwmTLs%)|gVZ)z`|rR1$^)ckO{Oh>}xlkBA<|r$JrTT=zSL+t+(^b7(KQ)P}P89sANJ5`7*{><VXCCLlWK7BX$)AK?i~|2u_j7^bt^E9<JMV@g%^BcG1#;m4Pn!dLq&VxoEj;$Z9Vs+$D)!2=p$?`8rL4V9}D*lD?Ce%s^)P37>w!s?qLf_&4VQ(S@+2$NZ-_5$1y$H<R#z^sL`jvE6)sE*muK=dBeu{0`!V4&HQ48tY;3VOD|FxT6IT5Fi58k#9I%*H>laA0vKx1ez8d=v!tH&F{U5mEbS>R|zPb(9mH9~tBa_1ZP{arE0uH5ku(q>uUcjh|B5f%X%ZXXl@MRrZtP0e$o%U$U_uZjPM5cT~4+i?Z>!-2n|(7bBY99RcrZ2XGSQxiKsqLOkAmrlEz)86V_6Q{-NiBuP1zIl34G=Zk0f;&+$RVPD#V5}oH~u(D=hONDww9bQNPPw0Wbqai)gT}7@axx<Y@SD?8{@iAOPqHbE5J|W9_^uw^B0qL$at)=lH%7DDOu+s57H=ldhu9yvh99Hy$`RZ`E5diq;?<joEvrc(K%`^19U^&<EZ1t0RE<%r?>XM`<7dgZ-d>LrOGaihhsAXPaXlj2*)zc(v;2vsU2boAE7hr>AM|$riX%xvLODJH+DGY(yi;>>QQ^E2Y<Y`{84GA_ETux3C$k!a)w3cr>xNwISh-#FJ22KaCdVw&b>ynBFZ+*V=IY?M=`yWKX8wJ1}ryERoi7)>6FUiEZ&KYFqBo<A2sG;>aNUgP-@-g<^MDop?5)|MkL3{~D9q|w6tWNcDCpZ-T;xb4jDieGM=7vN0(aqEN<sVFEU03HPgEY19^ACE-+sUW$9ryrBVFK>wU?vRTy~&UkUJbGufzs6~^meTF%@7_AuH5bT`A+wt^LcL4)^ZGk)8qLB8IafJcU?ggz)KlttCgzWJ%>57YiNJ=;{{DR!X+YW@D~&LX<Z6q>@YggO~Kl#)+OJZ*_KB;!%^9(_v@bhD#9F6_%S3;fyg)iw|Qcf1&<HgZ^!L6h~kUF(xso9)zUQMy9qik%Z9-1Au3T<GPs{{m!uFs`pxvoJ<aiQnqK<hO3ht7h5yPW{Wo=%@IzJ&E*e5eZm!QS1?6Au-2Y}eI&$3Mv<#$nuG+Gp(RDvG<DyN6w!^yd>s0=lgsCRAi=}>sxEc};O&p-InG3kE`TK5Kn)#WGYfNfN7knpIs9Jt6l&I>ndUM?4;X`MCQxV+R!c<ne`#S_C^5h*kl6T`yH;-Y_;^$Gmc!awb(%+TkCND_huEld*5D#9jY3v062E7^X1!yG9g{I5cE=3*BxIqF-%TupdDU?3ZJXEn{Vg2NXDV}nds1s5J-tIcIz#!a~g^WiUOP~0qF^92uFu>_|xlAPhVYWO&MKXu#>GU~%xNGyAuAds|CQc)9cK<*t8(VOE(8uQT;Oulv?v&9fr3Rg_Z~5p^x(nU!NsyW+%^BxV<L^BnxyIs-M^3Z#w0E1U%cJgt6_oGbx=$|GYX^G#IHf+tw#>%N4qUBEDi2*HqR$*;P*xpnNt#5n+_Ngz-~k#ZQMV)MQqcMW1rt+L!h?%G4dghYTqvDx@cmvGA1QHvd3$zu_Ha3v3HiP#9sK19*HHS2KoH;n?9KHhZ4)`SA>L7URZGYBluA{ObCgRRCobnP-BZS^#biXRQ4QN=UrpV0o~sJ{@Om{F{b)pe_W5(<>Zse2_vqGln;&YoJ~ipI%+NkUFe5{9l-;Y-<phs~gbwxVhnoC1vxUzuNk`(_Lq7uW4W)b4_XnH>Jt3hfb7~}9AAYONy29z^y5UwfNV)!gqYt6=@CfryJDB_UjFjO<vbzGh>fG7qI2VL%Q4MPe;c?U_j_+h#NI3(n6Ayl*%7w-MB_e+v#M5?pdlwO+33(=ULz-@&=5E2>x;&IBiDD+b=N-*q1?oFA_}aZm_u)hvq@}INn-^f8FyTaSM5#}BKEImHr7JQhE}_nxU&V9jsWWy^1-?S%EAa<CoM5#z{sXE?b;RP5t`msiRf+F@T1+HY1{nr@a-R6!R{rxmp2;ujmHP`c#uJd(zG%Mxiw9jE)2?n#AZg^%{e_=_1|<juTLXqZGCzeLz`4B(B#+p<cIW<-QY3{9!%Xz3)5EI<Q@LrODyNfw{QbYDiFh-mJRQF8C3c4vK*`VjnqK?cp9UgdI_`KgYh!WaCX}u*ZqWdNN$Z^sKI=w-s3v5*L`qWnWL)}exDKM+vuVK+xJV@n5E?pBYDl-BzZN<<xifBr#kV1({#EAM2}dYtsfxYrX7*h%hAl8)B;F3c-Tah~gJku$Sx#iJ^EuQRsx4bgxeuUgav!UV;gu~<yaso<X3xY=0*{m&@&_hcQq6z-rzjHsf-a)lV}9-Dbg|qn^>!UC7J3!U;`KCIFDBDyvCY!uJXvN*nusX+*OUJRF3p0z"


class Generation16PredecessorTests(unittest.TestCase):
    def test_both_fixed_predecessors_and_primary_ranges(self):
        import base64
        import tempfile
        import zlib

        import review_packet
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        bodies = [zlib.decompress(base64.b85decode(v)) for v in (PUBLIC_U, PUBLIC_T, PUBLIC_G13)]
        numbers = [6062530466, 6061320190, 6045434332]
        rows = [
            {
                "id": n,
                "issue_url": "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
                "user": {"login": "Zi-Deng"},
                "body": raw.decode(),
            }
            for n, raw in zip(numbers, bodies, strict=True)
        ]
        context = {
            "designated_plan_comment": {"id": 6064513854, "body": "U"},
            "issue": {"title": "31", "body": "acceptance"},
            "issue_comments": rows,
            "reviews": [],
            "inline_comments": [],
            "pr_comments": [],
            "pull_request": {"head": {"sha": "a" * 40}},
            "commit_statuses": [],
            "check_runs": [],
        }
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", root=Path.cwd())
        self.assertEqual(
            review_packet.g16_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(3):
            for key, value in (
                ("id", True),
                ("id", float(numbers[index])),
                ("body", None),
                ("body", rows[index]["body"] + "\n"),
                ("user", None),
                ("user", {"login": "wrong"}),
                ("issue_url", "wrong"),
            ):
                bad = copy.deepcopy(context)
                bad["issue_comments"][index][key] = value
                with self.assertRaises(WorkflowError):
                    review_packet.g16_predecessors(repo, bad)
            for duplicate in (False, True):
                bad = copy.deepcopy(context)
                if duplicate:
                    bad["issue_comments"].append(copy.deepcopy(rows[index]))
                else:
                    bad["issue_comments"].pop(index)
                with self.assertRaises(WorkflowError):
                    review_packet.g16_predecessors(repo, bad)
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
                (packet / name).write_text("policy\n")
            with patch.object(review_packet, "run", return_value=SimpleNamespace(stdout="")):
                review_packet.build(
                    repo,
                    packet,
                    "a" * 40,
                    "b" * 40,
                    [],
                    [],
                    context,
                    {"required_checks": [], "max_snapshot_bytes": 1000000},
                )
            inventory = json.loads((packet / "required-material.json").read_text())["required"]
            for number, raw in zip(numbers, bodies, strict=True):
                name = f"contract-predecessor-{number}.txt"
                self.assertEqual((packet / name).read_bytes(), raw)
                entries = [v for v in inventory if v["path"] == name]
                self.assertTrue(entries)
                self.assertTrue(all(v["kind"] == "contract" and not v.get("omitted") for v in entries))
                covered = [i for v in entries for i in range(v["start_line"], v["end_line"] + 1)]
                self.assertEqual(covered, list(range(1, len(raw.decode().splitlines()) + 1)))
                self.assertEqual(len({v["id"] for v in entries}), len(entries))

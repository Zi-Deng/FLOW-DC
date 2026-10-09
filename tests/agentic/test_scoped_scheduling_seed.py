"""Frozen measured policy, complete tiny execution and independent descriptor parity."""

import base64
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

import check_runner as runner
import review_batch_windows_v1 as windows

SOURCE = Path(__file__).resolve().parents[2]
PROFILE = "issue31-suite1800-v1"
SEED_DIGEST = "dd2f9ec560213889e3c1a27b6096c4a648b1032d44a60177ce7036e2ad4ea5ce"
FROZEN_WINDOW = "c$~#tZEqX75&kQIKL-a(b|Sgo4n+<qP@w6xKtB`*g5pvV&s=iD7fZt7{(EQmvM-L4QcEcUf;f)J^*%eCnP+C6)gO08AMD*<-u?HFJ0_{cf8IH69Wnex@B=vr)xZk!viRw*)KRH-pN^e2?%%M+-o1Z!Cz`Xi{1jGF<~)^TCmdmM3E^RlCH#e(6WM658bXZ|TUbjde$zC(A%?0Q$KTuP#!LR~-;HZJZG{tBA-?<f@9@uWf4+ZL09Ci0R$RHyej^3kB3z^p@wawPfICCkGoZ2B8^#ISBPv3-jvLYq8{zs}Z@Ha`cX4~X41Ua=P-tH!y%jH<2<2csu}*iEZ1`3iPT<bne3Xk@qx8lgB-*sXS~MIUSfLQT<c<^Ll@l#b_Ph+}<&8mag+dpWdIMkJE<{VDwl<yOWaJJm3tw#|)`uHog@wH1@E1VZaYq_q>CSN82CIXu4@ScoH{5xnz^_FUj`w`Pk2eP#%(sCJ=)4fNv<X0&)a;qG-h~}Do#^>MKinFqwH_Q&H>Vthg(XTmvKNqV6xp0Gh6Ott&a`4eig1njkU!oSGFT8iw$H86X~DGFsMR)I-5enNbua=0FSoGL@?bn*78Wk!_S%EjdbdxDG={4!hY-#xP_J){y47%OwGxaNE#(FXMzktsByi#6(Y3aHx-m*fDEugFpcE`G1rn*yXIpB&cy4F|TnfAxF04EFw$5D_x8_iWw;E3A5OdM2jqk!z_<!oyzS^#fTXU#{5}kC<X#;$5Pd)q;yi@D~)rhGlB%x^T<kJ<PZ_S}Bg9F<1878vEfocFau}!-wP~Mt7r78!)tXI!U9~ALGpE4W1R1I)z$^;Ba<tQ5tsqO^WwvRyh)K<d0HDU6~a0zh-0Rq^gd&QeVod=bA1;j#v^b1A>xZ4(<b{Vv<c0@@0h_O_JY88J3F-OHRb(c(s_KfvTyxL4~%i@AO-03If%&F6GBWJ*SwJ9RpaRs4-#omI&Y+<#0;Yi<ikW7L4H0gS1*F9&?K#^vGd^iu}--Y5349-9}-Z3sk<GS2pnbzVt@MKmP631yK!pHLv{wI8Y2EfsVN!Mrs(;2GLUCl%B;S3bUx_DDR=cif<=NmhxxFA0eIB?*8;YfCpo`|I_kLNbJVCL<OpgRR26i)Lr0L^?1ALl%V&yc#r@$6?X+Lqg6AxETI6Ax<0BB`mukDU4XifnaDgnI}tz~HdrWP2fjxB|J8LiPCqLXm@76f-680}Ee)y7vt=DfNNQFUwZNy}>I$$1I7IhjHVmP_G)hugcLb?8d`Sg~3Mw|KfCR3|<wt2d{v;C+r*D2ykfHG$Ix*4VQc<VYF%B`A}ef{0Na?KGZ^fdj+V;YJD$7AZ-`!6mw1>xfg1ct%`esSAZGMBDiM4`*o3)9X9%SKGc%J>EF*sT*!5=fEc^CSea!`5el<`4??ku9t{<#oy~Mt#ht(_;ByBJJ91D4)AiI4p{5iJ2!oH#2Uyq%yaGV45+F<{%;DS#f@<gtVrL8va>E(77XEXCeFbb69n<-pkGHS~_zNVx{iwY9^n!tdm6G7^?M{Rkmyj<&uKNDLG&W&wLNjaQ`Uo`>_PUr}{B7`{p9AwdlTH4g{W(^_sebuLWULa9BwF9biIgT#1MrC}&jkH62lQvm6IA{Rk@IZ8ulHt(5t5Mpoihy2nOBW^Nb&^N{o;k0Uy+xO%}4ipPSNxBwn>RFOj3E235=t0bt$wcpOjw#;uuO`LL^&{1^R^A2PsL1k^x6HC&x3FsCC^%`Rx44xCfRzp0)*sY>;SZB~PQ&WbuhqfG#A*R|cKP#n9ItmAeXcFjyS%+0DZ8x%m}v=CM->PLVWG9E+p%M$(8V7l?70(aw1@dx}v$H@^a4Ll1p$V%jTg2p_0v#!n!-Fp8ThKo-*Cf8aCiySZfu-`F#W!^uvg!CSD{WNMMvBOYj_>+0A=dB^mkXk$mI8A{nHl>*W@a@D?(q4<H$j!MhoP<&aWu>;jnGn%79><xOJ*npxN8K#Wfh$?u6LHDBYV#^fERw;J-eiYS99VQ|)MMIrXRkb3}!l-*upkoK@+Ywp5GaVg=A_&U!&b!&l!ew!&y)a5Rg~oSCc9XRrykZ^KC(LL)j&ozOr5h85<hC}oEN>Haa~7Sb?D{tG+p5B9Da*3BF>z6>db<tvOCHW7E*$ypM70yTqo@|BKv>wBxG2IDe1f@RRARhAhL4~IaovdeSzH9Mxr*d3i~AB6Me7y(GO<%bb_v?;1aHIRv}d3aVAY(3pjZ_(l%Y_hbYZx0!kEHRRybfyV-Xlu?>$|L`hd`wXrfow(1t>W^m#Z+zk)9ncAMVZmAVgnN7)Ig)h+eX#pIHl7Bxvv#NY-?8XX7{Feo}Z;gnC6FAfvfta{2_6q}haI4&ISK^}nIpU?&jJYSLYP|N{Gw|h3e3D3oLy`yc%%|tCYO*qJnbZCkjJk{1SOu2JWmXFUbg|dGMp--Iwm<`B!Kg>BM)GsV(yDK36SVNBKUzp5ns14BYO%oT&wm0?L6#xtA{KWuH)i}9Ai$m333UG<R41z19@D~CXde);%pO2^1V9C(`%bC=AW3YP=MfsIpSjgZn1TttH<I56z4He}!!PrtEGk5JpI~Ax3dHRJ=d6ilNK|#g1=%V^>>t!$1L4=s~C8sYKN$WEHV=+DdXTGyJMCW^(zgF*9sfWf77!hCOJLbqUY8cH0T`pGoSDf6=8Zdol@5oUHWm#@U_idu7k_irY-`Y$$Q_toG<vOK=qBq$&9nSCU%|=I>D|xeP(@T7T67-4yVTXj8&i!1(<+`OSBc4p4dQE_ZvoJgQmJ5@vifz=z%qm`GMZ?Fs2fN21agJ?u?-`WqlCBCnt2>6i7(aHxB-xINvilIwLrb?FbRnJiA)rU?c_?SVQS%5!_DYFl{qRuc)2MRE(p7Pz)Ft_F-iAz{^j3}my0U#=u2wn++_!>}@d^q3?~raF?ZnDEYCeDja}(Z{5w+E5xv--OQkaHW;!Yi;%uEWSBjR@<cW$@hSOKFj)K<aBpJQP7#zRqUBUivrIEbDWUv;s-0Eagxv`Ia|3T%b(v<_Py*0%I0THgYf-*dd%m>^mzC7i|yt*<~-7%VFhsVrF>KA3=ZT`$lL@7Tr0R;nhG_^|?5VKl4))_4=<bshELt=ACX167H$pQMVaq-N_M6YWN($&H(_jdG*hSJe9J;%cf2Nhk+}<82=&o;mM=jHQnTn3Y77hMT5NB(N?Xa8#kf=#~e=vAb`{p4xp<Elso^VdhbRr*MQ(g(r6K`)C(^Xy|GuZ;Aw}0!ra{q6$iG<PPr%$B(FpoxptL0&k$LkTq~h=MPnQ@_416q(t4q*R8;+QQ5luMd&ItF;z|#<xx8Hrt~j-MpFeO;0V9LkJ9`RJru98dc0pfEZ6rNzNE{=Q?usl2g=!Uwf?xcUu~9)hll3zfvwo$ae2RdYG5f{zz>(~L-V*~_jiB(4-;5hO#"


class ScopedSeedTests(unittest.TestCase):
    def fixture(self):
        value = json.loads(zlib.decompress(base64.b85decode(FROZEN_WINDOW)))
        root = SOURCE
        name = "test_review_windows_v1"
        module = types.ModuleType(name)
        module.__file__ = str(root / "tests/agentic/test_review_windows_v1.py")
        objects = [type("Inert", (), {"__module__": name})() for _ in value["rows"]]
        identity = {"files": {"tests/agentic/test_review_windows_v1.py": value["source_hash"]}}
        return root, [None], value["rows"], objects, identity, module

    def test_measured_policy_and_legacy_fallback(self):
        root, suite, rows, objects, identity, module = self.fixture()
        with patch.dict(sys.modules, {module.__name__: module}):
            legacy, _ = runner.assignment_policy(root, suite, rows, objects, identity, 2)
            scoped, assignments = runner.assignment_policy(
                root, suite, rows, objects, identity, 2, suite_profile=PROFILE
            )
        self.assertEqual(legacy["groups"][0]["weight_ms"], 82000)
        self.assertEqual(scoped["groups"][0]["weight_ms"], 622106)
        self.assertEqual(scoped["groups"][0]["reason"], "matched")
        self.assertEqual(scoped["seed_digest"], SEED_DIGEST)
        self.assertEqual(assignments, [[0], []])
        self.assertEqual(set(scoped), set(legacy))
        self.assertNotEqual(scoped["provenance"], legacy["provenance"])

    def test_closed_seed_and_provenance_mutations(self):
        seed = copy.deepcopy(runner._SCOPED_SCHEDULING_SEED)
        self.assertEqual(len(runner.scoped_scheduling_seed()), 80)
        self.assertEqual(len(runner.canonical(seed)), 8858)
        self.assertEqual(runner.digest(seed), SEED_DIGEST)
        changes = []
        for key in seed:
            bad = copy.deepcopy(seed)
            del bad[key]
            changes.append(bad)
        for key, values in {
            "schema_version": [True, 1.0, 2],
            "algorithm": ["wrong"],
            "source_head": ["0" * 40, True],
            "source_map_sha256": ["0" * 64],
            "provenance": [{}, {**seed["provenance"], "request.json": "0" * 64}],
            "entries": [seed["entries"][:-1], seed["entries"] + [seed["entries"][0]]],
            "extra": [1],
        }.items():
            for value in values:
                bad = copy.deepcopy(seed)
                bad[key] = value
                changes.append(bad)
        for key, values in {
            "weight_ms": [True, 1.0, 0, -1, 840001, float("nan"), float("inf"), 1],
            "identity_sha256": [True, "x" * 64, seed["entries"][1]["identity_sha256"]],
            "extra": [1],
        }.items():
            for value in values:
                bad = copy.deepcopy(seed)
                bad["entries"][0][key] = value
                changes.append(bad)
        for bad in changes:
            with (
                self.subTest(seed=bad.get("schema_version")),
                patch.object(runner, "_SCOPED_SCHEDULING_SEED", bad),
            ):
                with self.assertRaises(runner.RunnerError):
                    runner.scoped_scheduling_seed()
                with self.assertRaises(runner.RunnerError):
                    runner.assignment_policy(SOURCE, [None], [], [], {"files": {}}, 1, suite_profile=PROFILE)
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}'):
            with self.assertRaises(runner.RunnerError):
                runner.strict(raw)

    def test_fresh_rows_source_origins_and_unknown_profiles(self):
        root, suite, rows, objects, identity, module = self.fixture()
        with patch.dict(sys.modules, {module.__name__: module}):
            for profile in (True, False, 19, "legacy", "unknown"):
                with self.assertRaises(runner.RunnerError):
                    runner.assignment_policy(root, suite, rows, objects, identity, 1, suite_profile=profile)
            for changed_rows, changed_source in (
                (rows[:-1], identity),
                (rows, {"files": {"tests/agentic/test_review_windows_v1.py": "0" * 64}}),
            ):
                policy, _ = runner.assignment_policy(
                    root,
                    suite,
                    changed_rows,
                    objects[: len(changed_rows)],
                    changed_source,
                    1,
                    suite_profile=PROFILE,
                )
                self.assertEqual(policy["groups"][0]["reason"], "unmatched")
                self.assertEqual(policy["groups"][0]["weight_ms"], 1000 * len(changed_rows))
            module.__file__ = "/outside/test_review_windows_v1.py"
            policy, _ = runner.assignment_policy(
                root, suite, rows, objects, identity, 1, suite_profile=PROFILE
            )
            self.assertEqual(policy["groups"][0]["reason"], "unsupported-origin")
            empty, _ = runner.assignment_policy(root, suite, [], [], identity, 1, suite_profile=PROFILE)
            self.assertEqual(empty["groups"][0]["reason"], "empty")
            self.assertEqual(empty["groups"][0]["weight_ms"], 0)

    def test_real_tiny_workers_and_fresh_installed_descriptor_parity(self):
        with tempfile.TemporaryDirectory(prefix="scoped-seed-") as tmp:
            root = Path(tmp)
            (root / "scripts/agentic").mkdir(parents=True)
            (root / "tests/agentic").mkdir(parents=True)
            (root / ".agentic").mkdir()
            shutil.copyfile(SOURCE / ".agentic/config.json", root / ".agentic/config.json")
            for name in ("check.py", "check_runner.py"):
                shutil.copyfile(SOURCE / "scripts/agentic" / name, root / "scripts/agentic" / name)
            (root / "tests/agentic/test_scoped_tiny.py").write_text(
                "import unittest\nfrom pathlib import Path\ndef note(s):\n with Path('trace').open('a') as f: f.write(s+'\\n')\n"
                "def setUpModule(): note('module-start')\ndef tearDownModule(): note('module-stop')\n"
                "class A(unittest.TestCase):\n @classmethod\n def setUpClass(cls): note('class-start')\n"
                " @classmethod\n def tearDownClass(cls): note('class-stop')\n def test_ok(self): note('case')\n"
                "def load_tests(loader, tests, pattern): return unittest.TestSuite([tests, loader.loadTestsFromTestCase(A)])\n"
            )
            env = os.environ.copy()
            # The normal executing worker supplies its own runtime import path.
            env.pop("PYTHONPATH", None)
            env["TMPDIR"] = str(root)
            observed = []
            for profile in (None, PROFILE):
                for jobs in (1, 2):
                    argv = [sys.executable, "-B", str(root / "scripts/agentic/check.py"), "--jobs", str(jobs)]
                    if profile is not None:
                        argv += ["--suite-profile", profile]
                    result = subprocess.run(argv, cwd=root, env=env, capture_output=True, timeout=25)
                    self.assertEqual(result.returncode, 0, result.stdout.decode() + result.stderr.decode())
                    line = next(
                        v for v in result.stdout.decode().splitlines() if v.startswith("Workflow evidence: ")
                    )
                    directory = Path(line.split(": ", 1)[1])
                    request = runner.strict((directory / "request.json").read_bytes())
                    summary = runner.strict((directory / "summary.json").read_bytes())
                    self.assertTrue(summary["successful"])
                    self.assertEqual(request["version"], 2 if profile is None else 3)
                    self.assertEqual(len(request["rows"]), 2)
                    self.assertEqual(
                        request["assignment_policy"]["seed_digest"],
                        runner._SEED_DIGEST if profile is None else SEED_DIGEST,
                    )
                    workers = [
                        runner.reconcile(request, i, directory / f"worker-{i}.jsonl", 0) for i in range(jobs)
                    ]
                    self.assertEqual(workers, summary["workers"])
                    descriptor = windows.runner_request(root, jobs, suite_profile=profile)
                    self.assertEqual(descriptor, request)
                    if profile is not None and jobs == 1:
                        with patch.dict(os.environ, env, clear=True):
                            adopted = windows.adoption_descriptor(root)
                        self.assertEqual(adopted["request"], request)
                        self.assertEqual(
                            adopted["origins"]["modules"]["check_runner"]["path"],
                            "scripts/agentic/check_runner.py",
                        )
                    observed.append((root / "trace").read_text())
                    (root / "trace").unlink()
            self.assertTrue(all(v == observed[0] for v in observed))
            self.assertEqual(observed[0].count("case\n"), 2)
            self.assertEqual(observed[0].splitlines()[0], "module-start")
            self.assertEqual(observed[0].splitlines()[-1], "module-stop")
            # A stale parent seed is rejected before a worker can execute.
            request["assignment_policy"]["seed_digest"] = runner._SEED_DIGEST
            (directory / "stale.json").write_bytes(runner.canonical(request))
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(root / "scripts/agentic/check_runner.py"),
                    "--worker",
                    "0",
                    str(root),
                    str(directory / "stale.json"),
                    str(directory / "stale.jsonl"),
                ],
                cwd=root,
                env=env,
                capture_output=True,
                timeout=25,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"Worker source or discovery differs", result.stderr)
            self.assertFalse((root / "trace").exists())

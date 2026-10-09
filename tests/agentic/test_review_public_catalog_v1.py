"""Closed public data bounds; no provider invocation or readiness fixtures."""

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

import reporting_activation_v6 as authority
import review_batch_windows_v1 as windows
import review_public_catalog_v1 as public
from tasks import digest
from workflow import WorkflowError


class PublicCatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def context(self):
        return {
            "pull_request": {
                "id": 32,
                "number": 32,
                "html_url": "https://github.com/Zi-Deng/FLOW-DC/pull/32",
                "head": {"sha": "a" * 40},
                "base": {"sha": "b" * 40},
            },
            "issue": {
                "id": 31,
                "number": 31,
                "html_url": "https://github.com/Zi-Deng/FLOW-DC/issues/31",
                "title": "fixture",
                "body": "fixture",
            },
            "designated_plan_comment": {
                "id": 6074133818,
                "issue_url": "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
                "user": {"login": "Zi-Deng", "id": 29555112},
                "body": "inert fixture",
            },
            "issue_comments": [],
            "pr_comments": [],
            "inline_comments": [],
            "reviews": [],
            "check_runs": [],
            "note": "",
            "commit_statuses": [],
            "hosted_receipts": [],
        }

    def write(self, value):
        path = self.root / "context.json"
        path.write_text(json.dumps(value))
        return path

    def test_complete_context_and_legacy_refusal(self):
        value = self.context()
        value["note"] = "a" * 2100000
        path = self.write(value)
        self.assertEqual(public.read_context(path), value)
        with self.assertRaises(WorkflowError):
            windows.read(path)

    def test_exact_utf8_bound_and_plus_one(self):
        value = self.context()
        raw = json.dumps(value, separators=(",", ":")).encode()
        path = self.root / "context.json"
        raw = raw.replace(b'"note":""', b'"note":"' + b"a" * (public.CONTEXT_BYTES - len(raw)) + b'"')
        path.write_bytes(raw)
        self.assertEqual(len(raw), public.CONTEXT_BYTES)
        public.read_context(path)
        path.write_bytes(raw + b" ")
        with self.assertRaises(WorkflowError):
            public.read_context(path)
        value["note"] = "é\u2028"
        self.assertEqual(public.read_context(self.write(value)), value)

    def test_malformed_duplicate_nonfinite_and_bool(self):
        path = self.root / "context.json"
        for raw in (b"{", b'{"a":1,"a":2}', b'{"a":NaN}', b"\xff"):
            path.write_bytes(raw)
            with self.assertRaises((WorkflowError, ValueError, UnicodeError)):
                public.read_context(path)
        value = self.context()
        value["issue"]["id"] = True
        with self.assertRaises(WorkflowError):
            public.read_context(self.write(value))
        value = self.context()
        value["reviews"] = [{"id": 1}, {"id": 1}]
        with self.assertRaises(WorkflowError):
            public.read_context(self.write(value))

    def test_symlink_hardlink_and_directory(self):
        path = self.write(self.context())
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(WorkflowError):
            public.read_context(link)
        link.unlink()
        os.link(path, link)
        with self.assertRaises(WorkflowError):
            public.read_context(path)
        with self.assertRaises((WorkflowError, OSError)):
            public.read_context(self.root)

    def binding(self):
        return {
            **dict.fromkeys(
                ("local", "hosted", "source", "context", "identity", "policy", "inventory"), "0" * 64
            ),
            "profile": digest(public.PROFILE),
            "contract": authority.G20_CONTRACT_DIGEST,
            "authorization": digest(
                dict(
                    contract_digest=authority.G20_CONTRACT_DIGEST,
                    approval_digest=authority.G20_APPROVAL_DIGEST,
                )
            ),
        }

    def catalog(self):
        raw = b"line\n"
        (self.root / "source.txt").write_bytes(raw)
        rows = [
            dict(
                id=f"{n:024x}",
                path="scripts/agentic/review.py",
                artifact="source.txt",
                kind="source",
                start_line=1,
                end_line=1,
                bytes=5,
                links=[],
            )
            for n in range(1, 130)
        ]
        rows.append(
            dict(
                id="f" * 24,
                path="integration",
                artifact="source.txt",
                kind="cross-boundary",
                start_line=1,
                end_line=1,
                bytes=5,
                links=[],
            )
        )
        return public.partition(self.root, rows, self.binding())

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

    def test_global_bound_duplicate_and_missing_material(self):
        catalog = self.catalog()
        catalog["items"] = catalog["items"] * 19
        with self.assertRaises(WorkflowError):
            public.validate_catalog(catalog)
        with self.assertRaises(WorkflowError):
            public.partition(self.root, [{"id": "0" * 24, "omitted": "missing"}], {"source": "0" * 64})

    def test_literal_g20_preserves_history_prefix(self):
        self.assertEqual(digest(authority.G20_CONTRACT), authority.G20_CONTRACT_DIGEST)
        self.assertEqual(authority.G20_HISTORY_ROWS[:-1], authority.G19_HISTORY_ROWS)
        self.assertEqual(authority.G20_HISTORY_ROWS[-1], (6072111969, authority.G19_APPROVAL_DIGEST))
        self.assertEqual(len(authority.G20_HISTORY_ROWS), 19)

    def test_whole_source_mapping_line_equality(self):
        raw = b"x\n" * 125001
        (self.root / "source.txt").write_bytes(raw)
        (self.root / "source-index.json").write_text(
            json.dumps(
                [
                    dict(
                        path="tests/agentic/example.py",
                        snapshot="source.txt",
                        blob=hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
                    )
                ]
            )
        )
        (self.root / "base-source-index.json").write_text("[]")
        rows = [
            dict(id="1" * 24, kind="changed-source", artifact="source.txt", start_line=1, end_line=125001)
        ]
        public.whole_source_mapping(self.root, rows)
        mapping = json.loads((self.root / "public-source-mapping.json").read_text())["mappings"][0]
        self.assertEqual(mapping["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(mapping["lines"], 125001)
        rows[0]["end_line"] -= 1
        with self.assertRaises(WorkflowError):
            public.whole_source_mapping(self.root, rows[:1])

    def test_overlap_normalization_and_exact_mapping(self):
        (self.root / "source.txt").write_bytes(b"a\nb\nc\n")
        rows = [
            dict(
                id=str(n) * 24,
                kind="source",
                path="a.py",
                artifact="source.txt",
                start_line=lo,
                end_line=hi,
                bytes=2 * (hi - lo + 1),
                links=[],
            )
            for n, lo, hi in [(1, 1, 2), (2, 2, 3)]
        ]
        result = public.normalize(self.root, rows)
        spans = [(i["start_line"], i["end_line"]) for i in result if i["kind"] == "source"]
        self.assertEqual(spans, [(1, 1), (2, 2), (3, 3)])
        proof = json.loads((self.root / "public-obligation-mapping.json").read_text())
        self.assertEqual(set(proof["mapping"]), {"1" * 24, "2" * 24})
        lookup = {i["id"]: i for i in result}
        for old in rows:
            self.assertEqual(
                {
                    line
                    for k in proof["mapping"][old["id"]]
                    for line in range(lookup[k]["start_line"], lookup[k]["end_line"] + 1)
                },
                set(range(old["start_line"], old["end_line"] + 1)),
            )

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
            contract=authority.G20_CONTRACT_DIGEST,
            authorization=digest(
                dict(
                    contract_digest=authority.G20_CONTRACT_DIGEST,
                    approval_digest=authority.G20_APPROVAL_DIGEST,
                )
            ),
        )
        with self.assertRaises(WorkflowError):
            capacity.largest_fixture(components, additional, {"guide.txt": b"g\n"}, dependencies)
        result = capacity.largest_fixture_public_catalog_v1(
            components, additional, {"guide.txt": b"g\n"}, dependencies
        )
        self.assertIn("catalog_sha256", result["manifest"]["dependencies"])
        dependencies["profile"] = True
        with self.assertRaises(WorkflowError):
            capacity.largest_fixture_public_catalog_v1(
                components, additional, {"guide.txt": b"g\n"}, dependencies
            )

    def test_public_identity_and_metadata_closed_route(self):
        for key, field, bad in (
            ("issue", "number", True),
            ("issue", "number", 32),
            ("pull_request", "number", 31),
            ("designated_plan_comment", "id", 6072111969),
            ("designated_plan_comment", "issue_url", "https://example.invalid"),
        ):
            value = self.context()
            value[key][field] = bad
            with self.subTest(key=key, field=field), self.assertRaises(WorkflowError):
                public.read_context(self.write(value))
        for field, bad in (("login", "other"), ("id", True)):
            value = self.context()
            value["designated_plan_comment"]["user"][field] = bad
            with self.assertRaises(WorkflowError):
                public.read_context(self.write(value))
        value = self.context()
        value["pull_request"]["head"]["sha"] = "stale"
        with self.assertRaises(WorkflowError):
            public.read_context(self.write(value))
        meta = dict(
            public_catalog_profile=public.PROFILE,
            plan_comment=6074133818,
            issue=31,
            repository="Zi-Deng/FLOW-DC",
            schema_version=7,
        )
        public.verify_metadata(meta)
        for field, bad in (
            ("public_catalog_profile", True),
            ("plan_comment", True),
            ("issue", True),
            ("schema_version", True),
            ("repository", "other"),
        ):
            changed = {**meta, field: bad}
            with self.subTest(field=field), self.assertRaises(WorkflowError):
                public.verify_metadata(changed)

    def test_context_observed_mutation_refuses(self):
        from unittest.mock import patch

        path = self.write(self.context())
        real_read = os.read
        changed = False

        def mutate(fd, count):
            nonlocal changed
            raw = real_read(fd, count)
            if not changed:
                changed = True
                with path.open("ab") as stream:
                    stream.write(b" ")
            return raw

        with patch.object(public.os, "read", side_effect=mutate), self.assertRaises(WorkflowError):
            public.read_context(path)

    def test_immutable_git_snapshot_limits_and_deletion(self):
        import subprocess
        from types import SimpleNamespace

        import review

        repo_path = self.root / "repo"
        repo_path.mkdir()

        def git(*args):
            return (
                subprocess.run(
                    ["git", "-C", str(repo_path), *args], capture_output=True, check=True, timeout=10
                )
                .stdout.decode()
                .strip()
            )

        git("init", "-q")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.invalid")
        raw = ("é" * 125000).encode() + b"\n"
        (repo_path / "whole.py").write_bytes(raw)
        (repo_path / "deleted.py").write_bytes(b"old\n")
        (repo_path / "too-big.py").write_bytes(b"x" * 500001)
        (repo_path / "link.py").symlink_to("whole.py")
        git("add", ".")
        git("commit", "-qm", "base")
        base = git("rev-parse", "HEAD")
        git("rm", "-q", "deleted.py")
        git("commit", "-qm", "delete")
        head = git("rev-parse", "HEAD")
        (repo_path / "whole.py").write_bytes(b"uncommitted replacement\n")
        repo = SimpleNamespace(root=repo_path)
        limits = dict(max_source_file_bytes=250000, max_snapshot_bytes=12000000)
        old = review.snapshot(repo, head, self.root / "legacy", limits)
        self.assertIn("omitted", next(r for r in old if r["path"] == "whole.py"))
        current = public.snapshot(repo, head, self.root / "current", limits)
        row = next(r for r in current if r["path"] == "whole.py")
        self.assertEqual((self.root / row["snapshot"]).read_bytes(), raw)
        self.assertEqual(row["blob"], git("rev-parse", head + ":whole.py"))
        self.assertNotIn("deleted.py", {r["path"] for r in current})
        old_base = public.snapshot(repo, base, self.root / "base", limits)
        deleted = next(r for r in old_base if r["path"] == "deleted.py")
        self.assertEqual((self.root / deleted["snapshot"]).read_bytes(), b"old\n")
        for name in ("too-big.py", "link.py"):
            self.assertIn("omitted", next(r for r in current if r["path"] == name))
        self.assertEqual(limits["max_source_file_bytes"], 250000)

    def test_whole_findings_and_relationships(self):
        raw = b"first\nsecond\n"
        (self.root / "report.txt").write_bytes(raw)
        rows = [
            dict(
                id="a" * 24,
                kind="finding",
                path="reviews:1",
                artifact="report.txt",
                start_line=1,
                end_line=2,
                bytes=len(raw),
                links=[],
            )
        ]
        normalized = public.normalize(self.root, rows)
        finding = next(row for row in normalized if row["kind"] == "finding")
        self.assertEqual((finding["start_line"], finding["end_line"]), (1, 2))
        self.assertEqual((self.root / finding["artifact"]).read_bytes(), raw)
        rows.append({**rows[0], "id": "b" * 24, "start_line": 2, "bytes": 7})
        with self.assertRaises(WorkflowError):
            public.normalize(self.root, rows)
        linked = [dict(id="a" * 24, links=["b" * 24]), dict(id="b" * 24, links=[])]
        public.add_relations(self.root, linked)
        relation = json.loads((self.root / "public-catalog-relations.json").read_text())
        self.assertEqual(relation["edges"], [[0, 1]])
        with self.assertRaises(WorkflowError):
            public.add_relations(self.root, [dict(id="a" * 24, links=["missing"])])

    def test_complete_48_report_projection_budget(self):
        import review_capacity_native_v1 as capacity
        import review_report_material_v1 as material

        reports = capacity.reports()
        self.assertEqual(len(reports), 48)
        for _layout, raw, projection in reports:
            self.assertEqual(len(raw), 10000)
            self.assertEqual(material.reconstruct(projection), raw)
            self.assertLessEqual(len(projection), 31343)
        total = sum(len(projection) for _, _, projection in reports)
        material.packet_budget([projection for _, _, projection in reports], 2000000 - total)
        with self.assertRaises((WorkflowError, ValueError)):
            material.packet_budget([projection for _, _, projection in reports], 2000001 - total)

    def test_closed_binding_and_each_catalog_bound(self):
        catalog = self.catalog()
        for name in catalog["binding"]:
            changed = copy.deepcopy(catalog)
            del changed["binding"][name]
            with self.subTest(missing=name), self.assertRaises(WorkflowError):
                public.validate_catalog(changed)
        for name in ("profile", "contract", "authorization"):
            changed = copy.deepcopy(catalog)
            changed["binding"][name] = "f" * 64
            with self.assertRaises(WorkflowError):
                public.validate_catalog(changed)
        for field, value in (("bytes", 500001), ("end_line", 9001), ("start_line", True)):
            changed = copy.deepcopy(catalog)
            changed["items"][0][field] = value
            with self.subTest(field=field), self.assertRaises(WorkflowError):
                public.validate_catalog(changed)
        changed = copy.deepcopy(catalog)
        changed["components"] = changed["components"] * 25
        with self.assertRaises(WorkflowError):
            public.validate_catalog(changed)
        changed = copy.deepcopy(catalog)
        changed["items"] = [{**catalog["items"][0], "id": f"{n:024x}"} for n in range(2401)]
        with self.assertRaises(WorkflowError):
            public.validate_catalog(changed)
        changed = copy.deepcopy(catalog)
        changed["items"] = [
            {**catalog["items"][0], "id": f"{n:024x}", "bytes": 500000} for n in range(24)
        ] + [{**catalog["items"][0], "id": "f" * 24, "bytes": 1}]
        with self.assertRaises(WorkflowError):
            public.validate_catalog(changed)
        changed = copy.deepcopy(catalog)
        changed["items"] = [
            {**catalog["items"][0], "id": f"{n:024x}", "end_line": 9000} for n in range(24)
        ] + [{**catalog["items"][0], "id": "f" * 24, "end_line": 4001}]
        with self.assertRaises(WorkflowError):
            public.validate_catalog(changed)

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


PUBLIC_G20_BODIES = [
    "c$}@h*>W3Kx-NLnr^r6l5oPTHnDZo-cZ4iaHhasGjzn3m?&x5woG6q)fEx)&W`!^69Zp}K^K$!1`um5qawSN)RIXD|wn-33tl=NNfBr*oF^-2mjl<V((j@q=zyE*1*lut84*&VkcZc<Aa~+tQe$(Nf{mIGm)i3=R{2`4`PF}xx_4?J@^Jiy)IqW}dcdPwf@N*FrRT<@VQHI~g)oJi_yWMrGjoEK^kAvGovtG4kzuIp8{B058f6d*#AH(hEO}|^x&%vjDw;J?4;kY-Oju&uyyW4&;>*a^lh$r0zyT09S+SPicZ|rZ)&fw!%#WlWd9XuKLI}^nw>W4C3=4I3`%dC%<P1z*NB<+jB)M=5YMYy8<FVlG0Y!92R@0Pp%)2jcxJZx6`G3=Tl>*KyHOx9OfTt%j9@Y$ls;y5q!rml;=$jxvXTx>pI?fc;8p#P=qZ}<8iEN?suHrrse`Ggb1!CpKIyVZ3+;_177?DygI&TRJh*<i2zh<9(-UC`~!un$%@H;27x*8S<p$shg@yxy&D47T5{x8q^gpPan^uwD1~%BEj$KR?csJdfX9KCk3h-~R9<$&0$JlPd3$tSg#o80sRbO_`QinGHkPCgqT4RU21BUtpg@Rg`HK4?{cjb<w18)GY#>bdTk?>jPiL`_te}|DT7|t`FY-@an~RF!ngLaeLUceG!#O6eV?$<70HI!T-HlKI=Evi(n;mwg}$6c@ccr?{CM)VR*gTe>gO!?e-?rH-^t&yn4TU_Ehd=4Ab}z{1e_n6u6we;c;E(WkA~v{QX9_!>Eetx-W+&>e@8VaE1L))OFID)(owQa+8_1POHi!eN*>YYw${)HeJ(3r@>p^3KooO+fT%O8hp3if51U>{Vn(bi&@_VAIu0g2|n{Z-0Jn<TKN&UO1&u|E~bO=a6@kjw@e#m9pY1;K)WGW?c;L0!9&MGGwxUW!@j4Z9u8w4+~5#!_+fXrWftng&%5pBddY_@XB9Tv?Rp97x-sivySwSTkcemRme*{%yO60VBzEdIhAy_p55eaZ?`&_{ebAcCc7yL&2SRy{vfqb4h3~@4@H$T2R=42jYok<tgD=~y%zE`VJl3=ym~ukw*{wSG(64X%-D&U&j6Q&71Mu_h2<krn1>apoOB^c^xLd>j_4d`v$5oZjXu9i?q3&B;b`s+j>ppE`lavi;yJ$00WK|PKY0_o*If~1&?aL@F`owfuZ}Jv<dJE}xW8%=Pujz6>+yukwmp$Fo^=^B(U9NBU%bRieDLxD2E<b=hgRhka_Cr@U=Jsm*V6fz%Ex?>@RaKd(OlQ&#>*~9%>C8|!4c^if9ku~Zth%x-i=t`TqQ?H>yczo3oCdE!Y<(P+fgHl)lO5O)a^&={<8~7iZ5<CqTsCEE>aHuIzKR=@H@z9U#zYzZlQcMhEQSn>%Qo)nu7P-Hs>U=$)t)F}%G1&|ZSy+IhYo@us|(Y$Z5yR{SRQ8s{@6#Rt+J%T0-Gv{(yHr|x*Yl>&1+NCC!e>wj}Yn+KW-hEHfxf)?b9rau;m&nfso0%q!`*{NK8}bbsHxc1XI^#Y1%beT{cbEnkwp(BDT+q^?7ZSbR}*xPGW4W%bFCM>hY^Fbv{I<!d+BN6XU66oVHEUM0wX>WhDei-VY}y%*^uCF^oY-yY1&O%<7z3dR*RYJ7_x5M|}s1-F5vAywvr>Y6DhOi5KEypq<TizXMNf_F#k0{p$L|egr%G@=d(>2Iu+>QTf$oTtcJm_W0q^pKAW!qYz)s|9cb+yY1il%~`;rqRQw+=ugWCk3mv9;$>opkffnmw%aw7U%w#<b7jVMwfgpQ2SGqGk=Sn2e}+&2wZGg3P>_4DCJSrmn*D+HAX??zDXQR`vE8k1p_$Az7;)7Ghs_!)DcG~R>bC9p=;Q>f(|)|#9X1d>r?+>H1JeO6SQUKr_UWtF=g;t;AI_h>dvWpd`>VI-=g&g-I)1r&cJclB+sja(#?=Hhu4sARJPM%eR~^I()D1~cZ2Mz>H-`JWTgZSG2eelm>PfN?TLM1>Btceq8=vEN8ocPQO?wwSu%OvWj+PLXu|5cj9HQwoFp$948&X17w_Av_J6aP6sIx+QwePR6oME-@zkRvg^p6Aj_qT^l`@w)dm~_lxy@!IQ52!+1@&_SHB1oHWab<i(XE^zf8%TGS>d+{p1x|yX<Xlv0oAr80iUH!LU-vCh$GuZo1S8;Epeu$fy2WFjLqmO-Q7sM+(wh!pDaU&NRo&h)$t?v{h5gMfAYRjdFrQZ2ojn%NK`3klrLes<*R(vnKP*TO<V;!3{bdZ+s~bXCbA1X9TY)OTb|V;qRyna5sM1}*Xzv4`htk1Q0V9b+!~F`{7GUZlsY?~=AYFaxU1*_WAGYh>Y!0{h&KtAZ*!_T4pgmhGefqYr{{?W;NsAHMj2K<-6TBM!lqfONMTS46`m~c1L6cVv9q!d<Y!>^t`V=$Y2n$`Q7!OS+nBWS6huIK^%xAFVJ>G(Aae&jRpe5Jq@iD%Nv?~;}o;8!#DbzWn&3cXdvkwM=F|5BaIFJK`!EU?V)49?0j^X#KeaPQODAXRu6Q8$-!+L$i>_1*z$8~sh^n&0BdQmSAHbCH1Y$#W`oXiDmDZJf|*a<%(1Vk|F?KP-)yrlz!)CCmQ^BDs{|0E3;49(iZZb#?vpV*7D8u-kKaEX_(1p-%esGkZxbNafaXNW*E?dd4K$Levb{SJtXHegMTP)&}w3TcN|`s>7US0E=4M~nNDef>>r5_%bA6J9kdF=86PeC%`7>-j;STgplQ$*rSu<__bhFBlJCJhw|CX`*erIJ;MSKBRrvq~j&irez6Ao42O@*zfrhv_o7<*8{vl-;5x9!2WUd3Fz^#T6d2XO8y*9I2;_v_z+*<BSb~MfoZTd(9xRr7Q(gx5$j2hq2R%htsiDBtRK2ZY@*->42lhoj+>s`lBi&HbT#dNcK@<@y3f!qp#^Rl7Xj4In<tl$|I6>+T|7HKy@8Z{_437ENKeWmAJ#{B90+lbO#&S(8$67(su~>(6KGdhEUv#}A=U3#owCAQ(vykO2Qr;Qn8G;Cu%$!ZkVYQEr!Ou-vui)#bvg*zm1DnGq$xI#;2o?y@Kpad5D+9Y&I%6zETIqoXg>Dg(~A&-l;o+f{f;DE4=ECYIyWOdl_@k7RS-6Z?z+bpIrRj9N%rXOQ%`6Jl!nbgx~)XGkl<YN+5?ueiz?2-vaZiq{E+-yZ|MqyEzFkPlBD`Cq2cQyowCNMK>Mr)lACf;WJ#^sNx#1AKZjOR`l=}yu$`a*k)E;Le{+j(@WkL4D&RU}LJw9$K!*J82A9;4pa>Z5s0pnalsO-iBuePy<T)R0V0PD^LRnrYGFXL{7<$k;Z_b~k(HI!A@4;=v*eZ6HFdXQ2PfH1wBRfe1qrtx+T(EOch#-b=+x!(wNvP)LcHOgTUx74Qzz2{&(QmiZEAy;2JZep({SB~p6y3-n(HRAw#v%bIE^fw0A|;=^z6e)0eE+mALkcnIce^c_VW8t-+X7p3aS}n7fDd8!tu}XYm@HLg+m$WZoU?Y4Xj>=-vdS$0FeKI&o?@Ko#h=o}T&X)16?iI79zxo6&zDH2+y(@LJnI*zm|!@5Qp5v%Fvo(7o|bwy<VwN~5m<Tv*(_AE+=Y{BBI0mY;)%p1S<zOwL1967(%U!vH8aO30KzmoUMDFpb?d<9gfYHq(}>9D`g%v7=pjeRQhpq~ISfNcX1m$53>a6upl*eip8%k)Hp`p-hJ0VND%ujOSyibdg_a^?mIFANwLLTtdC*w+5MPUWggHOng5pF?)AI>yf{6t{TEb$xJQU_3bw?Hl@Bw2c!LMf6^OcWezP~(wpXJe?V5diaO0mU1;h%5gqyVp+2@7IbojAO35*AR1Hlc$6?Tzd_U;xEJB(93+jL3mV#DBf6@VcWN?CutS{L_;cFOEOkPToh_EiXPPqbT_1TUzNOAmP;x(isY1$p-$@3*H+{8nJ(}90W*o)g5qR))<T<yozVgMjxFyctmHytgqKDc7Z)mvoY(taW$r7)>#LdaD$wJ==d$$F8BDuVl-U}iLWCcC`-{mWxgCbX{9+f3Vy)4LQ-KNsU>j0O~1RIm>Lj}z!wAnN#y7EX#tQGxBcSvWl`m2UWfdj>V||k`3DKz;FRbradT_a_ACZh8^~;tM$lYL<;0}0SbO#miC(dS53AhL+S0Nr05Ga3&iE7wlJDt?S(^So%Q#;||1Y`NaI0VxbG_NJsBro&=(fE$@#GX>bM5Ku_oS|Xg~2t4HT%}qrs>z=rvwxO$kfVN{iuI7ckIi>g6Q7)WUmi~07_2=4(?pU3vdR2L&#Vb=s^CkF}LG~EnUoK!bss7Dh6z$)^qT7JM2G0hBD=fq<ngj<qIgLPdynHayB5Eu*N?i^|b*Ojb;GC;yeaTqZliY`nP7)MIy+|4Ss>Kq{wDE>m%dmsQ~aejxA8pJF*y(cTW#yVY?y~AfRGG)FG3U?>BwJICJ)%4+oGyAn&yNgEwwPiIE=SOR~#U)eh|-n3XvUgr)|INDvU_cjw<<ybR8Ne)9A(_~GL1<*PRrPoKO1bo%bqv%du2+!!|CEU|XvbGZqAesuB&8wLr&zy(5nehgAN5G9ga*!QGEq05(KHPHg;__v$0$^KGY1{pvQdkEg+q@eak95D%#@xky3Lr~TlE7CT^FMLD`4VU;)6Vj}r!}%A&t&_&W;jryJ9$HdW<i7@s8+@2u)XiP6hnfr2-3;Dn5A<cT7)@|<Ah3Gi56Sz*$KO&UhCL7a55PIs^v8a}jvUO92oA9ZU>D2EJ8NA@EPzN+#FW#l1kH9NFiEX0oJ{NO2d5B_Z(Q+{d^=@9%h}wQ#7Acu?6*fDqTn4&fv<@Zod(agEJ{R72gY8{#j^z)ZIc;zWI3u&hVI81c0enq!MO#Hy4UF{>DVcf7H3%uA^Uw?F5L6WBp$LlF@2Oam1&DKFUq2*>!Qt@s*H<rsN<|H3sai5tD~f^fJTNi?|UfYE+q)&nYU4&#ChIiO;ZnTN;|Fcy2ab0uITEf?vp&iS(H(3>ZGl^DC&|lNwa9cm#2B_pi^AvKGUr2^M<_aUDWnzok4iDb#2<*WLZ=tX2{DbP9uD4otCDmP2bj)iJCf2t29kdgYT?`C76Tw?}zgz&)Q^&p$w9IfM6WbYKXhGE~6^#%BsU9^?f}QSyhmgaE(Q=w^od9c2MmB@q^!t_MHZoLe%bi-yahY%m5gHDxO&S?>~V0Ty!j})4*AX{70tM{b%;Z282bSn;|Q3#jA~D1A5P4qq-WV7@I0T$1e6Fp1fPq?jp7)R+^k5!Fw3Uz;ypTd=tJlyZv|JTiD|7V3WUR9|`~m+e?$o=Qt%XvK3)qoPGnbuw(4{b0F+JN__DYFF4Z^6q@{TbOL(Wtw?VYxm$}A`u}j#FnwdO?1|~w87V=l0L0u+iu8=v@jkVPc6y$8Ew&;)CN(eU3X*j-5O-bEj_48UFUB#hJD8k+wS1(qg|py&E*R;PI%4=U%78$@Uc4`~;VhgEWIYRY#k`Y5J_V!xah%bNk3?u?0<9%R#$ZAYRldEso1H)YetXN#A~|qqg)KQAg3moplzbsXI93*t&F+MVilg^>yW<o)_Q(^IH%=0e{I%N=KYV5dAQuG)vJ+YtZAnal@3iVdpM}$-M_B?+s;n+W{<D1q>EGZXH`vGibKlF&SnaDfftN<|*3*U2m)k!B4GB^N39}XzwiIfAufZ=I14+0NIKR#Nu4$9FuFE3r>LkkgvZ}f=!!JV;l~r3Ays?auA~#hBq?dJNnhalt3-1{&JPZonKMquM1Rofs`uFWXf%2zbs>NrHw0;W+@M()h;PNgk`;NXv<CEdnYGvU(KgohlSjm4fcZHPL!S#Tu)wg?4L>w-YM`S`o5328lNQB9+d?aKi2MPq>o@DOK2dc}{i~v@^RynE;T1=XDQogY0l(OS8nCoV8eF-W80;iBqxAkNHf4jFrJkFXF3P^V@`b~Iu1h4#Fq=Ak48cf4PfmLClsI<kTX1-Q2vN&N2Ld~-Nm%cp^B{8lBm9nFEZHRzG1-+lEAig)J`v$h1)xJGBxquAZ5k$r@K7IA$>lf#j=k~6C0-DKkDEK^E#AQ|#_wzQYI;-0(DeE*dF@CC21MW>sS+-RN%n%J_0j4%Z(=;8xPZZ-In*<nXXri)BJtAm^rW?|(E#fT6Dxi&|ud}?3hZYzkt@;Y6318B8rtb2-Y_cJb;s_$FPV3kV^;z%^>rL`B!b%r$l+>E2)Mn+-w5jRwbc6rpSsJ(HFqpVWlfFx$IFC#LpapT%mU&SOL(*mqKvdPFF?m`p@!&Yc8|!Eh$5mSDgWDuY`aFUx8;U%}D$^n9izrEoE~=vxN01M=Nx-Ibh~qY|`x?(Ex~7S`IE~K&PRdxDO%Y{SRhFj7BCqQ@ljqlFFnOARJPJce2x(UKbpxmf8Bx@2mnCV}wOuoGO*P=@bqP6|CPma1Fgxo^{P&Zi{~pBf6G0Y}>Ir(YTdzFkP;sJpG6F6Xj^O_sc=K_|#_TA*u9$%Elr?s}D3VjpBgq?D9+s(5Z+4t6#4=vk9FK*U+_sQ>H)4UVHytw+*iL*brzEI}$W_#<S7^z+k8g$*pO#fpXGv9MMPT-1B<^GDt$?6-{f2xY%AxFdsS<$nxTuFHYm=<2O<G1!PEpwn2_!(54FhgDhOib7jT*`9RoEW(nSuDIlC%cE?t2prZEK<us5)+n9&}Z9Nzs@-ISrnZe~+SR!e1t@bI?&g)CR-}g6yC^^SDf_s4DXTM3HA*RMuJCl}Qm-eHIr9kUua#Id5Z!E4+Zglihwbfa56pC5_7(AG47Rm8{Uj&5%P|g4ax3f$6Fif&f$qqoagL05*yGG9LQ4=(;Q)s=6z|W>`#Ry2eb>hTsy2Rn(cfxx~BAu9V=fpb-dL^0V4txQ&NC6lo7%-+_KM>1mvt#`zg340dI+f5}q9Vsg}aXgwV8HTWBWT&TP~&iXz3zhlr}hda{H+?YqHBu><2b_xMFhOhp^2EwDcCCq*k&G7<mB9*BwYzmAp9VqcqfaUhEcj;-+q7ZyAF}AnMb^obf(;M(qFu+sxj2&(%GUMC=&L>8Wx*gEFBzK$!l#bQ49IhID%7)OMD4U#Gk`1G}ZDMuj=u`qAPUn=RGAE8FWYAAuTlaS5{=;U!JrGxd=bkEFT5n<1)K&fD#S7~kPLukSoXQL57lwj6&7+)+Fnu<}XMUQ+@u*&W2brKn)PedZJeDqkbI`~juIltOiE4qL{KWxL_9$LU<$uCKE0zqDSHf@7P$UOZr|>>u7BKl~Sw|yh{1E}SBDx^qaKjW(-3Kn~qDmGL$MMo`@#GcB%`<Vt5bzH^+})BvFya>xii;Ugab6C!F>ET5gjMz{F<JOlii{~>tp<k#VgsSQx#tKx=F$R|qZvSgxAeZhSqq5J5rtkx->qnou*3?;bhCtq_kz@WF}L(&l{yu5Bt*Mx#%c6TWHdd+rk|Xgyi)6pj0g)n*}nFTEHw%xf;!%s4aFJeIZooRrI@iz$+RqH4cIegbxEx_dW+dzvsrW&oJuZhXsOCQ-)f7~yPGvJ@!VwS=$1D&WnirV`YEGXxc7|gFMZ<DenWUapdCoWl@I;w-bTNi7qR$<*#wK=%b6_hZ!VKSZXLGveQj;JZY9ximyU3z`DJ#UUlHCH;py#`u!9))5ZLU29VK~%--*9pzJDd;@c;p=uBICq>G4LB435@x?~M#H9QDzAY^c>`0od4zno|>Pdbay4MPTNV_icA}6ksts5!qKx-W{J%%qvrW<@I{o;9X>>NO;lr7%@i(xD!8vH7dxDMPT|PE1tED(o;z7KrSS4gt)r|8N+}ggv63Ug%Tfu^|Is3`_{0gvGU!eDnK~hv6AAaCwWf7&e=Ddta5VlAANrt9OWJc=PxfW-kiVqOYq%aF3*>2$@@q4)A^gX7q4E1Z{J;9o?q#2i$H#b;r;yL#kv0R?fKJJFQ2_#{C1+C4;?u<WN|+)(7r?q(cH9!jd<HUw1vU|FCC2%=_KN9)FY^nCZs$f%~UuIK!Yw&Myw}d=gI}W_{Av+WN*fg`1-XZLHaz*Z`9D6HDYHOBJ*6FcH0qf{<vCG#EHW1*P+eaYbj=FqonjkUcL_~SoqCjO|N&`y~doGz<jrunY$&uS_;W0;xN=<!zn2O^yE=_Zi9)I$6jh|@t51xj>N{TN(XU45Z$jk=Pj^XejxD(l4QAjz}v~XR`kb$X#BI;k#A_fopAD;Xz`zKM0tFBdV2bZG*m;W$j9j<E?*2_i;*GLtbu%TP)LS_Q->jCuBh13I8|`$LS<$BQXJ6?h3!{`&hmi_^#BNmZM~#Z%9~iPBwNEOEv$ezJK)S_eCRPQXxy$TMQ2Ll=P($jz)&&3b|2y&`p?UEFTxR&vD$oOAiUj>e|ka6btGh}+MnDW3-vL1?pn6@w+tqyO;d30jPi%fnk5KnerqCtD^^UKflF%xt+l(JiAbuDB$sz@uIT{W2|}9ckK`TVAX(G9Khta9ki`41zyD9@pTpYhgr@)N@BizOCgAqN(2EOPd~l?$?AOq>i8s}4i1J~;T4o~%G~lWP!WN)b4kVLtsh?lycBlKs6|nQ{?FU0Hr11_I>n0!%4iPvZ()I&{R1ofOZb1%=@pwnHw$3`GTb3Z=n0JXx5X|&Di}{FBDeNZ1d|KFR-q?GEV4`eZHT|hHX8nocE)+zi4MQS`r%x+gIFufGh|b^Iit_lzRipU0HMc#154vPWNQBWfhQF}UPMgIwMP$4zAi0t8!%Ec)<?v8&8JEE3-ma&0nPA5Boc9u3t~PhpFoR`0_Ix;e;hN~Ar?aD=yQARMP7|$WWD7m~X}f|r1PN?E)2C=Z;Pu0fe);4w*Fu0{fF=`I(~Cefi43^|Vm!T1(~{ieTxPu8qc3}(ED+4DS?zfl;v-$p7~IfmU<z;~!?2?T0?{F2V1RLg#ZSH`jr{cL{3ig?mru{3Hs72-y?piNFIG6xR+z~?Q^Xd8!fEATZw>+&hT61JK^BBjw!-vG(@UJff$jg58U(~R9Yy62fcm5;ok8i`6z9KiHpq$~Nq{3X;)SA=L=*hyVao;x44^g67*-~oC~o*6aern#!;+D`U9)EZE0AU3)vZ2B36ApQ>2LHtPScdXPI_pRt5G8!0vX!&#)F1@I-wQ!T=#+Zu>`VyGtL8HgLUiq;O&`A>8Pi{JCEpv6{N6$l+dKbHcb3>q2waAn&TFS&U#}1!BDIU&_Z^lfvy#)%J<f+7Qsqkwtf=056BGVi#P9HzC3?(b$R~dYfYDATS;Is*<zy={B-z~MDzW{<qz+^yTS*rE}n(nZ%;1iDVJwf@V6gUq~%pAUOL$(?u=3FYfYe%Pk0Q#@>eSCY!RZzNOX*(9h_C*`DeTchkUmcUG|JsA?bH+ch1Ylr#;CV+_dI{2V^6zm<J~sQE!;@(BW>p!wfyaq3@I$FFs^PFGhWNfe2GB69`S~qFJs6L9KpvF5xBt$~C@wP<O~^Jy4TpcP}W4Z2R;y_;Iya-5hR?GCUuOqj6$kaNj;#jS^adJXx++4a+X)XpplN%I&pwi8O)aD}lAQYxanM@OMjxX_U3dJaJ|?$mF(&f}1T^o}i4f>F8M)-r=^}+q;nRghLmVo<)k!Et(a=wibaWYA1-}QP{D9ooZ|70?FNYPt{U=quEH_r48#4h=PUz+fu>B+l!xro%zg{&1t_=CE+6QD*>&cVk`OF@A1*^(dl}7?fn(MIUAHtCIHr0{+m;l3+KP$tU3JNvy`SEHlJ3zt>kv&Z7L@hftT{e*C+l_L0Z_Er>O|gkPc=s3Gv*o2iAsxMQ0v9k?JH8f}8D9R4-*0H|uRnB}9Qbyn0%}K=B0?xSAs@@HZ5DcwGIhr}S~dN=Cp0sS=g4UhHkbx?9A{^NZ)NKn9;yloGim4@&4*AM~{Ac%9~^SzRAE>R{MZhYSY#re$*V1Xd#^{GKulCp@L#8g{js`1HA0<IF43fGb9udnt|~rQz`+-JRO=8^8E@QVr}K9{0BT<~1p2d~pM+23v~V`dA8rK@@57<LbLJ`Be?vxZszR2=zB5!Z^oo*bHcOzugmx6bg=$5`S2*a&hct|Hcn+uKPHt@PhCxu>xgc<HT+_Nq1B+xY437J}FK^AeW_!1E{$<?su!!rgdM5M~C^=CJL_m%{3{6?+9N_3v)C<4<NhJ<?oQS0rjS}hG$MOaFPWcY?BR~1st+5#nnYz2KSVB%i%mRrD<WHW$mfJ8`QK3CfPd<bRWoTX)VfNDjLUDHj=werKZGbUZRq~alN;Iu>kN3NfHZ|7Lp;l8!5#H>Tk6>4W1J)p$iwuCpH;G>8_K2J9$x9K`xY4DB+pi)5K9{w)$OnM;?01{QkG@{k(fAR#e#_w*fnOZ6lO`|IF7?e6a|o!V~(2Ejz+{lC3uaN7tHzL>Pyp$KU$hRu0x)kd0@!JO4~kNMb2c`n^MrxaS**ORNq5<N`tDw0kn!D<m4wFD+pVCg-ppiqfS(?tY;6^QMlx;i`mfAk7*#{SD#ad471PZuqGgh2&8Y9NFD)Ce&&lv3EqXr^tgs^vhqY$rE;QqM+EnL6x{?HZ00uk<X4k=}5~{@$4lflV%-!FrSXkKVtJkPg#IUtdvDn!jJW}D<&Tjuv0z7LWJ^y>u2`_3a%(VOEvoZ3waFW7YV*zSwJQ)f-|mGo%~lrVhig;S!f9v<lun_ir|s^4S48^JtXoYBq)}!Hn$^Hn+c{MMR+^?;-uFV3s8|Y3%SyTM+{e7$wN_3oTYkdwQiY@ZaNocy9Gzc7xFaeOt8RZkK}|G77UdGL(6`m8>Lu;OKn8#vH5n`*cv5az-tI_;$Aj~L^u()IuOWKgh+LSpiZi3^&}B-M3F)FQt`kq`b}pN&*zjxHj~KpN7qT~)tx!{i1$!A$1m33b{wo<{AM_qZ6`JZwEc~z0ZHHr5d}w0O3tWC&h98+A|t59Rcq`JqN=4w3lhFrgncanA&BWTwhfElE^xs}#m<Ci)Xk+NNXnWO{k&qD8A$?`*DJCCJ}p`La?Y&WisBA->_|ca$JeM>jPEJXQmS{CPmlZo9u!l2##Ra&PfJ?NofQ!5ouAa4s~9HNzgkvM5==U0Ggen?6Y^&q?k3r#aY1X^aE6kVNq#abfDD@)Brz1?`{3EF$c!$4PQNc~B+^#X%rVRphI48;5FqlTuYh#3{%r2XZypIB5M);?PQ=zN88&-Ll037Yota3skjh>5+j+q6=vx+Z^UP0!JtFhvpXF=lmj8YEXDn((|5a)A?+fw8O2mvkRQ_E!*h#P(5^~v@yU7@{tprvZauVt~jrx^s?dWualVt?hZ|~7=gHcS#dl7Hk&9;*~-Fxv2`;jaAjBYgw7`B$E(+)PrK^<UJTHq_4F@Q`Z&)K9;tkzZ|!CBDuV{G4wZnb$7e7~Yi3CG>J)*4pxhoT3Wm53Bqn*-HOE?H8$HBy=#Ma?$XWDtwapJ2ay*<?tvf5PQxyVQ^$U>%U0U`LV-9<eP-&fc}a9XbvIdR(`ljU$wJwFN?Gqs)G3lV$}{5#jDIMGu;t)cHDr#iTxtT9LA+*8>+Ax|1rEcD+4N{rQfJ9L`3t*0_<l|8$A4w?VX{#SWI7vV4inQCQ)x+s67&xSWtmp2^iTa%HrNbagglz<zg9!09R=)elVpFPD}!6#%c-vlM<C17&hm46Q;F#)jr~O~r^iie1`ZCi_fP0M08trb@S;q)diuF=wXa4Zs1flncxkDnlt)#qg=oU2(*C;ZjYjIFs`1S-#0gh1$02S`q@w#8nq-8JyZA6!{Oxt+~_3p9bfW!&l}uA;q!D6iMdtZ5teB-;atI>|Qr5;>Lq}+L1#Xa(uw#Jdv(bfh`@EY;hZQkW+BCLrPmDEkk}6L7UZGcziP@D}lr~2}cVu`22C?q@V9d!H8y9ab6ScfuVuq_^UDO3aMNIFwToL6HovZn{!azrb*E)u5DgeID)PBl)vvw-_Xh;7|CO3bFd*6aa}e*IZqDI96zOHNxo2@huV*N^pZ~g-e%*Hn|v~PTpp2jK_HFSulrAiJ+t;*qTL?oDKLea*%R!<0y}AU6UULI@Y5`le9F3}#8obCz{z5_Oa{ULYqIZcMSyOU!;(~r@0tO4<z$QuoO5k2;#fLy_$%_#RRuwL(OP`*85^HdgEHq7XQu?+h>Li1{moYLJOqeZrc?=(tGkmt7jhZ1Jam`4s`*S$o?o87vF+r%ddXScA2jgPwxqmz`s(H78%})ie{%^7^x~7(uiw1->B)=Pi&F9M7k$kuDY6p8f76>+@83%Fn975=?oIBAOA1+-`!$AASTYql`>20*0G{vI>t&b5A)mq0<#aQLS~ehGDy5wkHjTe0^ux7{+g+Ro6T3`lA+rd=_i^FgR2kEyNm>r|P)AkUXZUA`hq|jW(^kpQbwyEkkxA1&DY~*5$|N;O)%86Dc3;+0mzGfymbg<!LlTNIBy7(DEgM*E2H*fewm4^D0$#M(Jaw8zNl`~h+qF?L)J;#tHBng?Q5)q|RhC6lXT53Dh&s8|SsvvkG6su_<D$u&Y*v^?=X)K;|MmC(ag9B@u6!O~b27@7127Md5GT|pP++N5{#s#7Z4a8&TDpQr!MRjj4;0xZdC6H7Qw6Y^GV_vk5mjVXP<RzUYRWIC{1Tkn-ewo{9p%3Kc4r?+M#6?bwyu?rtMlmFwQfW-!__s2+coo%JoB~Yl6Uu;`z($tSX8J4wsFGJRh&#+cKCa2`yZ`K^O_%-)lMCEhHvBv$J<ngSvC$|p8tI5m^%DE&hSqj5&_%(4!R*qr?fkwUPYPzOYZ+tTupscS({8;@(_@Bjf-qm*BR>yd09n<p7*@8h?9U=>s!;uj332@C3K*@S*X%w$2m(d+PF1^9IzOftIb~}Nyr`C!u2{~S3q|+tj#r7*RHV!NuKiWio%{=E44}bgs$s{E6Ziq$1EYvcJ;BzP?}!kx}Z&AW}iC0)R9TEOQao>&5fK^bVNH=o4K~P;$7x`doDE!bSxT7A9s4cQhHA7{Y-Q&m{si+yKUMC-eko20d)=QXitM#3Kf+$vjf%c(qY}-?uABsns*Z`rD#pm%B1dsd5;z$KU*pf9KrQqHf`8;U*G`d6enkz<Y0%oid@a7eDJ94+12sF)SNg<%Cu#E%$xFD*Io1*$zKm`);f1@5hnmOe%(LN`o*PphT;o(wBT+nY%*#GL0Gri>_yxHtAQ;zM^8&Fe{H2I)8<5z(}ggY5%4W>AUPE8zPCg0^xc~`=PxghyzGW~lj2%_CaHRki56H(C^)q>q2!Rn6TOoR=ck$-{hrUd%N_l(IBq>x906knQ?{L)y%;N`=3K_TBV5ZlCPa|gZ}-pBo_X9dPs3JIn}^v_oZnv$Uo>j(CGXm&+c^D0f0H~C&8_!!#+#|)RNW*~0j*&%J-N-Ur9(D{L8l@>vY~8w)8wGC_j%?=6Lw%2btE}A1}8RAP56paduJ_UaF(BFnVjF{*xze6M`um4$+rqbYjD-oR7ToQp$}<9XMw>4+rf3D8O8WhfVjTn_@ECyHS9#S4qj1W=oS2hUX31gvhynH<~^-F`D2ST-@SRUn5+?II+598)byimhxeWZC&RJHcp(QVeWZ75Z+jQymll-T(seNjCyP*)ev_(;XQ%sL_O8&*(T1nqy|+&-3<)UA`u`DFnB9)DGFOUoD?c?mokHZ;qmsUhE|R<uf4waTe4v4}Ajb&=;_M|4#&wUL8caJ8MRkw1_*s*jrIbZL%6pG?Y})3{fykV}o^5U@9zM4=O^sqMwrtIxa5}{K>}<<AuQ!=EC%1>uVtgC_SDLkA>9}pB*W7S)Y0yGnT;(u)P2WReXHQuY$aF0%3s&~TBbrm?dBqmeVa;}(-Suh&hFkHjc>9xPM;;z?GG8`I?KvkUt#Z$_0CywHe^t6R1<_-e0opgXCk&h^u1~l9p{UVO&vEFnH#ZUjIQZ2-y_aw!p8qXy!W3o4#U6+)=ufl8T2^BEj>^er_tLhDyq}i@6KY4v593h*6q|=^#E{x_3)GN--1wd{zQ*eV<xaSKKk2`rzJ!7TTokGC)ij+DH%B5#Jf56f*h+O;v#a8gEWCNb)t|qmPKKI;_vZZB%-9k_lB_k`*wAZlAo=;^Q4uGdO%LX%*S&6o&{)hlkIsU7jpnAf^d&`7$Uxk269U%8tBth?gcLTt_Q|$w3$Y98TuApsH1n977jkxO946Y9kiEi^VI=f0W!8Q2>RIinQ_|4~3UzI^8}=~!^`JJJ(yo7D-Tz+RgpG9hp%WV@=Y%Vp?qI{YF4PLSwd9QasX?F(0k{$po6@XF^1_Zb``P6Sg}ivqb_#8((Zrs5TGs#X<+E`{E9XmsEEZEXgH3wk9^9cM^{@fw5?UoSc;TJrQ4CL$LZ<K~0m|>EW(-sJ#n6)herrVu^~oK$;tj!o;(!rZ|7n(LB_Gqoe!f?GonEhziTjz90~b<IB;j`k9W4nezevM`g@TqZ^-|O}h%At~r(&?)Zf~a!aGIoQgVffKy7a<mo9*&?wO<}q1VlL}6kFg9wvr4psm|{&F0Y=v{g2mA-oB;0*sIr1zCV8>v?p3t(r*0tu1&p>n3fp7^9TOhnN6F0{g=xhUcG$%<njkg$l51<x_ifZoL)J4g}>JI&IzR(Qmbbwgu)+Q{dhh-gUL&SEs|7g-LNn=*PMl~P3nknT-4SI21TfkIpz*==XcnAxTEHA-_2RnXe5|VfQ-bZ-@<djFpdV=a0lAEwFUWRs(+>I02im=qB{o$wBq2o?Z)YeJR9}#&8+^K8<Kk!gQ2T_vQ-Y`I+x72c1uY}o4tlEH1CKa&-}f&+to&Lji*MOQZj4y$1P7rh)y!%&k3_~>8^B}d!YHHc1a??=B#0j?;M)CQ%{|ftkwSe`hW)1(p>6Fyz|G~r8gWGk=yX#^kDU!SOnttLZ`AUDN~Vduj8ztXfRpu(({-ai-MgGH}Y!$p+_#@kvfgqyy0)CQ7L`xpJ?&&6MgQH`od@&khV(oLN6pO1a{~Ntu|8|eG6J=U{E7k_K2-RpM5i|N3G9Sk&*Py=jY<=rAL_fo_JxTQ<vk-_M>#2mCTuGOwCaO0EZAIejeJ`xJzH~{QWlLn%v1Fbv@{*cO@4&UU+$?3YSAa|AV?<jtcfTc}_R_!L>Iv^W+)vTu6zR<gF0gr98G%jl2laN<BHd1Qz$3EisVddUD*w2T8i^Y!1)p?6&6@Zz!IkmNIbrR3ZF%OY!S*-e8u~)a?i!mWf283#*m5-RxoS<$ZCKT#5(mpbk8h8JRGxboBbDn3-C(%IKm8EqIEWYf6L0&YC-So*rD64((@CYJ`^|Wglr{532ZFuT{Mas&K_RLze_Hr8fB~)pDh_<LhL)GCm|-szqihh=05Nu1!_7?RTx*B_${B5lK;{%m%P)$IVR%@e;fG@frwaisewE`Q5Am0;h)A<_e2I8@v(@BelNpav`+mNHAjg6o=XQYnK)U>+Oa=ZTn3K7@!ipQT;hnO{}d>YBWV*ZMZq@ruQ1eX)7FCaczB7dQqvREDcwW4|k&t#jvJ!v<i`*Uc|XA_{Gmr`1S2-sX25Jg?ajUM=}0g_<!bc7XD~{v2AN3^#HM<b9>lPcxX(b|N8s?NYdcmtd55Zm~C&`r`yfA<xwA$f8yHUim}SYt}9v<c}OAqwi>sPDgZ4DnwqfD`z=_LhO?rzt$@;{%mgtf6gXcvA)mE>>3Sa;rxP~8%TpuB;HlM2e(JremHm?+)0Q(>%v|CcJzkBHoM@w8e*0bL=dfLPsina{8vw<&TcIVdMEcO!a^y)Od+kDKx(uUZ_fhf?QBn2O;>9)kl|mjEug=ja<{)PPs#U;w(k?rKlU?RVIBN*3rDIc9;Ik=LXitnki%d$66X{!yrX6mqEo)8g(rZU*J$PguxI_}|EF!Rsg07>^RGKBKOu0h;uuI#?$*;j{%F+AP=IsA^^6PTB<bUwDFLp2aHK>!WD4L<o+PH};FkDp*Lmru|D9b1|IgRE@+Oo>xyovfgF3Kvd%iPpe-6bY%=$ociC%*<onRNI|QDt3|RZV2#lp@|$X1d5kdDJ&q(hk(TDsQo+zAMXQsNyb9>N?Jh;pcr_UzA^izA3siEy^s*>!OX?a%k|Vq3QCW>P!-wx+zT5M_p0IeLHm2F{^EpJWIPIZTgnK>?^yZ{2F9&+>}F{mVMP%aT-@0o?4Y{l&1NB+sPaJT9`f~Gqh^kA#c0BFQUlgT@%M~@X%F!IXaDR?9#5yBYbDu)mfKDEv}{Lqan{|%30sw=!T*!+P;mls4F2uo2tpOiU-BzdHWB(@!^OgeJ}L^E+UXbo#!C8C@ZR=EAzH7Cat5Y>543hi?%ggT@O*yQlGG>tI7yuku+cW-Y-S+c&9_!v~k**CQ54)<4{fC#-N^VXp6Q?lfEsYo~Sjei#$!!GRo?*>T|5CtcKtE#;L>1uR&9TWs<=}QJkB$O8a<7iz3S6Vi=mXPoU|GD5<GeU!0YNfu6xTK}1cFg5rDro`1UY%&$QXHtvnV1JgL_vmwcv$}~w^#U(Z6Gew=|r76<3Nb54L%091(rfy2SKH&B0!TlXKHu*J(hPDH54oNyhag@-d!Ia4Glx!GcQzm8K^idU6IL#(%tEy?h2vwWKHCUxeAO4zI_YnG)q@)l>QML`Wc5W*g&DV5g(I+5=q)D-aGEQ=kVu|15KC6<B=KfW<<*1-TBzdPYU!|e7<F2LO@Hg)+pD!zyXeQm)84m9Z+ekL7@yH3k#}d%|HF&{(nulcJ7v$k@FyUjDRe=X4loOK_^i$oZeN$DHF;!7xuQ>6nhip!ptj^Me8uOJPH9G19|L^OjZ4zRhA<vWC{IjxPT82%%!Wv3iO-bBS7p86@0vgEPw(c@Wrfi5>+*2OaxY{Jj>y&f{^~}rKw2OM&o{8%;NvbmW=T|e!FyZsSTGEW9WCOArT8R0<)G>&-O|u%S%j1>?Yo;BRYLcpMhc>RWtb+ohX1uH#I{fcnT+4JkX--SZ@l#wTX%1x&4UiN=+NK7oltgOUmso}YJDRG_h630mtMas{pjb1!o}_((|NPD+9VO7wk_uW9(GMu0H6``T?3=WxaSBn}_jyJGN)re>XcD|Y>=PwLndVUU&}UT?x7AQU^Zd>w9i{JKMMOg>(@;m3P*q8jCtcFTMc!96SUNA$tRdNvLg$nz%>|93p--Vn>juk%Y-#hX{gMR!+7(f*Ag!mudh(bqZKwvQ&NKmPi1RK1+yPoB;vveR=ZdD!yDsUgrthGuntCw!rz%s(nz)B_{~a9meVq9uohP&uIvVJiHt916S8#k%)G$-ZI<K0xGXq3=pJq|t;hf4e&Y}5p8gtebT?ZIdLoGinyuW%W(`Ds+ICxx8Q9fbWKs`eA<NzEvDe6lL?K>pUCTWeMN*eH1+~Er2q$z;8OqW0&;)H(ZitZD1PHdmo_^E6<6Sa9h)U<a~4nT)h*W%v|4vlyZG}ZM96hR-A0ByiW5SVR7DT`fH|GpL7qvwKbz9OPa!z}^JCWJ5h7AU0!r$H1KfGTAHt>48hls>dC#p!GCS7h2Y8oCNdHTgX<(O-~~b3qED%vmg&m@uemK+6E4c`<aI=}LSZ<S;ZS7TYHP^Pt~0O9#_J3N%&J;D2or{SJozKF)n|E=l%O#91YDe~JZET?~nvcL0@L1F9juHw`cs;9MN#CD48YJw8AJ6-C)JJ)kEr=D)a<>8>hbuqq+#0l=C?QI^#?IGaYnLp4Aw55TxNZY+UpP5_@Gm<W*0MGMdY8p?6KMP)?A|MM%lPtH|JD@y4nVBM6)L>p2UF;F#i&W$05tF9lwPkCBpV5+LGyQ=PCla*Lw-htmT2!h|mVEKJ=t}<E>t5uq-TtdJl(3t=gLk6svM+xcIx=rw<Kzu-VriVlVT@7VYbOjU#mIkE!yT~~|Cg+M&Y8(-JwSdF;%FL9oANv?ku!TG>GnizsiD0HcWsn}p<2p}>*&7&?km+@l0|@>eb@6>%`1Bk{F)yZI%dErIB-ADyHXw|I6k-mN8X_7vItL-usR5Uh0bRx=Gzr9FhWEpm&FkcM(NFOJMaNOeYa*qQrvu2R$kGTnA80J8$v}e6$IF0@G1dZPUL`$D)u;g`OCWk69P&B>EHMv5@qf6c`!pR#6|afZM%uvU0dfWAfk_!feO$!@%yV#h3v&S$CeS-9>I4R>>0m@=<p2zy07E8Zsr<e*-J|L_s(DGQHvk|KSdb0GO`g_iT!Wf&+)i266{JvC0Fgj_rJya?!Z<ZRHjordHRzc3UtE&XbsUr0hy~gA5W}GCt}2M-Oa-enj-X-jLz}_^CR-`(5-8OHQUD9i^QJ9|280CDuKed0G|R$fxXNr+(eV@jhiTQ9T}_n(LmC%`tZd+oDo(154h>%pteECQn?mzKx#isu$9do2UB82}{VZ@=OifZcVK$3!8X%esK(QVQ80J+2DbNqFqFWf51d_0toXBecduD*OZ3#MyqZSyy{(Xzdy~QL<I7vbhI&R_!fDg=!djLb@#|DZFTn<Abt}Akwk&zvl0<@b9>@ffyGI3M&QS(oGV|X!zznFwHYLZ2a%%Tb=v1xHJB~WSGmHB{W!N!I$2(1ou3R}NuNTEh0hzb@_0|W$355w_GC-X&z$zwk$mbfS*CNDE4nS>?+K(KTs9g--mV0Z$ICeT&j=eCCJk3*}XGBPmCvWzUDI>*w%ZV*Ag=}P&BYa!~P^)PrYi<lWIqOnjhxCQtWm<FmWuF{VB)<;b@WGybPPxBP-%c0@=5>`kKL)J8D-1Pmwx}KcD4=GX*iwMLNwib+-7)D`Q)(}8-Z(#8C83Yg%7Vue{Vs&|Hh`X{pE3>GD<Q|%#`4WTw#(J)fE{ma9ozTM44EGe_xEhcXBwA^J1hXQEi~*5AXQq`YVITHMR<#h+LkVOA00gZ9ET2dJ>fsdZ0w&^tHB2mSpr^3exJ^227)E>90*r!WimV}FXsWy@0D`gz<`^JbisQ?ng`)weWB3y8`O3qw3(5-C4{QfQA67s#<iN^`p)ZDvv^o?((MGtVqR#S!e0Zcu@luhHv`I=FY63W4=U?X;eEeW;E6YC-Idoa3$x0~70^d+)1EJD3r~0X^!4V}H&Z_QUWdJBAuqpZ;nu3&PMjp4kO}=_5#~HOOZ0L>>HnplOi2?HlAe_1`%K-<I)I|h!31g;!_5!Cz9k>h>+ycvYbp&kI<^w)nK#P9;GKzc4C|DcR#N$H;;Sb#i{2NEjKu9!!tw6JqN?Zk?UImp|fUEkfXzMm=$objVP*iPYium`rQs@3S!s|)(zsn!@FD%CM$}zpv|GT_$zhg~ghrYdha(T{JB2qAsiX)@Kehc7mAk4Z;ix_yOg*<_TN!zY%;x;0)-H^>((9jqFoeFkXm72QFe*cQ{X+?F(Gz8`#SvVN5N1uW!$h~Fy1dzrw5H4j5+}wj~3Fk#s0;mo`!m9eX?&B0zGHlZF_phjMD@wAOnX49%Qvm6Ki((3Uq-KD94&!C0p#nhIrNOb3rq9co44X0mT?4PitjmfhO&?aD$DaDfw#zt!8U|SGaq1Ep^HS1Kry3gBkUuc(tEd5;R3v=zoE!s?cvRI11)r5BF&)SrxbI;seE+MDo0VHeN_i15yrC9hO&NM72sVbz1i%1AP|?*GP%GKzP?^mz6fjl5`%qO7KMl=Z`szhY4bn*uvTGgxW&lGbq$%1WF?eWE4n>-E21*&mBWxrCHIPDcS7{D)P}e|Ez;_^|yajYFzjg()hVDd)%(*4At^wQTRoXO=pBW%ZRAYa*m=x$KN{hb5l3H?1QY;Ysly(4_N!-Iq0sDSI4Ss0}+C`q0kP<E6-yAVYcqUzxM{Pweg}NFHtP<QwR~y)IT>-kwfHVPps|v~h>JH*9%^T8F4@1f?En%X@GG&)lN*4hb2BZcWY+xkjR5DCMdus4R0_=~QNwc^BBft_dX;Y@AiGf_A1ZE`gTSEfu<j?=da_QPCxjBK4aZzoQXUVcit8xh$Sug7$8<rsYw1II^q;(xEm;Zipa{lt!7pLv<#LI+IhPb;EH}bSqxjL1Z+PHk6rn1sb;pBt|nbgT2$GCK9pEgHV^5Ts&a2m&>xeB&4<BYraN>@95w`+zzXyvZ9PSGBrG(uXwH~phz)!P!)B`3VEIhB4#Iib8>*M!05c2BMRf)7~aS_k9u1QqUwcgN+z+Bk+=$s6s+A-yVXelg|B#bI;`(ME&gK;vZ|;r_~8(+#KMJ}C#Y9Y2a_sr`m|2GGHdgn?3!Gw^C%xF?VC+ngRaLGJdkOF+7s(&@R#Sw=%@T>&*RC%WD4&VHjKzV46Hk177A9r7QY-h9NvzmaE+-(Kzx{eqL}uC^cfw?~@J*h#ZVs@&ngAs41f3m%>`N?B9e)js%aM%x*-^Kup&Nsr_EcCAUZpWPy$4tvYy&|UX?DUP%0lcVcwA<3>bN5!j<<+juxyrLAMj+=hh>&Ox}S?BcR<kByx@LO+jn@-zeQj&RG-*1k$N>&n&TA)loNrLhF+&|QIdDiTmK1B_2`A~*6jSa%;xjUjwH(f|?_Txf?h3h}dJiFa^(-E{7@4veaqkbPPx@_CN&GlaO2*!3@)iMTQ9@ncq<SGZQnh=9yalap{ODnWl)~`6No%X3+{CT(&^~sY;X6Z&mEO*p|cj}k^rJnj!ebi8{|Em3X_rMee?(V5=C%ta|OZsBTB%*SD{_ylr7Gn1r=}V%%^q1l$k}LZ9PTO*usU5slc`!B5hTcB;@mx-jv#2P!d10Gn*<x-LjCAac<U>+1oK%&$ajy0}-|`><a<9-SOM^{KkyUb)VZWi*V-{j~HA0NJ)^~QuuuVhN&Gjq3EX}&w@PIx?8n(ACb@-C3UU!aDAuP9J|A#GWt(bI)m0a8+DRpnoU%z<r^!%;NK+w)t^G?`2qBfDEcPX^HZNr1qB{QGDkOmZw`v>MAb3-;cY$+(`Vn!+X^qa3!h5nYh0cdH()P!RZym|{7(C(%(^Z@sNEuBnlUpndp&FzDGX(n?tNmvwciaWiLB9EsRl0mEQo-%WN$z=KB<m8R!*L^WRjUM2V%NPZ`_zRCuaryt6BBw2?^x~nh-<Jz1cX5^P+;7iL2AmJ<o~BM~6@n|(UrEO3gJWDI`*87F^9$^3fQ9Yw?q7gwU&k%?btss7=goHWvA>gwHf{meSk{CjyE(l#)kwNfO_j9%NF&X_g<>QbV;gP<mp{J7Ql-w!(M_l=P(zwjWCu}&(}Wjq`jD*zb5-o~(N<mw3M&~~nCP{UR;8)rqGg}jJnML>dbh*v*lq$ln+!^$h+WT^lauG1pv&DjdHnM6^iAm@^7(k&z!%ltbldV1H_|(rDKj-!_d4h=Y_Cx}PsVl-ZCA@byRM(o9QNOjUa^fA3D}Z0of`Pt#8iLE$Z7DDcfkm1z6&W2<I0OAjcwtErB2ICl1)f%-eYq<i-0n#R}AsyQweaH46e0(+Gs~(O#L!xw9}cw0-LkNLwumS+_WEV?YML)%3kAgO}!^cEn{!)rrYy`N~l~zg2D~&aBOdHb3J8i?-V{x;6j!wJmJd2bwI@V%byf$%?7uP((6$w;B0-|+gHyo-#>YCuBSgMmtzX2PjdyH{jRNDU^rbBRY2&ZGz%@vTB=Z{`Se`ExBPy!U%tDL&L`SInu^_Ut_>Z=>#0sIloCCibMG~0c|qz+bJyKyGWWeg=F-)gq>k+$sx^Jz-b#tWn|IHjf4e&jg98!L58X{A67->(n@p4g_w(d(Jn=ueEPQXYg4bM#Gzrgoky$R`(-#*DX|GM)!_4(m-F!UjYHGQv(qU@s>t=i?{vPo%cgcn7lrB@+1AjJnhx(>?71e~|rQGCR|GPihJ}Fws;p%9?P9r~+iB^U4H&USY?SHe9OHTa1Jv!6R<8ewiM_meBB>-3G**PJOq83_8C-5B$I;@$F?NCbFMZnGB<HUBB#bdu$=9}qptK7T}<l$lCFcqH?cRTWmTUBcFarvdrwpAXmG?lG<T9NGGsj&E78aXvx4;xf^X!HXF^E9xKO9x10PTh5Nyi7mSq5eKBUupfXJ70#tay+zxWyC$+@k1;a+m@-S9F^L9E>oRqUA4(EbJnDrX%?<ppPTuhUfK|!90TH=2tRc~9L@LId*gc{NIR+RJDLzlBVTWNu9K6K<3UnX_^X@r=Sz83qFs-SMg!f+l&w4O0d7X6vY3@LQ<qLH>gV3Ns%kwh(mwiJ<7~&fQr#Oj;hzMdKr*lMr;KW*4m9iu@$JElI|r+uMjn}KtCVku=5KjalRddPoBf-2lCzl^+@zfK-AjK!%Gh#O`<-*a*ij|-LC#WZbm_8tgXJ*_lv=T=Pm<2>c47<HE^d|_Wc+S3@4xxYBN~{|jZ?dC?pG*sQlS+KIf2FY8|($J&OW<8i!yX2pY|En-yniUY%U%`JQcy(ay=<dqUO9-)hVIvde*+Z>#05J;2qwuuo=77rlG&i(wOw7ke>sb3n^1wP>0Ee{yR;sn$IOX4StmIjKg7VTO6&qMl}u-YaI#0@E$)&k6kJ4)LKFrXKyQqr`i;qer@L#9nB|oO`+JHQBkA4R>xz@XEBduh*bVCRrY&Rg$3_RUi{>enUs%P$Z7D?1AVDnH)`%L@_B1}Kl;UCIy%Ga@%iNjR(`E<I2+As2ffZ3`#OF;bH&K|+3AGU1*^TVB#t8mn1m?-TExG3aFf%#&q{-?k0N`8WbRjd`a^?Q3^i-Bp~ggxWVKh_pb}8JHjpETc>9KjK+)VJMih>*q@H|Y;nB5t>9|mynvDp}XuoRi^TGW_bZV9~N|}7Aw%3-}1!{I@)4=OlZ!#+m=iNl?EZlqTXC9o1CDZ8{WZMooL?`2o_${YS1$g-GP5{S#>ntIqnd5nxG6hlwS=|G?>h^o>J$z-S3@q%X(#Q?F*lZm}RPVS>G;j_)7j2Mt3ukG!aSvz`3_BwUvs1R=`Sw24=Q?R`!k1;8*_%i^tozALe*W^=^77S^!WS_5J!9OC7g*?eiJfeXzAk++UFjllBf_M$w>DQ1EG?2o-OSQ?NW%qo>SFMUh8?+KWWvJMs<KNtlP>)(iearEDa)t3V)e9Hj-Pp67DEIXk4s|F>~8F^+-Zy?4&jrGawep|@%-&Brop}K#f2*p%$j7b6~66mAm5;EkJ+)<xmk}Kh;gked3n+rb+3zBNQi+R#>YHrqfB-L`g3_mgARusYkgJdp;r=nrWfy|I7tVr$u;w5AJl1ZF-ZRdH_Q(7$6(I-d+}Fo9&YdQJL)#m4+B}P_l5#ZlP_Hh2LIt=b$I358u?N`qixZB#fQP|X>aeYR^!#wOo<(kqdkgHuTnholZN*vX7P~;*$Ju`?>5*XsVvdtGSf?A0#oA&mHz$^+erdUzLtp(oa>(a84G{-s~h-h!H<nCI4hso-py<SgRf|y9`~YVm&OeYoR!Qdwo9;0jMPu0QYTI-Z*jej%3)b+y*tveFUfPIfgiakmcElt+HWZ@VxJn6h}knu?4tJn-#iMbDK!2w;Y_gIM^H5Q_*yZE-kNiYzIu7C^Xmwg@!`&pt+uju!&9A_7-6ulep$?B)q2O~0zT6)VhV+baYP~2h+CLEm4Q4K7jk_bcMeiXLtz=)`qV+}GaDtEKBPtgvA6^4J|t%#FQb^q0%z9X?gh8E5EATF;LwalA1xjwVX|>w;%o!k%1J#pTqI6Lr<9v-y{2EG7Zwu#g$qUKsNL4r4=&a+?mNciSQ~HO_M4(v0EQb7J?HM4hs_ELm62H9M3wuYF6o3k1pf3zQj{Ex^k=Fmlpx!lA{xh0Et$RZzyLRy)TCzWTb^AfL)~m-Z)&Yb6QY>M2G?QkU>Re(T`$eJT*;JXp;gA*7CEY$IbCpblf<GPb<fyl7Cc5t+h99?rHr@I7}al0XbQdYSkj+j`dMelSh&pH{@vN>7YK0eZ2K!0bFfXc{jJG$XfI%Aka3gwSv=`WW0!(&)b^XVpOM*1j~1@Ev-d4Jy6ZouNg4AoPdhv8z*%xMu0n<;OG?ETqU+w|>wsZ8wRcuLhrK_(XSU3&mc{A(hcO&K!eSg3TGqdKpyGaUG!FXh=}i|}TPL(%r;Btgw+O$oTysi_&UHO5<z&>rRo}C9QK@7D(fjT0yz#0;_D7ibsIQeY8~KHMuPB7$W^dZJ(aw98mWI~U2{ngcYTfAIvgJ-1&O-yh<@je|X*Ct>sPG~JY3l$1n|E+0!Al#<bW^U#o}~jKX~&KzpB#xI_FKQ1o%1b7w+^wA?^{^TiXW$Lt1B5CH1ncTFHpZ@t4!CCUXd~kf@Rbw<27}g7KxroV>xYg=#A!5KOWzwJpkIK5$bkG_D)>g-ZV<z>A1Dfocs2%ho6qe8eW3%C5mw0)h3P6B&N(`yBuiY{cEpdZTjVP(ecL_04(lZuMzb=&bo?fsBIw+@q*d&DNAnt?yMR2j!$pi%$eb-U5rleQ!T7bTDLnVEmkAzOzP;R(3cey^`s<KBV4B8I<ucCG4GgWW2oM}{O;8|*hEu%@Hr57+#{OUdEck!an6j+?4$%GtJR&V#|FLT&!b~}*JC%7lnS>g^R)vx9VT{*(P5#sv9C6uvLo6ibBG(`yB0n4g5mDIG8U1(Lof(vkcYE!d}uadc{X7AXuk5%c;(r2Wq-J`Xw3J2v_3*-iiy69i!jbZ`gakgNvKnIanmw5H~5n~7n+tEe0w}D_mm&~@JM7kMOnL-PH0k7OKcC)CHsux#ne2M2PCt>V{>*yRtV^l$-B0Jkz@f#--iX$sdG12?+TVj<5c<M$L-fCn&}U+4Q(edmN~2YLu>ci3yc%P)CyEEDa@e+U|N9*Hb@w%k9N-}*ioCuC)Q;R;uonXbJk>3ri^{@ebFQWWLUON`JIi#4rLBwsB^Ks&RSFA8c7Q`!;u!x_+6T5Q?yylNo(uM_Wb_Cu7R7KOeey}4p(xcMpwQ#w!>&a_s^!@aPB~Cb2wjPVG;8tv1g8*>|ocG?S;L0IEUb<N!RSyKdo#wqQXx5&~GSCp&k9Y5bIfQ?+5`%1953}Cxg3Pvd^P?Ee_wOo+{xutfT{WowMWwgI&2fhbjf^P?*!%8z@hcGy}T_SZqSDQPWx-MzfHvte@$EI3LMceeA7r_N!}>>L>uJVY@y^PvsA%$BU2?X3wbA5IT(6=KWdSu{dnZCs2lD!D#Y^)kt=Ba6sc&#DgsSdT&;3r~_PSWb5cgl{qngr80goz2c<3_Zo{i?|0P$c5`;^_II}jVUa6~mHjc_Hss<49{j?D$vu+;S+p<_06NdtzL|1)HKt%$Lt1TW9D#F??Cizalo!`T+K#;z=|EI4jn-93Xa&W0tEq)1byQSOvH<E(O(?&YEoU!>Vthz*OrHghEEVZ2JN1cm>JO^_eX3&WAmKd$%zEz&HE*W2pd>iS_UA5hlO;rnd|vBF+Zfa_8eNKa3mynk8NqEOA<HA3G03BTyfM^no;D}%Dos0_G(o$`V4AWMHJ{D;gcQO$UmYMO=cAH>V?;R2hFa@kapX&eWP3YxBlFZNf9ZW<);W`Ta!}Jk9)GGXe|IW!Ih3)oaQ(G)G>Mz6x|q*sRRP4J%@1F@$+N#TRCXFC>kX5gE>FHlCAFV<r0$t}<7|1nEL8E`Xp^h#ys~z}^Y><_`ZH6fsjrjsb2`IbMz`vOR7!64rlZaVqE1cidmiIMMxW+IO_`+1$ZqcH#jH=O`Z@3|Q#)7YAlpUti<48v`E(d&FB?B+7|xBB=OH7FrD^2In=h!!pLLLHaL8*rIk8O)X}Ia6L}vrywW+zrU*4YobpG;aq~_zVYvy;R!#g#7^s8F(k!Am-a_z-G>T<XEa#8ByUM(aUK68%;OAwd+Yo~H~W2Q7d0Tt&2w>Ss<eg3nBYwEVMJ%eZQr_1-R*w^m^9?8nPAM>-xKKG|lkerm9+=p{;fp4Y;l{3T63qWw*Ee&TJg7>mfe^Kse1P<7kD~EdSWqWZ7&?j3~Aza~7fg}r3{m~TEV}YvItcm+sRZJ_E^I(U_tgy$}j^bxoonw!C^4al3XPiW9GzPP<8WcDIH^%At4&dtiHx>GE7J1#)p;D?i`@)P}J1nkD9vrUgkJV-8fE-Wf;^q&G*d>n>V^`cJW5tEn!f$arEPF9cW2ISpW9T)=4>co19C)E?OH9*D)lU&Vz0jXnqsj<Q4MMW6cwc(bgPNT>W6Rw&4Rr8H<Y*FODARew?KU5)c0ALobi;<msmrUEh|C6nbkkq(q}XeYA=$YQIuMFSCMvL(=dqN%Ke`MbT$!VG;IsL;KYwW=>*M)EzVGA8jT&>4m1t|OhpufraWuj4I<Br9#mj`#atR=5*}#rDl~u(B|IHhq;i+|jvR=j~PtLP5`aW|8gt(y?Rf0pJK8f?Z$(p7fS}rK7^12<;JnE?BL)|BN)Ravb<)%*Bx{Ih?WRhml&{QVPTOBLPnIrvraFbiZd%Kpels)U<zKdthwK>l%abB}dhDWlMt;tV3lG%E1zHXqYtJl&whm6uFe00VKBz>XTSmt-Taq!|>J8X2uudm!~p5S5Qb!~Q|QYCwrgEBH6)wb04=+AHafBL}a(fQ<29Xz^l=9?V=bnCd|&a*J6>_iJGRX`GX+If%j*_htcCmAX;g@-0>=w@WHC7|Os$)=Mzp5bhaXK1rTslm8EQgP1d)iLW=Q_a#ews9I0#qklQXg6Sy>78_@<veXK?yA+M9f&V}iE~>Dm)kg+%eK7M!NfM5$xc~3%KPLQgLaCAHUYmsO>m?WKVz!<!bm~qx$=f1*wQ`=7DY8lKUFW;`w36btL8)7WDv~*W6orB-b5VoR2h}^@VLC$z&kseS!=`?H=RFu`onZH?~AB_dd%XyUU=j67T$Qhg*RSrF&nRENA0{%s;n-_^H-SCoSvS_(7b7ic=#^7q{)0D!Hx#(=@2<hrF+kDVX-ZMl->Ne<Jo&WcW*Xt4>w1_S}AfM6=WyY{l>sO=kS(6W6K-w`)IwC3N2IfL<@q3)}8)5hHlR8yve<!D%wFjb`?90T!$CW#?YH}K8E+kuKOB{_(LV`&-PoUnzi|K^`{0RkW;T_<JYD%fElJ`nRag`x}-62CdZOSyU$04hyhb@iHB%TA2%@Ox>@O7Bs|7(DCfe2NO6c#uTcxa!#WM?jJ7K!vErJSlJXl@BW_Cz-6S5(OC?{YV2eFFT<8Mk>rzf-^xyNi(4(&Zvoum)X4ruk>oq>3tF_CKBn#Fp8mA@Ia0{^`3C0XvUTGZ+^!M|NpU&@@37*5jR>D)Jl18K`(_zaeC3*Z3#(R7c1BXwx1IHRqe=E`Gu_iXmN~3uytR(G@nyzJC!Zz6AXJm!*ys4AqOKWgB6j$nt8?WTbu3Q$uW4FPlFD{mnwzTAsx3^ExDGy{TdIito>m$K7)ic>aYj*J3eBhcFc$Pv`N~Do|I7vj65?$C&Ud<=hq|*?A4VSv5{??zaSKj6+YJ1hLVcSU+%oR@Z%0kU68I^QpXCpnfQ7|GUhqoweUf96ePB^4EOej}F&cs&OaGD9%k%E$JMIlWtyk9VBYaGw)I5uB{JP={^rab!7h(b-*o)Xt*spKv&ZF8e}ITlpdnz;*gA#>gg?+W1jma<q>i@LEnnx8_mASfAz1&f>^hl3^;oq9#*IT$)B$`ys%stI)|vb#}y?`zZa{1#)E;F5OOA9;F``B#2132pD{sYvUJ!ejgK%8J%2&da$n;}wOm+<mJAp_}%vW4dv!wCX6I`LH?4mf^xHUwXUD8Cyzw=4V`MHoavdTVG>SjBVZ!D+pIU=%QXn(*hw}RfIdiG)-8P^t+S%%d!buywV`n6~CLgLuJ$zjajxu!tSJ`u#(QkB}^?jyIQg;R-+w}V)d47*&+EdUQxoClQ~j=rK7~;mskMlQK;oxn<K}5ar^zV*swr4O5h_X+G@h((t58g$#KdvB_OMJdp1C6B4etjizMzKMRT+mvY`&+w=TvvtA3xlt!T!Cy%)8%6^|Z^)$PHCPdPyBg;q~bo<cd{ZO4~LHB5)wkbRJ$a;U{Zth2O)n8W8Bi?eYQC%VJqiXTa$;E|&^2RUmSCObMpZ?d&=k5_LZ9zhp{Uig=Vy-CXVR3}GahtOuWxN)0qbZ6*NT?7#<CU$VCwtBFX7)rx_treri`>e_cH*lyOt9W_-USzr@2$w-ZZ6oLx?2V<C&5U4V<96u$ZZz!G#y!QsB$s1B=@e0h$Ud~ae@c{~)DraAa{h#u?BZyPh*=MQDOE<cI(jy^i$VMBjF+4Q<z|0UX3{-|kxBNS7*BAjKReunBV<mOms&2cy<4gJa#BWeMnCaqIYC55m(P5{M~OaS%X;}#JPuDHiT;4ymJ%jH?FuB2kiW^V90&yp`FZa))1Bx;a`Z`&aAv;3D47Lrt39Qt&W)(G^ghdEpQzOvQ!A$!YBq{ZkQk>&OqQaiKZl%DzW=<nc}8ABFMq|F!@<`!R~B2}ipN9rsa(!Dy>4&Pg&%LsEzO7<T_BU@%}&`%>}XzU6<!B01-04rek|`dr_H)V-znnfO^S8+Z`M=D19-<ncAc^0+Wkms2xSY}EF&*723y*4LG>-sUO#&62iP)`&M3k(*2huN%i%30#2wGn)<iN3^HlhHK#L6of0V8yb!}`pw{1)%6Wm)h_?B*%vpo*U!}A81Qg)w%W>)-9>@O1*r?Q+$6|luux-r%czb~xy>6}^kMor<<nn_OCabx{x*d2*98oL_Vo+^?MET9`8oGoHnMbl3c;nX3{1L6FF5Dh%y@O{Vc><x5cUKfR<G*Sm(7aAa!hz{uxZ5Jku!Ms9L=?=Bxnh`cZIU`iEIk=sfH<WI{Ne6!u_qJ~^hCG>A#k5$oQYd5_qG@`i%*oa{q>5B#0?eA87Tzvde7Sp;)*@n%_g!3C>r!eFSk96qE;Ey#|K=!UUnSX~4Tm<z#g~R8b6c|6K;Do??JlK&t?$&i-P`fHL?t{R(&e?W$4AS|h-}DiU_YgGgbxGnF{@Jtn`x46NCw>~Q|@Qe-mImfP6p?z@RC9nGJE!cQM}>t7+xI|Jn%77AFA8EVGu6S-rzNN7y~P>X}jb=vu(d<e5&;|Xz5!xg_cO%aPAWpnkA8^5y`$7<1&l%)OidZmBp*8T8bX*#6y{j?IMGtoxS|z^#!AUI~tf=JUl8{2L!v4B*!^kR|@sNesM<d?;Hh8MBxwqJ$}H$V|-<R?<GYHX=bmC@wFUd#>ojsBMwPAJ3)Idp_)wFg~vKxXnuMV6$@$^?lijCVn%<UIguO<-}O>!w5EkOIE|+D$KXCZnpRxH-e_93nf>a>G0SZbJC0PQ3z+hXSS>rVOco1ENu3z{?7<4%FVAFUK6P_hB}bS9yylo~X4;+G6;ckC4d*g;lvE@d%DTCBgW~OEOH0D@Y|m--=VF$V$V!<jX}O4L7u=hHsdGP_=k&qRoZ**8b4p&o*H7q_8KQPR=f5zjH9j8D%Ez{}!&|?8T&tVa8verpt<y1E-RpEQN2RB)lCi9+ntA80D-}0Nr1X^Kt-0lhjkcfq@*q*K@z*qwn%ZAE7-bz~|LjCj$YSxKzqgJ8stk7%LCHXr8}{;srhttA3(pfzp$BhKHfA6J(Wdu9pfs7;6J5$Ob0l&mfl*sP*#QN}Xi_=#QCoCKgzi&6>$vrW2CrG!ws8`d%c{%MWfr&DvW}{5nU;OpCQUqaUG^IdUVqjGuU~k2-ga4)<-i+76fZN7<+84uZkaVn)TTq-Wo^t)|NjBwDI#|",
    "c$|e=ZFAd3lK!q=F;_{|CM7@s2#_GnWUI8XoaoMLtNgNi_sInULlQ9vFc?5G^Xu>Fo}K~Z_3dp{))WcMOZU@HKixCmse5BaohH%4yL9nFo%`<G8C@w;c4z!kcBgZr2OXZrT~n7~Sqw$f9m2;%^}6gHb$_|KdR2eXMtzqiS62`3-afp2fBRa!ee?Q%ZqzSb*H?903|+5IqZt$rV~%P#)<!)YyGCP*cIb<82oy%E_ZQVYwYraLT9+!?O6f2771Ir4-(pF-%b-o5DsAcmM%Jnx)Yw+KS7yNDb$d|7I2^ma{<9eBt_^slZCPV33^W#vYW0`FJv3Y*tOX0a@MK1xiyosk_|wn~swNAZbUW~9YUmW+I$%?cx;0q2`uOqwWt<eT-sefU%wrwqnU2F<zFUNgRIgUWCS5Jl)s1@7sk(i{P}s&nW0$f(SeYhJ_b&rgoZ*8<3|`d-ZD5r{U*IPgu-654r;WK$@4K-twb~U%H~32FMYfA$y<6k|i*>nQl(Ej^JWe*nZdb)=g+<T;x6|5xO*YXg&KF6NY*rg*gJjKCD4Is;qHk(gbzjuY*u%lSJ{2|D<1`M%uF*GFSKoc7?jGw(x21Jejjh2IaFRY2k9F4r5l1XCS*(>A&*!eEt)KKzQ~**AxLI?PVd?$-*fgKYqb@(2Plsd!f0g=7ujnsWAGx+1`<_hIbY;;*Mb(|@<!E2`W(bd+p@&VkAD)UHW|6f4h!(fNoo=y_{YV&}@CQ8H{BFATg<1rQI1V<OmHPPM)!KrMfN4fUFDq-O8k4g>bpIEFcVfJQ|8CU%U{n&Um&>po^hvzU*lOtUi#isSS_k<u4$?eUpId+(1N|}9y@Pg?tT14f%a7z=v&Gy9OZB9&tB2cnAMO!iLx(*947aL07VQD=<LyE13FuFSQN_-XrEF}_+80Y+r8?_A<flwqoXxQtR9T#FysI`?cBRkc$aX*+!1ve!4UaKwFm|VUAWQ#>*xNPTPF0I6jn~y~nQyWxiIXzT%jJGwRjVS+*Q>PL$7!*tmI1HPL<QhK)MZ3}eF6vo40!j`ABfI+59^XsKRvE)&X+)8;9$L4y1;+`+wEexT4Ys*d3JeLCfO#<s$Bu>?~>hSnXOmr-7YPwb&T)wokm2YRjE@11b)raMV1u=36Moa1mZ}J;MR>T;|z2Iup2-K6gNdp2l_fl)D~4`F=26Z0%OseY*{itx$ZNJG`-_vD_Ja6+qL?19xn2_pW@*iS^V6!2FN=%z}+6vj$!d*co?x{ineJ`^8$$5tTV6&tdqpSB3%K*xwBW2FN4KqVS#j`J{&cGMA1R)-GFXr(dWKC71(G`KtiaV@M7B$c<Mhu!$$aqqXAS@C@^pRs3~7y4+>ty`-5hRbKu|z0E@tiCE!yJH|m{(Qt6FP5juc{6%s;K7l#%xT$ieAo6DAluf~#1$LT{zljjABLF7-gZEM#S;5;Kq0h3)*g|dwB{$)6HA)X9<GUHtub+VfW@^2-WLz-5?i53I|jC<g*#u~9V%+_}!rsu`{-qjo*MRlSu=tgEfn<yQt1TK0ekWtK7dj`C25Fqx?spU9(Ot($ZAD+X<XsqJSQDLR`#n=oHeD?^OL^h0MC-9$(ZhVxqy}<~lf&$N)3}*5i>F9lIg-tBI|I)A=AO^$=oG^gJD*D*h=UFiK7)w8WvKIIxbAEbE5XC&*H$(sbe1)J27o30q3Xdni=<S<3RoVps032wwl`rAH5h|RRz{>;{`8ql`fQvz<ITeT)C<jows1v+veJ7Tj49T80#-(iQr-|a*u0Mgk{+vR6vYK~tgw#&J$43qpB$5WWqvVKeTd8(DK?RkFR)7kgW5g4liZcgn4{vb@?(yQTC_jV$K`8cd4r@S~W*oYM2B3OHDH}g|gGs#IsNbek_WJ(s_aEK|yyG8!45sDMAF^WY-1F=^G9-OLwj@XW@S`WWS*m0WBLIzrI!?N10b{Q#h+INWFO-UOFD%j)ecwH?vN(uRFz13;&tiwbWo3kpyrZfb&kW^}*W%=c)t8c7K&rhYG|L$XWG^Wz#)?483sm7G2U*x2;YiS~z-_c6;ux<gX;GCfA=fYwffgDn+KqKX8ne-*2MJ9jYMV1bC}cYU3tKt*d3Ut0PftkE-IH;_HCq8|sE{?3dg{S&fB;QniEEk~I+nI?XQw#u+jGenTP9-hnRuF}0k!vV>%U6q3Eh#ODwvJwiiS_}f{g4`9lkr0A!hIL+Wq@ehH!7XfM$1?$fpy#Uz+dGT6(4ZAzDUkg7FEz8j3apwYn#YrW^&SRkUuaUCYG+Igyg%EE|zaoNhyUo0U1P*r-J%P!=@^JnZg?7Dow3{g>8fX5T&NmXmn2Lx{i+lye3G8%I$^_&>oQN8P}YB=*iE%+%Y5yLY!A?%(_x{)+13?&eh47woUkfylJW{{DZ(mSDh&9<RCSJt*uiD8NYB7~OmDQqU68@vFqKu@aHVjVhU%)TjQU%TfF}k_I1S+JL12{arL%sA*)BK>hOio^tQN<mkx-Yr*Fc;KLL5R03L&T9QC>-jVDL4xx1YO-6sAiUD~ADLiw^9m3~H<jqqJ*GfX58UXW*L=oVH@Uy-|&~4CSHG}JShf<gQcR&HjC@$3w0^Y-jR8D%&Tu!73JqJfrNwF<$&2yvvA-rm}fTX(Tdoai1Y^yD)W<_z>vKeY9I%4*SYQqTVK4|)c@emM)<CNZK^%xY%Z{rRqZY1h%)GLsP*aKUdICP6`V6!M>5IRtdzQ#60qS&#E02pwTu{e!W{%Q$Vgi&NV2ISoLoFW1xVkV?P`ih2w0MJc16E_IaS!!AQ{cqw{5^PZK-AHH=|ND%<bnrrC{RC~Novhy?=OF#1gz%=gwCcf?yi>tL(AF(U@vR6z=Vz`Pg~KW1Vo_(>>{5^>C_7Rzw#%V~_!T9-npDN%fI+!Jf%S;U-F9x&Ke=ciPdoBN0$T;yaup;=CIh>YQeC?m%GYu~uY!D)1)F@c<u*b_uLVh-gJRmtRFbkKDlV#dM_42w1LZftRrI`6g<qM;mLJyXPhI~Ry9<Y21B(pfafh*nkpM`!n0g>wBG?K97#Y|e_I+>*?Cn}cPCk31O%*hu9(d85f+S>EYR)Srf|XQ1R@Flk*L!-1g}s!4yevzFethn>5F{!)^GMCCv7KURhMYZWBVQHz5vWY+fp`Z{(V*%8Q~}N)K;k)SL}XN|OI>Dby9pY8I9srZtrv@L{wkFpCmqtQP^9gUEY`#of=b(Y*w9C5I0!{%YyXD1PG&0XM%~XOnLJH>9&Sr;RaUoz9Y!}>(T^kvJ@7FBZj=D$h1{qI09E&ouC}&6Cj4;7IR_CDDDa~}fU&q0Do&Fi%~HFae8YP|4Jt~|i}vaH&<Y1{SwFfYuy2%Bv@d)zw9fbJ%lc0%q`0b}{4t{#%TG*ZoFoW)By+Z^>R24WrkuHWmzvdQK?=@d7v(EdeP3LRkg^CJs-OvMnjTjKu|~^0e=+#`Pw<ir9fTabV_yp^z@oHDt!p5DY@^~o-@knm%I0h<h$_Dh)dM+;6N@?4XHnAyNm&FfKGZv)b+}wz{o>kk;H&a7d*xV|JfC@fUz}>9f9!_38nOfGNR6$lqFLLEN(moVD&G)N*YPq~W|{QJ7D2ww1*%pVheSmvE|WZo@;D9B#X3sTAWp$>L;xYm{Ul#RNf75rkY)Ha4_0}$RU)Q0S(L32cJvG$TCZm?<Yix`X2otC=9PnMIK!T^qEB8TPuA^d1EtpufAH?7FLs*Ry(j`uxs^<a*a&rm4f)4b1%5DwFa_Y%)kmX!MUU}Ve!cMZO5~bFl%af|v;|{rz2Igw4x?@TC&Vg{VVCIm6PPHlQ7A*H-D=ahZOAu6*EPPE;Hn4=fD$M=cd(N$El6AG;sn3V+C8&LeUU9$o?o<8!}N`Xb2K;Y7Mc)XM`=j2;H44P?6)FIIhC-K_RylTpAi!X0vxfQJ7X(`39L5V!g|%bWTihJYeYdqy_^A>amV@0)>d^tHCH$wth6k9uvTcjMXhKGvXVKkz#BHTMOL#TZsoZKq09oA<qKyy&ixKf6l+KTh<*xTvdUrXM#FVVWV68BRtDO6pc1<!lA<2O#a3D@_AFadJQ9^0i~j4<)V0jj2Z^h%2B*HY=UVBqL4iFP*Aj(+W*Zf{U~Sp;XXsP^66rt5pz^A(KW@|=uTuhp6~k1L&HDw<OET5%x%{IZhw;q{(bCpA^wYe$(A5DBy1M#s)RVF*g&#xT%j6>jTDof+`zZ6>POy2$hb25wy7Yy^lx5ZReDQ%Ajn#+J+7v(>AZX97#9+7k1C3e3&^9^)3p`jC73zsfh3N^3D`DN|8QrVO70ZMigt+-CO{46AWdWi?mQcQ@S<<Bhm=WP$lZ|2XJxeEV30oM?D(b0oKu({XK)5~+b*|wmz}8aEVi8Vhg_}o23VcRXH05aHf~NN#BRwaagRi;O%j({!H*Y_1#Wfv&I#%_aFzONy^iZ~T#fWIN;9I(angY;6!7|Q*Y-O|7M7te#=4MYPixiKo)Bzhyg4H4qHfap2&5o8{b$z;=!DKi(T@+3Oxiw)^NMRdzEnbSGdgpmiXwQmWE7HJih?=8Q`LmS;HnKvBf%$osbgq1|nG=^iay^}y45hi^X&u7Kri^!%!033&0a>E(A-dyM=GgKX*w(Lj&jEn&7VtcJE~1|}LPz>haqql$4Y0@%mM}>))F)yjF!dMmyYu&p377ydhBDdOMop5=#EWUV!AuE-^nu_-)H`u<-$C}*CQDW%f+08ZRT|_Q(WF@x<nh|-(!brkdi(C~-<eU4IegV-nS44rwwz7TYr764|Jc=H{Rx>4dCxugl`fCwtSM@>wI@%4jIFBo+#sV<VLFWa@b<x;VpvO%7M)Je<+R^cJ!Eq34j^OB=yMif>ZO%Lpm>s|-k686pyq&v$;?<%H;zR#H`Or9uHz2zZUjOHt6gt>^NZW?6SX)yUtQUZKDWKPee+T%ln%%LTYN!><Z-;%td`L~qklvnqYu#ms1OMt1zIq_wGiFEjBLK-PKj+h(@737Kl#(ufQq)h=erNg(3FUL?KbOi%uCm^NYV<0`hFl5;r37ATbu!%0DSwVL%_}9>52gv$8~P0XCu`#5+oP&$(puDKBLslN0Bn@uXHx=M(Oy3-AiTL>+AZ6YJRTwX_~QRsY1EGFrR@b-4Ieg+6a$!kdKk?v!ULSlonABP)|FMvwLI9_Nh}YZ8PI>&TW%t9nS4!I$+M2=|+QBX$3Gp&}9mno%SPlJnbw1*59T!G1ER`M*Z}28i$4iM&(X4U}N>}?j@-=$tf<!NweB<7MEO8_*w{9w5q>S-S}1KPg=0sd3XAmj+?}hp(EMC-nH>v5;0dNL>vieu}SeS%}@J3br4z?K0mS9^vsmP%99Z4Ppai+h^yJ(vN@84x5vIKM>(tGpe-1$`@Z{AW2FA@7@ahxtR&E=Ry=AHFs(mlwf8@a*u-h2XZJQDM@&+B%JHW(s%(o^)VN=`JuTbXR^4fAhMKE>+r@JJ2oR<X9FCeH8c<A>j9x00XB}Y$OPPqfGYsLWIcyOgsOyBrj;<5peEb7$xK3umg{9Y4=JxGH=Jk0m%N-$YC-=GLvFln!EYEBcQhe%K`E=9(=}D?u%Y~}uV)@9{BrZ+3QbB4#&hnk>mB}$a#qd*2Cieyv$@h)A$e$8~wstOQQG_C^Dnz`q7LQnFOX=fiaO?E{#17*B+4)*Dm9s+SzBRvfg~mi7zUht;P@9@zvKA<dT3$1ISfw`;ST@brJupz+t*qI3lAzA^_9A!kijAE7p`_(CN^!)3NSu7336?|rx<EI==k0Wo!rS0OV_SR~E8?bLx3(dyAqw#WQ{Zs1g!Vf>OJDiRLlbR#pm-7q<~c~t{33T^#k;Vx`7;H3tLVfXr1xZG)c$sS3sgRutm^$<G(EFtJhizkZy;!>WB1d~t9Z*C>!pJ25sPxS$*-X6aIfh07nXq{$@Whs&W#)iIkKYO8~2Qu&!!dxUw9#ZqY6j<8DNa2>(12TvwFs*1A8ryyE8AaV#?#mI*ubEtUg)OqUnXE(ev*&YkcpHv?@BV#*ZlVZePFt`t8|iA{ne~kX9Bs$^h%M^^%6-3vWs;yvdb{Y>7lLlFF3*C$W5yrc^<Y;I=nh>~%S3k^&)oLGs5#|9bzCgoc;7d@TO#K4dgqwu7>4AXn4)8adz^gI&{7uW4@j3z;EUrLp(18F!YOyXOa4c35{7Z+lD~hbYO>y+{%uu0OC2=<JaoxvnWLblrh3ms&x}eRivpm~@y6LJmVx?FCn{w)B~#vSew#kcbz;x`mdW3YbM4Es|)lh!&|_YK5hInL~%i{@`4i?l!ii7+GhrT!Da5!mi)I`*!UBLNG(*B8y}DVD$YW^PQq7PuKAib`t^ab$>}>!q0S+mggDagAed%pFjPaZDeW_eV=XOX{01}t<Q5os*138+EJ~%Or!6Y$!yYWwe+ijd(#y!;W}F%6+?G+Z%EyJp=+_X5BCv*sR}jy8FP630*bW#d}j9(U#q&c-XFTpkOj7yxciyv*dLOc<-?EHP~Z3;K!ttA5E%TYKjd%?$XO)2&UfAJt=>2}iSnj6X>%_8Z6|k0G8)^x)fRO9uklgfkooLFu{7f6EefB{ub=mwnB=BA%+CNm^%+fc98c8rqSIQJL){V(ww=A{^Mbj{_i}CK*7Lq-<RaR&JOVD1rjWKP;wI=LB^rh}qn|?ovGUV@grVb;kcv-U?Tfffll^+V3-erOVW!t<xLc%YxG$HhbiY}z*856@;a{)*7h>P66#",
    "c$|G#+m74F5q<Ypbl{f+5!K>IGb4?I0LIu}3=*u3g%byP5t{5G)mf6w?QTk=JozK?5+M0Ge@RYtlM-il5o0`~OJAx^ojP^+T6}l25~a0Hn@$DmgfDGR!cl3v(lkZ|V>|Kf>%Xw5?HdY|^+VB^GAmV3jjgj|{_52S^Gsg6UgPEGgYj{@(_VxF2|mbS--}y*cD|#Z2IEMJ;w&_IQ)3n0acqYWdjBA0ZNgzFmWV?B-DK~mtL2A3fBcfYdz8lefxKMjuj4B$X{ig@-Rgdmuh-jr`+x-gqq?(DS{uGlHKL=EyjSildS~@e#yENv4pT7x{{*}coejd92KR`Q(bSo)UVi)fU-20kx5{(@|H*y(`fo4jxd&bb>x1fWUM<uR4%V4}rsP$R2#)ddW*(QO(s(TJQpM;PPG^VufQZdeMMTp{i0)|UBuqklJU*O7h3^6vq_9J$f#?LlQaldMarBJw1bNI1mkq!tMWq^v?Ev7M?*yVpGE@!;5KdDJU3pMlP5KQW5oxU{XZuB5>xU+YNQ@H#3xoh7QShT?lyf>Et2$v2Z%_Dz7{~M?I}JWCl_|$Vm&`cASEe>x*0(-56=ClkWuzH)JGhcj;WwOvv$z5!qB??99;p10X<8ZA0;6zHA>v5SNQdjf&>TrL%GKmUR+;B8I1<Rt=*W&)u+15f{e#Ldu|IW<D7p4JRjuu9#|*U{R-5jK*Fad<D&O62pWb<yuk!m{o^OkD0I~D?&1Su7Kq$|!kw-GP9&`%8K^s+f*7M5>weur_Akt}0?cj`R;&yR5*oH)EV^F^Qk%An%IbR|AUKz*n@;Sx1SKcQQd3@tI1EpJZ8lM_@1+t<HZ)3qx3Qp?d_wVP;HkN?f>j5=%rVn6s&7~}5s_wp22nF@nC$#s+eA%BD@f4I`#C|_wzfaNc$FQ+jC_g6MDX!vynx5QJd;(9%Auc)+Fuv5Z0&_V!iNbSZa;kwAC`Z{0zjmcejEP-x`3xynPytyuqid43;Je}yFRATK?1eq3O-Okuo?h@|<PRmHG}e!jHy*l}lV_?<#JE#kiLX;ej@wTTo%v}X@$^ontvE6&3$BTh&^S^$Y6{XKeY07uur|M0#ebZ-yoqm^W>W+LVdH%b+Wm>B-)t|`1u1VLQ*ST(z1)r4nJEpGNlyAyz=iMwq9Io1De)+NQB%O%#L8)^h=cM6QJAQgOYs8@ApJ#aL}O1`gY=s*`vNRPxT)rd_IoQpE&}?Z8`}7{F|9#QqFJwP>xTkl2t!gt5Z^lH#{Sw;`{Ui1%-93zibQhmswfkqzEAahQN=fNQRD{jlXS+k(KwH&h^fvq<%h~+&R#$izE@CxY?;a!6;qG8xGz*F5Bn3qvL}yLc!-6^g{cB_-liO(5tZpop#6(Uv=~#}_bzq%RG`KxjW+EYg=yJ9oVjNhS3vz%>nj3izii^ptjO9R1H>2=d=in=raWu0k|_lS@V-7ZEO+1xxx#7#iWR6g^xDU;rW>#_wF0lEDN-EHv0~CCO34_x$L?04kx)w=r2y2coNZ_9ig=EbF1i9|gDq{di^KwCwvEc0aL3Ioh53qwSEMsUurG=OC7WaphqaJ`>j!AaK@)$FUhS_uyoA8t_NFK9F1%if4}$~s+|@Y!%*1%n4jzdahu_83&yk*67jQV57t%aNp>>{z3Xh~0qLD^9E@5>=qw~29rP5y{3JV;1vR36eJK6^9n2m|pE|YuVqxq!NN(+%TA;fC%s)4L2pwA29aQS_{fT3#OWIz{LSldWFBz9kPD$xOQw0L0~MghO7QC#*bIujiLHeV@#z_N?j$(eM;n6UI}>Lx-lR;b8@>wMRoz~^w`wn~EYCC=eA8|7HZHi|2QEC~~Z`vWY8ONm06MwoJq!A&eO7E`dEJakl*77JP2&OB{+lu;5cp0#WmW?qX&Cah%3k^@E)I|LNnLh!+g(I(&)`CYz~f0HRtxN6Lao88^5{38D<{~|xjI$ysL2#rN7Y5C|a><Jp2?776WNe9MR0ls|#^pvl2?%JN!tRNH_tbtD-{Q}#!h4|q;hMFx0pJ>h^$wCrMGb0Xko7tRam9GXk>8s&GRb3tRa)$oLj^-A{v}1`AnlYVGLfMzw^m22%^4v;zw)rjMTCY-Mn4>b;vGr>8yXb1Co?+kZeqsGD-ke+E8E#Z3Tgw*Rvl-_|-Y{Q|#twcKfsfvgdzc$0n)>9hDX=WWCx}xv0_bfOPP`r*p{V+-2M3HcY<ql);EaCXc-zF;HJa^_PLo`te^X%43h{n4QJdY#AyL$r0ndsH?`NJgxj7k3%N(x=4;|&zSg<X?9Ll#V02qhb<J0Dr+Y2`S?WZRRFRHWnZWT}NZ1xBcqJg#G*!dR^B5C?#PJlYm^e!ek@q-}~3WKZWM}~`d>DdDmvLqMMqiDE3%Yv|f_)V6H(X?g=H4Vsz+k1Ui=&H=_@F&}>?(=L%tJ`d|R=1n&_P)B^-iR#w<E#Gxz$mDc",
]


class PublicClosureTests(unittest.TestCase):
    def test_complete_current_predecessors_and_scope(self):
        import base64
        import zlib
        from types import SimpleNamespace

        import review_packet
        from test_installed_qualification_v1 import PUBLIC_U
        from test_reporting_qualification_v6 import PUBLIC_SCOPED_SEED, PUBLIC_V, PUBLIC_W, PUBLIC_X
        from test_suite_deadline_issue31 import PUBLIC_G13, PUBLIC_T

        g19, g20, scope = [zlib.decompress(base64.b85decode(v)) for v in PUBLIC_G20_BODIES]
        numbers = [
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
        bodies = [g19, scope] + [
            zlib.decompress(base64.b85decode(v))
            for v in (PUBLIC_X, PUBLIC_SCOPED_SEED, PUBLIC_W, PUBLIC_V, PUBLIC_U, PUBLIC_T, PUBLIC_G13)
        ]

        def record(number, raw):
            return dict(
                id=number,
                body=raw.decode(),
                user=dict(login="Zi-Deng"),
                issue_url="https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
            )

        context = dict(
            designated_plan_comment=record(6074133818, g20),
            issue_comments=[record(n, b) for n, b in zip(numbers, bodies, strict=True)],
        )
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC")
        self.assertEqual(
            review_packet.g20_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(len(numbers)):
            for action in ("missing", "duplicate", "body", "owner", "issue", "bool"):
                changed = copy.deepcopy(context)
                rows = changed["issue_comments"]
                if action == "missing":
                    rows.pop(index)
                elif action == "duplicate":
                    rows.append(copy.deepcopy(rows[index]))
                elif action == "body":
                    rows[index]["body"] += " "
                elif action == "owner":
                    rows[index]["user"]["login"] = "other"
                elif action == "issue":
                    rows[index]["issue_url"] = "other"
                else:
                    rows[index]["id"] = True
                with self.subTest(index=index, action=action), self.assertRaises(WorkflowError):
                    review_packet.g20_predecessors(repo, changed)
        for field, value in (
            ("body", g20.decode() + " "),
            ("user", {"login": "other"}),
            ("issue_url", "other"),
        ):
            changed = copy.deepcopy(context)
            changed["designated_plan_comment"][field] = value
            with self.assertRaises(WorkflowError):
                review_packet.g20_predecessors(repo, changed)


class PublicCompletionTests(unittest.TestCase):
    setUp = PublicCatalogTests.setUp
    binding = PublicCatalogTests.binding
    catalog = PublicCatalogTests.catalog

    def test_public_url_whole_owners_edges_and_scope(self):
        url = "https://github.com/Zi-Deng/FLOW-DC/pull/32#issuecomment-71"
        (self.root / "whole-responses").mkdir()
        (self.root / "whole-responses/71.txt").write_bytes(b"whole\nresponse\n")
        (self.root / "source.txt").write_bytes(b"source\n")
        rows = [
            dict(
                id="a" * 24,
                path="scripts/agentic/review.py",
                artifact="source.txt",
                kind="source",
                start_line=1,
                end_line=1,
                bytes=7,
                links=[url],
            ),
            dict(
                id="b" * 24,
                path="pr_comments:71",
                artifact="whole-responses/71.txt",
                kind="finding",
                start_line=1,
                end_line=2,
                bytes=15,
                links=[],
            ),
            dict(
                id="c" * 24,
                path="integration",
                artifact="source.txt",
                kind="cross-boundary",
                start_line=1,
                end_line=1,
                bytes=7,
                links=[],
            ),
        ]
        context = {"pr_comments": [dict(id=71, html_url=url, body="whole\nresponse\n")]}
        catalog = public.partition(self.root, rows, self.binding())
        originals = copy.deepcopy(rows)
        public.add_relations(self.root, rows, catalog, context=context)
        proof = json.loads((self.root / "public-catalog-relations.json").read_text())
        self.assertEqual(proof["edges"], [[0, 1]])
        self.assertEqual(proof["cross_part_edges"], [[0, 1]])
        self.assertEqual(
            proof["public_references"],
            [dict(source="a" * 24, url=url, classification="retained-primary", targets=["b" * 24])],
        )
        for changed in (
            {"pr_comments": []},
            {"pr_comments": context["pr_comments"] * 2},
            {"pr_comments": [{**context["pr_comments"][0], "body": "changed"}]},
        ):
            with self.assertRaises(WorkflowError):
                public.add_relations(self.root, copy.deepcopy(originals), catalog, context=changed)
        partial = copy.deepcopy(originals)
        partial[1]["end_line"] = 1
        with self.assertRaises(WorkflowError):
            public.public_reference(self.root, partial, context, url)
        for external, classification in (
            ("https://example.org/paper", "external-citation"),
            ("https://github.com/Zi-Deng/FLOW-DC/pull/30#issuecomment-71", "outside-retained-scope"),
        ):
            self.assertEqual(
                public.public_reference(self.root, originals, context, external), (classification, [])
            )
        with self.assertRaises(WorkflowError):
            public.public_reference(self.root, originals, context, url.replace("71", "72"))
        with self.assertRaises(WorkflowError):
            public.public_reference(self.root, originals, context, url.replace("issuecomment-71", "unknown"))

    def test_final_guard_actual_git_task_and_all_returns(self):
        import ast
        import subprocess
        from types import SimpleNamespace

        from test_reporting_qualification_v6 import G17_STATE, G19_APPROVAL, G19_HISTORY, G20_APPROVAL

        root = self.root / "git"
        root.mkdir()

        def git(*args):
            return subprocess.run(
                ["git", "-C", str(root), *args], capture_output=True, check=True, timeout=10
            ).stdout

        git("init", "-q")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.invalid")
        source = root / "source.py"
        source.write_text("value = 1\n")
        git("add", "source.py")
        git("commit", "-qm", "fixture")
        task = root / ".agentic-local/tasks/issue-31.json"
        task.parent.mkdir(parents=True)
        state = copy.deepcopy(G17_STATE)
        state.update(
            contract_generation=20,
            approval=copy.deepcopy(G20_APPROVAL),
            approval_history=copy.deepcopy(G19_HISTORY + [G19_APPROVAL]),
        )
        task.write_text(json.dumps(state))
        repo = SimpleNamespace(root=root, main=root, name="Zi-Deng/FLOW-DC")
        initial = public.catalog_observation(repo)
        for payload in (
            {"plan": {}, "metadata": {}},
            {"schema_version": 10},
            {"components": [], "items": [], "files": {}, "dependencies": {}},
        ):
            self.assertIs(public.final_catalog_result(repo, initial, payload), payload)
            source.write_text("value = 2\n")
            with self.assertRaises(WorkflowError):
                public.final_catalog_result(repo, initial, payload)
            source.write_text("value = 1\n")
            task.write_text(json.dumps({**state, "unrelated_fixture_field": "changed"}))
            with self.assertRaises(WorkflowError):
                public.final_catalog_result(repo, initial, payload)
            task.write_text(json.dumps(state))
            bad = copy.deepcopy(state)
            bad["approval_history"].reverse()
            task.write_text(json.dumps(bad))
            with self.assertRaises(WorkflowError):
                public.final_catalog_result(repo, initial, payload)
            task.write_text(json.dumps(state))
        tree = ast.parse(Path(public.__file__).read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "catalog")
        returns = [n for n in ast.walk(function) if isinstance(n, ast.Return)]
        self.assertEqual(len(returns), 3)
        self.assertTrue(
            all(
                isinstance(n.value, ast.Call)
                and isinstance(n.value.func, ast.Name)
                and n.value.func.id == "final_catalog_result"
                for n in returns
            )
        )

    def test_consumer_dispatch_mutation_matrix(self):
        from unittest.mock import patch

        import review
        import review_report_material_v1 as material

        plan = public.plan_catalog(self.catalog())
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
                contract_digest=authority.G20_CONTRACT_DIGEST,
                plan=changed,
                authorization=funding,
                unit_policy=policy,
                admission={},
                application=applied,
            )
            (self.root / "batch.json").write_text(json.dumps(batch))
            with patch.object(review, "verify_packet", return_value={}), self.assertRaises(WorkflowError):
                windows.replay_prefix(None, self.root, changed, 0)
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

    def test_native_aggregate_exact_and_plus_one(self):
        import review_capacity_native_v1 as capacity

        deps = {**self.binding(), "assignments": "0" * 64}
        extra = [dict(id="f" * 24, artifact="extra.txt", start_line=1, end_line=1)]
        files = {"extra.txt": b"x"}

        def component(n, size, lines, count=1):
            raw = b"x" * (size - lines) + b"\n" * lines
            rows = [
                dict(id=f"{n * 1000 + i + 1:024x}", artifact=f"{n}.txt", start_line=i + 1, end_line=i + 1)
                for i in range(count - 1)
            ]
            rows.append(
                dict(id=f"{n * 1000 + count:024x}", artifact=f"{n}.txt", start_line=count, end_line=lines)
            )
            return dict(id=f"unit-{n:02}", items=rows, files={f"{n}.txt": raw})

        def admit(components):
            return capacity.largest_fixture_public_catalog_v1(components, extra, files, deps)

        cases = [
            (
                [component(n, 500000 - (n == 0), 9000) for n in range(24)],
                lambda rows: rows.__setitem__(0, component(0, 500000, 9000)),
            ),
            (
                [component(n, 400000, 8800 - (n == 24)) for n in range(25)],
                lambda rows: rows.__setitem__(24, component(24, 400000, 8800)),
            ),
            (
                [component(n, 10000, 100, 99 if n == 23 else 100) for n in range(24)],
                lambda rows: rows.__setitem__(23, component(23, 10000, 100, 100)),
            ),
        ]
        for rows, overflow in cases:
            result = admit(rows)
            self.assertIn("catalog_sha256", result["manifest"]["dependencies"])
            expected = [
                {
                    "id": row["id"],
                    "items": row["items"],
                    "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in row["files"].items()},
                }
                for row in rows
            ]
            expected_bytes = json.dumps(
                expected, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
            self.assertEqual(
                result["manifest"]["dependencies"]["catalog_sha256"],
                hashlib.sha256(expected_bytes).hexdigest(),
            )
            overflow(rows)
            with self.assertRaises(WorkflowError):
                admit(rows)
        rows = [component(0, 10000, 100)]
        for key in deps:
            bad = dict(deps)
            bad.pop(key)
            with self.assertRaises(WorkflowError):
                capacity.largest_fixture_public_catalog_v1(rows, extra, files, bad)
        with self.assertRaises(WorkflowError):
            admit(rows * 49)
        for size, lines, count in ((500001, 9000, 1), (500000, 9001, 1), (10000, 129, 129)):
            with self.assertRaises(WorkflowError):
                admit([component(0, size, lines, count)])

    def test_each_retained_public_surface_and_owner_ambiguity(self):
        (self.root / "whole-responses").mkdir()
        for surface, fragment in (("reviews", "pullrequestreview-71"), ("inline_comments", "discussion_r71")):
            url = "https://github.com/Zi-Deng/FLOW-DC/pull/32#" + fragment
            artifact = "whole-responses/" + surface + ".txt"
            (self.root / artifact).write_bytes(b"body\n")
            row = dict(id="a" * 24, path=surface + ":71", artifact=artifact, start_line=1, end_line=1)
            context = {surface: [dict(id=71, html_url=url, body="body\n")]}
            self.assertEqual(
                public.public_reference(self.root, [row], context, url), ("retained-primary", ["a" * 24])
            )
            with self.assertRaises(WorkflowError):
                public.public_reference(self.root, [row, {**row, "id": "b" * 24}], context, url)
            context[surface][0]["id"] = True
            with self.assertRaises(WorkflowError):
                public.public_reference(self.root, [row], context, url)
        for number, artifact, raw in (
            (6074133818, "plan.txt", b"contract\n"),
            (6074219127, "contract-predecessor-6074219127.txt", b"contract"),
        ):
            url = f"https://github.com/Zi-Deng/FLOW-DC/issues/31#issuecomment-{number}"
            (self.root / artifact).write_bytes(raw)
            row = dict(id="c" * 24, artifact=artifact, start_line=1, end_line=1)
            context = dict(
                designated_plan_comment={"id": 6074133818},
                issue_comments=[dict(id=number, html_url=url, body="contract")],
            )
            self.assertEqual(
                public.public_reference(self.root, [row], context, url), ("retained-primary", ["c" * 24])
            )
            with self.assertRaises(WorkflowError):
                public.public_reference(self.root, [], context, url)

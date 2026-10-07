"""Cheap standalone exact-report fixtures; no provider or qualification credit."""

import copy
import hashlib
import json
import unittest

import review_report_material_v1 as material


def report(text="", *, multiline=False):
    value = {
        "schema_version": 2,
        "inventory_sha256": "a" * 64,
        "reviewed": ["b" * 24],
        "incomplete": [],
        "findings": [],
        "limitations": [text],
    }
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    if multiline:
        raw = raw[:1] + b"\n" * (10_000 - len(raw)) + raw[1:]
    return raw


def dependency(raw):
    return {
        **dict.fromkeys(material.DEPENDENCY_FIELDS, "c" * 64),
        "review_sha256": hashlib.sha256(raw).hexdigest(),
    }


class WholeReportMaterialTests(unittest.TestCase):
    def test_multiline_base_boundary_becomes_one_complete_obligation(self):
        raw = report(multiline=True)
        self.assertEqual(len(raw), 10_000)
        self.assertGreater(len(raw.splitlines()), 9_000)
        item, projection = material.material(raw, "component-reports/component-0001.txt", dependency(raw))
        self.assertEqual(len(item["id"]), 24)
        self.assertEqual(item["start_line"], 1)
        self.assertEqual(item["end_line"], 79)
        self.assertEqual(item["whole_report_projection"]["end_byte"], len(raw))
        self.assertEqual(material.reconstruct(projection), raw)
        material.verify(raw, projection, item, dependency(raw))

    def test_layouts_preserve_every_byte_and_safe_native_lines(self):
        for text in ("", "\\" * 2000, '"' * 2000, "\u0085\u2028\u2029" * 500, "\U00010000" * 2000):
            with self.subTest(layout=repr(text[:4])):
                raw = report(text)
                projection = material.render(raw)
                self.assertEqual(material.reconstruct(projection), raw)
                self.assertLessEqual(len(projection), 31_343)
                self.assertLessEqual(len(projection.splitlines()), 79)
                self.assertEqual(len(projection.splitlines()), len(projection.decode().splitlines()))
                self.assertTrue(all(len(row) < 900 for row in projection.splitlines()))
        raw = b"```json\r\n" + report() + b"\n```\r\n"
        self.assertEqual(material.reconstruct(material.render(raw)), raw)

    def test_offset_canonical_unicode_truncation_and_order_refuse(self):
        raw = report("\U00010000" * 80)
        projection = material.render(raw)
        rows = projection.splitlines(keepends=True)
        first = json.loads(rows[0])
        changed = [
            projection[:-1],
            b"".join(reversed(rows)),
            b"".join(rows[1:]),
            rows[0] + projection,
            json.dumps([True, first[1], first[2]]).encode() + b"\n" + b"".join(rows[1:]),
            json.dumps([0, first[1] + 1, first[2]]).encode() + b"\n" + b"".join(rows[1:]),
            projection.replace(b"[0,", b"[0, ", 1),
            projection.replace(b"[0,", b"[0.0,", 1),
            projection.replace(b"[0,", b"[1,", 1),
            projection + b"\n",
        ]
        for altered in changed:
            with self.subTest(altered=altered[:20]), self.assertRaises(ValueError):
                material.reconstruct(altered)

    def test_independent_dependency_and_every_descriptor_field_bind(self):
        raw = report()
        dep = dependency(raw)
        item, projected = material.material(raw, "component-reports/u-1.txt", dep)
        material.verify(raw, projected, item, dep)
        for field in material.DEPENDENCY_FIELDS:
            changed = {**dep, field: "d" * 64}
            with self.subTest(dependency=field), self.assertRaises(ValueError):
                material.verify(raw, projected, item, changed)
        mutations = {"id": "d" * 24, "start_line": True, "end_line": 1, "bytes": 0, "links": ["x"]}
        for key, value in mutations.items():
            with self.subTest(field=key), self.assertRaises(ValueError):
                material.verify(raw, projected, {**item, key: value}, dep)
        for key in item["whole_report_projection"]:
            changed = copy.deepcopy(item)
            changed["whole_report_projection"][key] = None
            with self.subTest(binding=key), self.assertRaises(ValueError):
                material.verify(raw, projected, changed, dep)
        with self.assertRaises(ValueError):
            material.verify(raw + b" ", projected, item, dep)
        with self.assertRaises(ValueError):
            material.verify(raw, projected[:-1], item, dep)

    def test_schema_bounds_paths_and_extra_fields_refuse(self):
        for raw in (b"", b"x" * 10_001, b"\xff", b"{}", b'{"schema_version":2,"schema_version":2}'):
            with self.subTest(raw=raw[:10]), self.assertRaises(ValueError):
                material.render(raw)
        raw = report()
        for path in (
            "/report.txt",
            "../report.txt",
            "component-reports/../x.txt",
            "component-reports/x\n.txt",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                material.material(raw, path, dependency(raw))
        for dep in (
            {},
            {**dependency(raw), "extra": "c" * 64},
            {**dependency(raw), "execution_sha256": True},
        ):
            with self.assertRaises(ValueError):
                material.material(raw, "component-reports/u-1.txt", dep)

    def test_all_reports_and_old_projection_storage_count(self):
        projection = material.render(report(multiline=True))
        projections = [projection] * 48
        expected = sum(map(len, projections))
        self.assertLessEqual(expected, 1_504_464)
        self.assertEqual(material.packet_budget(projections, 2_000_000 - expected), 2_000_000)
        with self.assertRaises(ValueError):
            material.packet_budget(projections, 2_000_001 - expected)
        with self.assertRaises(ValueError):
            material.packet_budget(projections + [projection], 0)
        with self.assertRaises(ValueError):
            material.packet_budget(projections, True)

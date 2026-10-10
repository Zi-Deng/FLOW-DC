from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmark.tool_study import execute_cell, first_attempt


class ToolStudySafetyTests(unittest.TestCase):
    def test_changed_source_refuses_before_origin_or_native_process(self):
        with tempfile.TemporaryDirectory() as temporary, patch('benchmark.tool_study.subprocess.check_output') as probe:
            with self.assertRaisesRegex(ValueError, 'acquisition source differs'):
                execute_cell(Path(temporary) / 'attempt', {}, 4,
                             {'source_files_sha256': {'benchmark/study.py': '0' * 64}}, '/unused')
            probe.assert_not_called()
            self.assertFalse((Path(temporary) / 'attempt').exists())

    def test_missing_tool_first_attempt_retains_planned_identity(self):
        cell = {'scenario': 'mixed-sizes', 'block': 3, 'method': 'img2dataset-1.47.0', 'cell_id': 'missing'}
        with tempfile.TemporaryDirectory() as temporary:
            result = first_attempt(Path(temporary), cell, {}, 4)
        self.assertEqual(result['status'], 'missing')
        self.assertEqual(result['cell_id'], 'missing')
        self.assertNotIn('goodput', result)


if __name__ == '__main__':
    unittest.main()

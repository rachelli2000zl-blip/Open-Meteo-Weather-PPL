import unittest

from meteo_warehouse.contract_fingerprint import (
    LEGACY_COMPATIBLE_SHA256,
    LEGACY_SOURCE_SHA256,
    fingerprints_match,
    implementation_fingerprint,
)


class FingerprintTest(unittest.TestCase):
    def test_formatting_comments_and_reviewed_import_cleanup_are_equivalent(self):
        original = "from datetime import UTC, date, datetime\nvalue = date(2026, 1, 1)\n"
        cleaned = "# A comment\nfrom datetime import date\n\nvalue=date(2026,1,1)\n"
        self.assertEqual(implementation_fingerprint(original), implementation_fingerprint(cleaned))

    def test_constant_f_string_cleanup_preserves_fingerprint(self):
        self.assertEqual(
            implementation_fingerprint('query = f"SELECT 1"'), implementation_fingerprint('query = "SELECT 1"')
        )

    def test_sql_logic_and_used_import_changes_are_detected(self):
        cases = [
            ('query = "SELECT 1"', 'query = "SELECT 2"'),
            ('query = "SELECT 1"', 'query = "SELECT  1"'),
            ("value = 1", "value = 2"),
            ('query = f"SELECT {value}"', 'query = "SELECT {value}"'),
            (
                "from datetime import date\nvalue = date.today()",
                "from datetime import datetime as date\nvalue = date.today()",
            ),
            ("from plugin import register\nvalue = 1", "value = 1"),
            ("import plugin\nvalue = 1", "value = 1"),
        ]
        for original, changed in cases:
            with self.subTest(original=original, changed=changed):
                self.assertNotEqual(implementation_fingerprint(original), implementation_fingerprint(changed))

    def test_legacy_bridge_only_accepts_the_reviewed_pair(self):
        self.assertTrue(fingerprints_match(LEGACY_SOURCE_SHA256, LEGACY_COMPATIBLE_SHA256))
        self.assertFalse(fingerprints_match(LEGACY_SOURCE_SHA256, "different-implementation"))
        self.assertFalse(fingerprints_match("unknown-legacy-hash", LEGACY_COMPATIBLE_SHA256))
        self.assertTrue(fingerprints_match("same", "same"))

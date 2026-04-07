import json
import tempfile
import unittest
from pathlib import Path

from babblebox.shield_link_safety import (
    DEFAULT_EXTERNAL_MALICIOUS_PATHS,
    ShieldLinkSafetyEngine,
    _load_bundled_intel,
    domain_in_set,
)


class _CountingContainer:
    def __init__(self, members):
        self.members = set(members)
        self.lookups = []

    def __contains__(self, value):
        self.lookups.append(value)
        return value in self.members


class ShieldLinkSafetyLookupTests(unittest.TestCase):
    def test_default_external_feed_paths_include_explicit_large_feed_chunks(self):
        self.assertEqual(
            tuple(path.name for path in DEFAULT_EXTERNAL_MALICIOUS_PATHS),
            (
                "malicious_links.txt",
                "full-domains-aa.txt",
                "full-domains-ab.txt",
                "full-domains-ac.txt",
                "malicious_files",
                "malicious_files.txt",
            ),
        )

    def test_domain_in_set_checks_only_domain_candidate_chain(self):
        hit = _CountingContainer({"bad.example.com"})

        self.assertTrue(domain_in_set("cdn.bad.example.com", hit))
        self.assertEqual(hit.lookups, ["cdn.bad.example.com", "bad.example.com"])

        miss = _CountingContainer(set())

        self.assertFalse(domain_in_set("cdn.bad.example.com", miss))
        self.assertEqual(miss.lookups, ["cdn.bad.example.com", "bad.example.com", "example.com"])

    def test_external_malicious_feeds_merge_and_report_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bundled_path = root / "shield_link_intel.json"
            external_one = root / "malicious_links.txt"
            external_two = root / "malicious_files.txt"

            bundled_path.write_text(
                json.dumps(
                    {
                        "intel_version": "test",
                        "source": "bundled",
                        "malicious_domains": ["dlscord-gift.com"],
                    }
                ),
                encoding="utf-8",
            )
            external_one.write_text(
                "https://www.bad-one.example/claim\nbad-two.example\nnot a host\n\n",
                encoding="utf-8",
            )
            external_two.write_text(
                "bad-two.example\nhttps://cdn.bad-three.example/path\n#comment\n",
                encoding="utf-8",
            )

            intel = _load_bundled_intel(
                bundled_path,
                external_malicious_paths=(external_one, root / "missing.txt", external_two),
            )
            engine = ShieldLinkSafetyEngine(intel=intel)
            diagnostics = engine.diagnostics()

        self.assertEqual(intel.bundled_malicious_domains, frozenset({"dlscord-gift.com"}))
        self.assertEqual(
            intel.external_malicious_domains,
            frozenset({"bad-one.example", "bad-two.example", "cdn.bad-three.example"}),
        )
        self.assertEqual(
            intel.malicious_domains,
            frozenset({"dlscord-gift.com", "bad-one.example", "bad-two.example", "cdn.bad-three.example"}),
        )
        self.assertEqual(intel.external_malicious_skipped_lines, 2)
        self.assertEqual(intel.external_malicious_load_errors, ())
        self.assertEqual(
            intel.external_malicious_source_paths,
            (str(external_one.resolve()), str(external_two.resolve())),
        )
        self.assertEqual(diagnostics["intel_source"], "bundled+external")
        self.assertEqual(diagnostics["bundled_malicious_domains"], 1)
        self.assertEqual(diagnostics["external_malicious_domains"], 3)
        self.assertEqual(diagnostics["effective_malicious_domains"], 4)
        self.assertEqual(diagnostics["external_malicious_skipped_lines"], 2)
        self.assertEqual(
            diagnostics["external_malicious_source_paths"],
            [str(external_one.resolve()), str(external_two.resolve())],
        )

    def test_external_subdomain_entry_blocks_descendants_not_parent_domain(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bundled_path = root / "shield_link_intel.json"
            external_path = root / "malicious_files"

            bundled_path.write_text(
                json.dumps(
                    {
                        "intel_version": "test",
                        "source": "bundled",
                        "malicious_domains": [],
                    }
                ),
                encoding="utf-8",
            )
            external_path.write_text("login.bad.example\n", encoding="utf-8")

            intel = _load_bundled_intel(bundled_path, external_malicious_paths=(external_path,))

        engine = ShieldLinkSafetyEngine(intel=intel)
        assess_kwargs = {
            "path": "/",
            "query": "",
            "message_text": "",
            "squashed_text": "",
            "has_suspicious_attachment": False,
            "allowlisted": False,
            "now": 0.0,
        }

        exact = engine.assess_domain("login.bad.example", **assess_kwargs)
        descendant = engine.assess_domain("cdn.login.bad.example", **assess_kwargs)
        parent = engine.assess_domain("bad.example", **assess_kwargs)

        self.assertEqual(exact.category, "malicious")
        self.assertIn("external_malicious_domain_exact", exact.matched_signals)
        self.assertEqual(descendant.category, "malicious")
        self.assertIn("external_malicious_domain_family", descendant.matched_signals)
        self.assertEqual(parent.category, "unknown")
        self.assertFalse(parent.provider_lookup_warranted)

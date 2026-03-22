import types
import unittest

from babblebox.cogs.afk import AfkCog


class AfkCogTests(unittest.TestCase):
    def test_resolve_afk_reason_uses_quick_preset_aliases(self):
        cog = AfkCog(types.SimpleNamespace())

        self.assertEqual(cog._resolve_afk_reason("sleeping", None), "💤 Sleeping")
        self.assertEqual(cog._resolve_afk_reason("Deep work block", "working"), "💼 Working - Deep work block")


import types
import unittest

from babblebox.cogs.afk import AfkCog
from babblebox.utility_helpers import AFK_QUICK_REASONS, get_afk_preset_default_duration


class AfkCogTests(unittest.TestCase):
    def test_resolve_afk_payload_uses_quick_preset_aliases(self):
        cog = AfkCog(types.SimpleNamespace())

        chosen_preset, resolved_reason = cog._resolve_afk_payload("sleeping", None)
        self.assertEqual(chosen_preset, "sleeping")
        self.assertEqual(resolved_reason, f"{AFK_QUICK_REASONS['sleeping']['emoji']} Sleeping")

        chosen_preset, resolved_reason = cog._resolve_afk_payload("Deep work block", "working")
        self.assertEqual(chosen_preset, "working")
        self.assertEqual(resolved_reason, f"{AFK_QUICK_REASONS['working']['emoji']} Working - Deep work block")

    def test_apply_default_preset_duration_prefers_explicit_duration(self):
        cog = AfkCog(types.SimpleNamespace())

        self.assertEqual(cog._apply_default_preset_duration("sleeping", None), get_afk_preset_default_duration("sleeping"))
        self.assertEqual(cog._apply_default_preset_duration("sleeping", 1800), 1800)

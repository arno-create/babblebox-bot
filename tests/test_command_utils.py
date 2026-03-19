import unittest

from babblebox.command_utils import defer_hybrid_response, send_hybrid_response


class FakeMessage:
    pass


class FakeResponse:
    def __init__(self):
        self._done = False
        self.defer_calls = []

    def is_done(self):
        return self._done


class FakeInteraction:
    def __init__(self, *, expired: bool = False):
        self.response = FakeResponse()
        self._expired = expired

    def is_expired(self):
        return self._expired


class FakeContext:
    def __init__(self, interaction=None):
        self.interaction = interaction
        self.send_calls = []
        self.defer_calls = []

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage()

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        if self.interaction is not None:
            self.interaction.response.defer_calls.append(kwargs)
            self.interaction.response._done = True


class CommandUtilsTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_hybrid_response_uses_prefix_send_for_prefix_context(self):
        ctx = FakeContext()
        message = await send_hybrid_response(ctx, content="hi", ephemeral=True, delete_after=5.0)
        self.assertIsInstance(message, FakeMessage)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["content"], "hi")
        self.assertEqual(ctx.send_calls[0]["delete_after"], 5.0)

    async def test_send_hybrid_response_routes_through_context_send_for_interactions(self):
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction)
        message = await send_hybrid_response(ctx, content="hello", ephemeral=True)
        self.assertIsInstance(message, FakeMessage)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["content"], "hello")
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_defer_hybrid_response_defers_pending_interactions(self):
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction)
        deferred = await defer_hybrid_response(ctx, ephemeral=True)
        self.assertTrue(deferred)
        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertTrue(ctx.defer_calls[0]["ephemeral"])

    async def test_defer_hybrid_response_is_noop_for_prefix_or_completed_interaction(self):
        prefix_ctx = FakeContext()
        self.assertFalse(await defer_hybrid_response(prefix_ctx, ephemeral=True))

        interaction = FakeInteraction()
        interaction.response._done = True
        slash_ctx = FakeContext(interaction=interaction)
        self.assertFalse(await defer_hybrid_response(slash_ctx, ephemeral=True))
        self.assertEqual(slash_ctx.defer_calls, [])

    async def test_defer_hybrid_response_skips_expired_interactions(self):
        interaction = FakeInteraction(expired=True)
        ctx = FakeContext(interaction=interaction)
        self.assertFalse(await defer_hybrid_response(ctx, ephemeral=True))
        self.assertEqual(ctx.defer_calls, [])

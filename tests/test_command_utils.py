import unittest

from babblebox.command_utils import defer_hybrid_response, send_hybrid_response


class FakeMessage:
    pass


class FakeResponse:
    def __init__(self):
        self._done = False
        self.send_calls = []
        self.defer_calls = []

    def is_done(self):
        return self._done

    async def send_message(self, **kwargs):
        self.send_calls.append(kwargs)
        self._done = True

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        self._done = True


class FakeFollowup:
    def __init__(self):
        self.send_calls = []

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage()


class FakeInteraction:
    def __init__(self):
        self.response = FakeResponse()
        self.followup = FakeFollowup()
        self._original_response = FakeMessage()

    async def original_response(self):
        return self._original_response


class FakeContext:
    def __init__(self, interaction=None):
        self.interaction = interaction
        self.send_calls = []

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage()


class CommandUtilsTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_hybrid_response_uses_prefix_send_for_prefix_context(self):
        ctx = FakeContext()
        message = await send_hybrid_response(ctx, content="hi", ephemeral=True, delete_after=5.0)
        self.assertIsInstance(message, FakeMessage)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["content"], "hi")
        self.assertEqual(ctx.send_calls[0]["delete_after"], 5.0)

    async def test_send_hybrid_response_uses_initial_interaction_response_when_not_done(self):
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction)
        message = await send_hybrid_response(ctx, content="hello", ephemeral=True)
        self.assertIs(message, interaction._original_response)
        self.assertEqual(len(interaction.response.send_calls), 1)
        self.assertEqual(interaction.response.send_calls[0]["content"], "hello")
        self.assertTrue(interaction.response.send_calls[0]["ephemeral"])
        self.assertEqual(interaction.followup.send_calls, [])

    async def test_send_hybrid_response_uses_followup_after_defer(self):
        interaction = FakeInteraction()
        interaction.response._done = True
        ctx = FakeContext(interaction=interaction)
        message = await send_hybrid_response(ctx, content="later", ephemeral=True)
        self.assertIsInstance(message, FakeMessage)
        self.assertEqual(interaction.response.send_calls, [])
        self.assertEqual(len(interaction.followup.send_calls), 1)
        self.assertEqual(interaction.followup.send_calls[0]["content"], "later")
        self.assertTrue(interaction.followup.send_calls[0]["ephemeral"])
        self.assertTrue(interaction.followup.send_calls[0]["wait"])

    async def test_defer_hybrid_response_defers_pending_interactions(self):
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction)
        deferred = await defer_hybrid_response(ctx, ephemeral=True)
        self.assertTrue(deferred)
        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertTrue(interaction.response.defer_calls[0]["ephemeral"])
        self.assertTrue(interaction.response.defer_calls[0]["thinking"])

    async def test_defer_hybrid_response_is_noop_for_prefix_or_completed_interaction(self):
        prefix_ctx = FakeContext()
        self.assertFalse(await defer_hybrid_response(prefix_ctx, ephemeral=True))

        interaction = FakeInteraction()
        interaction.response._done = True
        slash_ctx = FakeContext(interaction=interaction)
        self.assertFalse(await defer_hybrid_response(slash_ctx, ephemeral=True))
        self.assertEqual(interaction.response.defer_calls, [])

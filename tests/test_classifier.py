"""Classifier tests — _first_text helper + classify_intent fallback logic.

Live LLM tests are opt-in via RUN_LIVE_LLM=1 (cost ~$0.01 per run).
"""
import os
import types
import pytest


# ─── _first_text helper ──────────────────────────────────────────────────────

def _block(type_, **kwargs):
    """Tiny fake content block with the attrs Anthropic SDK exposes."""
    return types.SimpleNamespace(type=type_, **kwargs)


def test_first_text_returns_first_text_block():
    from agent import _first_text
    resp = types.SimpleNamespace(content=[_block("text", text="hello")])
    assert _first_text(resp) == "hello"


def test_first_text_skips_thinking_block():
    """DeepSeek prepends a thinking block; helper must skip past it."""
    from agent import _first_text
    resp = types.SimpleNamespace(content=[
        _block("thinking", thinking="...reasoning..."),
        _block("text", text="actual reply"),
    ])
    assert _first_text(resp) == "actual reply"


def test_first_text_returns_empty_when_no_text():
    from agent import _first_text
    resp = types.SimpleNamespace(content=[_block("thinking", thinking="...")])
    assert _first_text(resp) == ""


# ─── classify_intent: tool_use path ──────────────────────────────────────────

def _tool_use_resp(name: str, **input_):
    """Fake response with a single tool_use block."""
    block = types.SimpleNamespace(type="tool_use", name=name, input=input_)
    return types.SimpleNamespace(content=[block])


def _text_resp(text: str):
    """Fake response with no tool_use, only a text block."""
    return types.SimpleNamespace(content=[_block("text", text=text)])


def _empty_resp():
    return types.SimpleNamespace(content=[])


def test_classify_intent_extracts_tool_use(monkeypatch):
    import agent
    monkeypatch.setattr(agent, "MOCK_MODE", False, raising=False)
    monkeypatch.setattr(agent.config, "MOCK_MODE", False)
    fake_client = types.SimpleNamespace(messages=types.SimpleNamespace(
        create=lambda **kw: _tool_use_resp("run_campagne", source="parisattitude")
    ))
    monkeypatch.setattr(agent, "_client", fake_client)
    intent = agent.classify_intent("lance la campagne pour paris attitude")
    assert intent == {"tool": "run_campagne", "source": "parisattitude"}


def test_classify_intent_falls_back_to_text_when_no_tool_use(monkeypatch):
    """DeepSeek replies with plain text for chitchat — surface it as `reply`."""
    import agent
    monkeypatch.setattr(agent.config, "MOCK_MODE", False)
    fake_client = types.SimpleNamespace(messages=types.SimpleNamespace(
        create=lambda **kw: _text_resp("Salut Illan ! Comment ça va ?")
    ))
    monkeypatch.setattr(agent, "_client", fake_client)
    intent = agent.classify_intent("salut")
    assert intent["tool"] == "reply"
    assert "Illan" in intent["text"]


def test_classify_intent_falls_back_to_canned_when_response_empty(monkeypatch):
    import agent
    monkeypatch.setattr(agent.config, "MOCK_MODE", False)
    fake_client = types.SimpleNamespace(messages=types.SimpleNamespace(
        create=lambda **kw: _empty_resp()
    ))
    monkeypatch.setattr(agent, "_client", fake_client)
    intent = agent.classify_intent("…")
    assert intent["tool"] == "reply"
    assert intent["text"]  # non-empty canned fallback


def test_classify_intent_passes_history_to_llm(monkeypatch):
    """History should be prepended to the messages list on each call."""
    import agent
    monkeypatch.setattr(agent.config, "MOCK_MODE", False)
    captured = {}
    def fake_create(**kw):
        captured["messages"] = kw["messages"]
        return _tool_use_resp("reply", text="ok")
    fake_client = types.SimpleNamespace(messages=types.SimpleNamespace(create=fake_create))
    monkeypatch.setattr(agent, "_client", fake_client)
    history = [
        {"role": "user", "content": "lance la campagne"},
        {"role": "assistant", "content": "Campagne terminée: 5 préparés"},
    ]
    agent.classify_intent("qu'as-tu trouvé?", history=history)
    msgs = captured["messages"]
    # history first, then current user message
    assert len(msgs) == 3
    assert msgs[0]["role"] == "user"
    assert msgs[0]["content"] == "lance la campagne"
    assert msgs[1]["role"] == "assistant"
    assert msgs[2]["content"] == "qu'as-tu trouvé?"


def test_classify_intent_mock_mode_uses_heuristic(monkeypatch):
    """In MOCK_MODE the classifier never hits the network."""
    import agent
    monkeypatch.setattr(agent.config, "MOCK_MODE", True)
    intent = agent.classify_intent("lance la campagne")
    assert intent["tool"] == "run_campagne"


# ─── Live LLM routing tests (opt-in) ─────────────────────────────────────────

LIVE = pytest.mark.skipif(
    os.environ.get("RUN_LIVE_LLM") != "1",
    reason="live LLM tests cost tokens; set RUN_LIVE_LLM=1 to run",
)


@LIVE
@pytest.mark.parametrize("phrase, expected_tool", [
    ("lance la campagne", "run_campagne"),
    ("envoie les messages", "run_envoyer"),
    ("donne-moi les URLs des annonces préparées", "list_pending"),
    ("qu'as-tu trouvé en dernier ?", "list_recent"),
    ("active la veille", "run_watch"),
    ("c'est quoi mon budget", "run_settings"),
    ("salut", "reply"),
    ("merci pour ton aide", "reply"),
])
def test_live_classifier_routing(phrase, expected_tool):
    import agent
    intent = agent.classify_intent(phrase)
    assert intent["tool"] == expected_tool, (
        f"phrase {phrase!r} routed to {intent['tool']!r}, "
        f"expected {expected_tool!r}"
    )

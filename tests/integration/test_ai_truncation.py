import json
from datetime import datetime, timezone


def test_large_ai_prompt_truncation(
    mini_sentry, relay, relay_with_processing, spans_consumer
):
    """
    Tests that large AI prompts are truncated properly without breaking JSON parsing.
    
    This is a regression test for the issue where gen_ai.request.messages could be
    truncated in the middle of a JSON string, causing parsing errors on the frontend.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    relay = relay(relay_with_processing())
    
    ts = datetime.now(timezone.utc)

    # Create a very large JSON payload that would cause truncation issues
    # The error occurred at position 99973, so we create a payload larger than that
    large_content = "A" * 50000  # 50KB of content
    large_messages = json.dumps([
        {"role": "user", "content": large_content},
        {"role": "assistant", "content": large_content},
        {"role": "user", "content": large_content},
    ])
    
    # This creates a JSON string > 150KB, which should be truncated to 102400 chars
    
    envelope = f"""
{{
  "event_id": "{uuid4()}",
  "sent_at": "{ts.isoformat()}",
  "dsn": "http://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.example.com/42"
}}
{{
  "type": "span"
}}
{{
  "span_id": "13e7c1ffd66981f0",
  "trace_id": "a9351cd574f092f6acad48e250981f11",
  "parent_span_id": null,
  "start_timestamp": {ts.timestamp()},
  "timestamp": {ts.timestamp() + 1.5},
  "op": "ai.run",
  "description": "generate_text gpt-4o",
  "status": "ok",
  "data": {{
    "gen_ai.request.messages": {json.dumps(large_messages)},
    "gen_ai.request.model": "gpt-4o",
    "gen_ai.usage.input_tokens": 100,
    "gen_ai.usage.output_tokens": 50
  }}
}}
"""

    relay.send_envelope(project_id, envelope)

    spans, _, _ = spans_consumer.get_spans(timeout=10.0, n=1)
    
    # Verify the span was processed
    assert len(spans) == 1
    span = spans[0]
    
    # Verify the AI data fields exist
    assert span["data"]["gen_ai.request.model"] == "gpt-4o"
    assert span["data"]["gen_ai.usage.input_tokens"] == 100
    assert span["data"]["gen_ai.usage.output_tokens"] == 50
    
    # The important part: verify gen_ai.request.messages was truncated but is still valid JSON
    messages_field = span["data"].get("gen_ai.request.messages")
    if messages_field:
        # If it's a string, it should be valid JSON
        if isinstance(messages_field, str):
            try:
                parsed = json.loads(messages_field)
                # If it parses successfully, the truncation worked correctly
                assert isinstance(parsed, list) or isinstance(parsed, str)
            except json.JSONDecodeError as e:
                # This should NOT happen with our fix
                raise AssertionError(
                    f"gen_ai.request.messages contains invalid JSON after truncation: {e}"
                )
        # The field should be truncated if it was too large
        # With max_chars=102400, it should not exceed ~104448 chars (102400 + 2048 allowance)
        if isinstance(messages_field, str):
            assert len(messages_field) <= 105000, (
                f"Field was not truncated: {len(messages_field)} chars"
            )


def test_multiple_large_ai_fields_truncation(
    mini_sentry, relay, relay_with_processing, spans_consumer
):
    """
    Tests that multiple large AI fields are truncated independently.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    relay = relay(relay_with_processing())
    
    ts = datetime.now(timezone.utc)

    # Create large payloads for multiple fields
    large_content = "X" * 60000  # 60KB
    large_messages = json.dumps([{"role": "user", "content": large_content}])
    large_tools = json.dumps([{"name": "tool1", "description": "Y" * 60000}])
    large_response = json.dumps({"result": "Z" * 60000})
    
    envelope = f"""
{{
  "event_id": "{uuid4()}",
  "sent_at": "{ts.isoformat()}",
  "dsn": "http://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.example.com/42"
}}
{{
  "type": "span"
}}
{{
  "span_id": "13e7c1ffd66981f1",
  "trace_id": "a9351cd574f092f6acad48e250981f11",
  "parent_span_id": null,
  "start_timestamp": {ts.timestamp()},
  "timestamp": {ts.timestamp() + 1.5},
  "op": "ai.run",
  "description": "generate_text gpt-4o",
  "status": "ok",
  "data": {{
    "gen_ai.request.messages": {json.dumps(large_messages)},
    "gen_ai.request.available_tools": {json.dumps(large_tools)},
    "gen_ai.response.object": {json.dumps(large_response)},
    "gen_ai.request.model": "gpt-4o"
  }}
}}
"""

    relay.send_envelope(project_id, envelope)

    spans, _, _ = spans_consumer.get_spans(timeout=10.0, n=1)
    
    assert len(spans) == 1
    span = spans[0]
    
    # All large fields should be truncated and remain valid
    for field in [
        "gen_ai.request.messages",
        "gen_ai.request.available_tools",
        "gen_ai.response.object",
    ]:
        value = span["data"].get(field)
        if value and isinstance(value, str):
            # Should be valid JSON
            try:
                json.loads(value)
            except json.JSONDecodeError as e:
                raise AssertionError(f"{field} contains invalid JSON: {e}")
            
            # Should be truncated
            assert len(value) <= 105000, f"{field} was not truncated"


# Helper to generate UUID for testing
def uuid4():
    import uuid
    return str(uuid.uuid4()).replace("-", "")

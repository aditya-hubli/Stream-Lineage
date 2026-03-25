"""Tests for SchemaDriftDetector."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "lineage-ingester"))

from schema_detector import SchemaDriftDetector
from storage import LineageStorage
from streamlineage.envelope import create_root_envelope, _compute_sha256


# --- Fixtures ---

@pytest.fixture
def storage(tmp_path):
    s = LineageStorage(str(tmp_path / "test.duckdb"))
    yield s
    s.close()


@pytest.fixture
def detector(storage):
    return SchemaDriftDetector(storage)


def _envelope_with_schema(schema_id, schema_json):
    """Create an envelope with specific schema fields."""
    schema_hash = _compute_sha256(schema_json)
    env = create_root_envelope(
        producer_id="test-producer",
        schema_id=schema_id,
        schema_json=schema_json,
        topic="test-topic",
        payload_bytes=b"test",
    )
    return env


# --- Classification tests ---

class TestClassifyDrift:
    def test_removed_field_is_breaking(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}, "b": {"type": "int"}}})
        new = json.dumps({"properties": {"a": {"type": "string"}}})
        assert detector.classify_drift(old, new) == "BREAKING"

    def test_type_change_is_breaking(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {"a": {"type": "integer"}}})
        assert detector.classify_drift(old, new) == "BREAKING"

    def test_added_nullable_field_is_additive(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {
            "a": {"type": "string"},
            "b": {"type": "string", "nullable": True},
        }})
        assert detector.classify_drift(old, new) == "ADDITIVE"

    def test_added_required_field_is_breaking(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {
            "a": {"type": "string"},
            "b": {"type": "string", "nullable": False},
        }})
        assert detector.classify_drift(old, new) == "BREAKING"

    def test_no_field_changes_is_compatible(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string", "description": "old"}}})
        new = json.dumps({"properties": {"a": {"type": "string", "description": "new"}}})
        assert detector.classify_drift(old, new) == "COMPATIBLE"

    def test_invalid_json_is_breaking(self, detector):
        assert detector.classify_drift("not-json", "also-not-json") == "BREAKING"

    def test_field_with_default_is_additive(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {
            "a": {"type": "string"},
            "b": {"type": "string", "nullable": False, "default": "x"},
        }})
        assert detector.classify_drift(old, new) == "ADDITIVE"


# --- Schema diff computation tests ---

class TestComputeSchemaDiff:
    def test_fields_added(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {
            "a": {"type": "string"},
            "b": {"type": "integer"},
        }})
        diff = detector.compute_schema_diff(old, new)
        assert len(diff["fields_added"]) == 1
        assert diff["fields_added"][0]["name"] == "b"

    def test_fields_removed(self, detector):
        old = json.dumps({"properties": {
            "a": {"type": "string"},
            "b": {"type": "integer"},
        }})
        new = json.dumps({"properties": {"a": {"type": "string"}}})
        diff = detector.compute_schema_diff(old, new)
        assert len(diff["fields_removed"]) == 1
        assert diff["fields_removed"][0]["name"] == "b"

    def test_fields_changed(self, detector):
        old = json.dumps({"properties": {"a": {"type": "string"}}})
        new = json.dumps({"properties": {"a": {"type": "integer"}}})
        diff = detector.compute_schema_diff(old, new)
        assert len(diff["fields_changed"]) == 1
        assert diff["fields_changed"][0]["old_type"] == "string"
        assert diff["fields_changed"][0]["new_type"] == "integer"

    def test_no_changes(self, detector):
        schema = json.dumps({"properties": {"a": {"type": "string"}}})
        diff = detector.compute_schema_diff(schema, schema)
        assert diff["fields_added"] == []
        assert diff["fields_removed"] == []
        assert diff["fields_changed"] == []

    def test_invalid_json_returns_empty_diff(self, detector):
        diff = detector.compute_schema_diff("bad", "json")
        assert diff["fields_added"] == []
        assert diff["fields_removed"] == []
        assert diff["fields_changed"] == []


# --- Extract fields tests ---

class TestExtractFields:
    def test_json_schema_properties_format(self, detector):
        schema = {"properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}
        fields = detector._extract_fields(schema)
        assert "name" in fields
        assert "age" in fields

    def test_fields_array_format(self, detector):
        schema = {"fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "integer"},
        ]}
        fields = detector._extract_fields(schema)
        assert "name" in fields
        assert "age" in fields

    def test_empty_schema(self, detector):
        assert detector._extract_fields({}) == {}

    def test_unknown_format(self, detector):
        assert detector._extract_fields({"something": "else"}) == {}


# --- Drift detection integration tests ---

class TestCheckEnvelope:
    def test_first_schema_no_drift(self, detector):
        schema_json = json.dumps({"properties": {"a": {"type": "string"}}})
        env = _envelope_with_schema("test.v1", schema_json)
        result = detector.check_envelope(env, schema_json)
        assert result is None  # first time, no drift

    def test_same_schema_no_drift(self, detector):
        schema_json = json.dumps({"properties": {"a": {"type": "string"}}})
        env1 = _envelope_with_schema("test.v1", schema_json)
        detector.check_envelope(env1, schema_json)

        env2 = _envelope_with_schema("test.v1", schema_json)
        result = detector.check_envelope(env2, schema_json)
        assert result is None

    def test_changed_schema_detects_drift(self, detector):
        old_json = json.dumps({"properties": {"a": {"type": "string"}}})
        new_json = json.dumps({"properties": {"a": {"type": "string"}, "b": {"type": "int"}}})

        env1 = _envelope_with_schema("test.v1", old_json)
        detector.check_envelope(env1, old_json)

        env2 = _envelope_with_schema("test.v1", new_json)
        result = detector.check_envelope(env2, new_json)

        assert result is not None
        assert result["schema_id"] == "test.v1"
        assert result["severity"] in ("BREAKING", "ADDITIVE", "COMPATIBLE")
        assert "diff" in result

    def test_drift_event_persisted(self, detector, storage):
        old_json = json.dumps({"properties": {"a": {"type": "string"}}})
        new_json = json.dumps({"properties": {}})  # removed field = BREAKING

        env1 = _envelope_with_schema("test.v1", old_json)
        detector.check_envelope(env1, old_json)

        env2 = _envelope_with_schema("test.v1", new_json)
        result = detector.check_envelope(env2, new_json)

        # Verify drift was stored
        rows = storage.conn.execute(
            "SELECT * FROM schema_drift_events WHERE schema_id = 'test.v1'"
        ).fetchall()
        assert len(rows) == 1
        assert result["severity"] == "BREAKING"

    def test_cache_updated_after_drift(self, detector):
        old_json = json.dumps({"properties": {"a": {"type": "string"}}})
        new_json = json.dumps({"properties": {"a": {"type": "string"}, "b": {"type": "int"}}})

        detector.check_envelope(_envelope_with_schema("s", old_json), old_json)
        detector.check_envelope(_envelope_with_schema("s", new_json), new_json)

        # Now the cache should have the new schema
        assert detector.known_schemas["s"][1] == new_json


# --- Drift history tests ---

class TestDriftHistory:
    def test_get_drift_history(self, detector):
        old_json = json.dumps({"properties": {"a": {"type": "string"}}})
        new_json = json.dumps({"properties": {"a": {"type": "integer"}}})

        detector.check_envelope(_envelope_with_schema("s", old_json), old_json)
        detector.check_envelope(_envelope_with_schema("s", new_json), new_json)

        history = detector.get_drift_history("s")
        assert len(history) == 1
        assert history[0]["severity"] == "BREAKING"
        assert "diff" in history[0]

    def test_empty_history(self, detector):
        assert detector.get_drift_history("nonexistent") == []

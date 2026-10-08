"""Contract checks for the source catalog and the HTTP API. Stdlib unittest only.

Run from the repo root (server.py mounts static/ relative to the cwd):
    python tests/test_catalog_and_api.py
"""

import os
import pathlib
import sys
import types
import unittest
from unittest import mock

# The code imports bigquery_agent.*, but this checkout is not named that.
# Map the package name onto the repo root so the imports resolve either way.
ROOT = pathlib.Path(__file__).resolve().parent.parent
pkg = types.ModuleType("bigquery_agent")
pkg.__path__ = [str(ROOT)]
sys.modules.setdefault("bigquery_agent", pkg)

from fastapi.testclient import TestClient  # noqa: E402

from bigquery_agent import server  # noqa: E402
from bigquery_agent.agent import create_bigquery_agent  # noqa: E402
from bigquery_agent.sources import SOURCE_CATALOG, get_source, list_sources  # noqa: E402


class CatalogTests(unittest.TestCase):
    def test_every_entry_is_complete(self):
        for key, src in SOURCE_CATALOG.items():
            with self.subTest(source=key):
                self.assertTrue(src.display_name)
                self.assertTrue(src.suggested_dataset)
                self.assertTrue(src.origin_repos)
                self.assertTrue(src.domain_prompt.strip())
                self.assertTrue(src.example_questions)

    def test_list_sources_matches_catalog(self):
        self.assertEqual({s["key"] for s in list_sources()}, set(SOURCE_CATALOG))

    def test_unknown_source_is_none(self):
        self.assertIsNone(get_source("no_such_source"))


class AgentFactoryTests(unittest.TestCase):
    def test_source_key_injects_domain_context(self):
        agent = create_bigquery_agent(access_token="t", project_id="p", source_key="meshtastic")
        self.assertIn("DOMAIN CONTEXT — Meshtastic Mesh", agent.instruction)
        self.assertIn("Default Dataset: meshnarc", agent.instruction)

    def test_no_source_lists_every_source(self):
        agent = create_bigquery_agent(access_token="t", project_id="p")
        self.assertIn("AVAILABLE LOG SOURCES", agent.instruction)
        for src in SOURCE_CATALOG.values():
            self.assertIn(src.display_name, agent.instruction)


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(server.app)

    def test_sources_endpoint_lists_catalog(self):
        resp = self.client.get("/api/sources")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual({s["key"] for s in resp.json()["sources"]}, set(SOURCE_CATALOG))

    def test_config_serves_client_id_from_env(self):
        with mock.patch.dict(os.environ, {"OAUTH_CLIENT_ID": "test-id.apps.googleusercontent.com"}):
            resp = self.client.get("/api/config")
        self.assertEqual(resp.json(), {"client_id": "test-id.apps.googleusercontent.com"})

    def test_query_without_bearer_token_is_401(self):
        resp = self.client.post("/api/query", json={"message": "hi", "project_id": "p"})
        self.assertEqual(resp.status_code, 401)

    def test_query_rejects_empty_message_before_building_agent(self):
        with mock.patch.object(server, "_build_runner") as build:
            resp = self.client.post(
                "/api/query",
                json={"message": "   ", "project_id": "p"},
                headers={"Authorization": "Bearer test-token"},
            )
        self.assertEqual(resp.status_code, 400)
        build.assert_not_called()

    def test_query_requires_project_id(self):
        resp = self.client.post(
            "/api/query",
            json={"message": "hi"},
            headers={"Authorization": "Bearer test-token"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_index_is_served(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers["content-type"])


if __name__ == "__main__":
    unittest.main()

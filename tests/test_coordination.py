import tempfile
import unittest

from scripts.coordination import CoordinationStore, StreamState


class CoordinationTests(unittest.TestCase):
    def test_store_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = CoordinationStore(tmp_dir)
            state = StreamState.default("ess_string_HON")
            store.save(state)

            loaded = store.load("ess_string_HON", auto_create=False)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.logical_table, "ess_string_HON")
            self.assertEqual(loaded.open_segment().physical_table, "ess_string_HON")

    def test_close_open_segment_creates_archive_and_new_current(self) -> None:
        state = StreamState.default("ess_string_HON")
        updated = state.close_open_segment(
            "ess_string_HON_a2603090215",
            max_id=456,
            archived_at="2026-03-09T02:15:03-05:00",
        )

        self.assertEqual(updated.next_epoch, 3)
        self.assertEqual(len(updated.segments), 2)
        self.assertTrue(updated.segments[0].closed)
        self.assertEqual(updated.segments[0].physical_table, "ess_string_HON_a2603090215")
        self.assertEqual(updated.segments[0].max_id, 456)
        self.assertFalse(updated.segments[1].closed)
        self.assertEqual(updated.open_segment().physical_table, "ess_string_HON")

    def test_drop_decision_skips_incomplete_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = CoordinationStore(tmp_dir)
            state = StreamState.default("ess_string_HON").close_open_segment(
                "ess_string_HON_a2603090215",
                max_id=456,
                archived_at="2026-03-09T02:15:03-05:00",
            )

            allow, skip = store.drop_decision(
                state,
                ["ess_string_HON_a2603090215"],
                require_replication_complete=True,
            )
            self.assertEqual(allow, [])
            self.assertEqual(skip, ["ess_string_HON_a2603090215"])

    def test_drop_decision_allows_complete_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = CoordinationStore(tmp_dir)
            state = StreamState.from_dict(
                "ess_string_HON",
                {
                    "logical_table": "ess_string_HON",
                    "next_epoch": 3,
                    "segments": [
                        {
                            "epoch": 1,
                            "physical_table": "ess_string_HON_a2603090215",
                            "closed": True,
                            "max_id": 456,
                            "copied_until_id": 456,
                            "replication_complete": True,
                            "archived_at": "2026-03-09T02:15:03-05:00",
                        },
                        {
                            "epoch": 2,
                            "physical_table": "ess_string_HON",
                            "closed": False,
                            "max_id": None,
                            "copied_until_id": 0,
                            "replication_complete": False,
                            "archived_at": None,
                        },
                    ],
                },
            )

            allow, skip = store.drop_decision(
                state,
                ["ess_string_HON_a2603090215"],
                require_replication_complete=True,
            )
            self.assertEqual(allow, ["ess_string_HON_a2603090215"])
            self.assertEqual(skip, [])


if __name__ == "__main__":
    unittest.main()

import datetime as dt
import unittest

from scripts.db_archive import (
    TableConfig,
    build_archive_name,
    compute_drop_candidates,
    filter_archive_tables,
    resolve_tables,
)


class DbArchiveLogicTests(unittest.TestCase):
    def test_filter_archive_tables(self) -> None:
        source = [
            "ess_string_HON_a2601010210",
            "ess_string_HON_a260101021001",
            "ess_string_HON_a_bad",
            "ess_string_HON",
            "other_a2601010210",
        ]
        filtered = filter_archive_tables("ess_string_HON", source)
        self.assertEqual(
            filtered,
            ["ess_string_HON_a2601010210", "ess_string_HON_a260101021001"],
        )

    def test_build_archive_name_adds_seconds_on_collision(self) -> None:
        now = dt.datetime(2026, 3, 4, 2, 10, 9)
        existing = ["ess_string_DLN_a2603040210"]
        new_name = build_archive_name("ess_string_DLN", existing, now)
        self.assertEqual(new_name, "ess_string_DLN_a260304021009")

    def test_compute_drop_candidates(self) -> None:
        archives = [
            "ess_string_0000_a2601010210",
            "ess_string_0000_a2601080210",
            "ess_string_0000_a2601150210",
            "ess_string_0000_a2601220210",
            "ess_string_0000_a2601290210",
        ]
        to_drop = compute_drop_candidates(archives, max_generations=4)
        self.assertEqual(to_drop, ["ess_string_0000_a2601010210"])

    def test_resolve_tables_subset(self) -> None:
        tables = [
            TableConfig(name="ess_string_HON", max_generations=4),
            TableConfig(name="ess_string_HONSJ", max_generations=4),
        ]
        selected = resolve_tables(tables, ["ess_string_HONSJ"])
        self.assertEqual([item.name for item in selected], ["ess_string_HONSJ"])

    def test_resolve_tables_raises_on_unknown(self) -> None:
        tables = [TableConfig(name="ess_string_HON", max_generations=4)]
        with self.assertRaises(ValueError):
            resolve_tables(tables, ["unknown_table"])


if __name__ == "__main__":
    unittest.main()

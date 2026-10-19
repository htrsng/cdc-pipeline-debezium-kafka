import unittest
from datetime import date
from decimal import Decimal
from history_generator import make_orders


class HistoryTests(unittest.TestCase):
    def generate(self):
        return list(make_orders([1, 2, 3], [(1, Decimal('0.10')), (2, Decimal('10.00'))], 90, 10, date(2026, 10, 16), 42))

    def test_reproducible_and_covers_every_day(self):
        rows = self.generate()
        self.assertEqual(rows, self.generate())
        days = {r[1].date() for r in rows}
        self.assertEqual(len(days), 90)
        self.assertEqual(max(days), date(2026, 10, 16))
        self.assertTrue(all(items and all(q > 0 and isinstance(p, Decimal) for q, p in items.values()) for _, _, _, items in rows))

    def test_repeated_customers_and_status_diversity(self):
        rows = self.generate()
        self.assertLess(len({r[0] for r in rows}), len(rows))
        self.assertEqual({r[2] for r in rows}, {'PENDING', 'CONFIRMED', 'SHIPPED', 'CANCELLED'})


if __name__ == '__main__':
    unittest.main()

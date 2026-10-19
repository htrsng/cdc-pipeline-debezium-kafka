"""Regression checks for order status propagation and durable deferred events."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal

spec = importlib.util.spec_from_file_location('cdc_consumer', Path(__file__).parents[1] / 'consumer/consumer.py')
cdc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cdc)


class ConsumerTests(unittest.TestCase):
    def test_offset_committed_only_after_database(self):
        events = []
        kafka, conn, msg = MagicMock(), MagicMock(), MagicMock()
        msg.error.return_value = None
        msg.key.return_value = None
        msg.value.return_value = b'{}'
        kafka.poll.side_effect = [msg, KeyboardInterrupt()]
        conn.commit.side_effect = lambda: events.append('database')
        kafka.commit.side_effect = lambda **kw: events.append('kafka')
        with patch.object(cdc, 'Consumer', return_value=kafka), patch.object(cdc, 'get_dwh_connection', return_value=conn), patch.object(cdc, 'process_message'), patch.object(cdc, 'retry_pending_order_items'):
            cdc.main()
        self.assertEqual(events, ['database', 'kafka'])

    def test_failed_event_stops_without_committing_offset(self):
        kafka, conn, msg = MagicMock(), MagicMock(), MagicMock()
        msg.error.return_value = None
        msg.key.return_value = None
        msg.value.return_value = b'{}'
        kafka.poll.return_value = msg
        with patch.object(cdc, 'Consumer', return_value=kafka), patch.object(cdc, 'get_dwh_connection', return_value=conn), patch.object(cdc, 'process_message', side_effect=RuntimeError('database failure')), self.assertLogs(cdc.log, level='ERROR'):
            with self.assertRaises(RuntimeError):
                cdc.main()
        conn.rollback.assert_called_once()
        kafka.commit.assert_not_called()

    def test_parent_status_propagates_to_existing_facts(self):
        cur = MagicMock()
        cdc.handle_order(cur, 'u', None, dict(order_id=10, customer_id=2, order_status='CANCELLED', order_date=1700000000000000))
        updates = [(q, args) for q, args in (c.args for c in cur.execute.call_args_list) if 'UPDATE fact_orders SET order_status' in q]
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][1][0], 'CANCELLED')
        self.assertEqual(updates[0][1][-1], 10)

    def test_missing_parent_is_saved_durably(self):
        cur = MagicMock()
        cur.fetchone.return_value = None
        cdc.handle_order_item(cur, 'c', None, dict(order_item_id=1, order_id=2), 100)
        self.assertTrue(any('INSERT INTO pending_order_items' in c.args[0] for c in cur.execute.call_args_list))

    def test_delete_removes_deferred_event(self):
        cur = MagicMock()
        cdc.handle_order_item(cur, 'd', dict(order_item_id=1), None, 100)
        self.assertIn('DELETE FROM pending_order_items', cur.execute.call_args_list[0].args[0])

    def test_money_uses_decimal(self):
        cur = MagicMock()
        cur.fetchone.return_value = (2, 'CONFIRMED', cdc.micros_to_datetime(1700000000000000))
        cdc.handle_order_item(cur, 'c', None, dict(order_item_id=1, order_id=2, product_id=3, quantity=3, unit_price='0.10', updated_at=1700000000000000), 100)
        fact = [c for c in cur.execute.call_args_list if 'INSERT INTO fact_orders' in c.args[0]][0]
        self.assertEqual(fact.args[1][7], Decimal('0.30'))


if __name__ == '__main__':
    unittest.main()

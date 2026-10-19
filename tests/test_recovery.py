from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from state_store import Counter, Store, encode, root, restore, snapshot


def layout(base):
    base = Path(base)
    data, trusted = base / "data", base / "trusted"
    data.mkdir(); trusted.mkdir()
    store = Store(data / "state.json", trusted / "roots.json")
    store.initialize()
    return store


def counter(count):
    result = Counter()
    for i in range(count):
        result.add(i % 7 - 3)
    return result


class RecoveryTests(unittest.TestCase):
    def test_roundtrip_and_independent_200_event_prefix(self):
        full, prefix = counter(205), counter(200)
        self.assertEqual(restore(snapshot(full)).events[:200], prefix.events)
        with tempfile.TemporaryDirectory() as folder:
            store = layout(folder); store.save(prefix)
            restarted, _ = store.recover()
            for event in full.events[200:]:
                restarted.add(event["delta"])
            store.save(restarted)
            self.assertEqual(snapshot(store.recover()[0]), snapshot(full))
            unchanged = store.trusted.read_bytes()
            store.save(restarted); store.recover()
            self.assertEqual(store.trusted.read_bytes(), unchanged)

    def test_invalid_events_future_derived_values_and_boolean_input(self):
        with self.assertRaises(ValueError): Counter().add(True)
        broken = counter(2).events
        broken[0]["after"] = 999
        with self.assertRaises(ValueError): Counter.replay(broken)
        data = json.loads(snapshot(counter(3)))
        data["checkpoint"]["value"] = 999
        with self.assertRaises(ValueError): restore(encode(data))

    def test_process_crashes_at_intent_data_confirm_boundaries(self):
        phases = ("intent:partial", "intent:after_replace", "data:before_replace",
                  "data:after_replace", "data:ack_lost_window", "confirm:partial",
                  "confirm:before_replace", "confirm:after_replace")
        for phase in phases:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as folder:
                store = layout(folder); store.save(counter(3))
                script = """
import os,sys
from state_store import Store
from tests.test_recovery import counter
def crash(at):
    if at==sys.argv[3]: os._exit(91)
Store(sys.argv[1],sys.argv[2]).save(counter(5),hit=crash)
raise SystemExit('hook missed')
"""
                run = subprocess.run([sys.executable, "-B", "-c", script,
                                      str(store.bundle), str(store.trusted), phase],
                                     cwd=Path(__file__).resolve().parents[1],
                                     capture_output=True, text=True, timeout=30)
                self.assertEqual(run.returncode, 91, run.stderr)
                restarted = Store(store.bundle, store.trusted)
                actual, _ = restarted.recover()
                self.assertEqual(actual.events, counter(5 if phase in phases[3:] else 3).events)
                restarted.save(counter(5))
                self.assertEqual(restarted.recover()[0].events, counter(5).events)

    def test_unknown_data_preserves_intent_and_blocks_next_save(self):
        with tempfile.TemporaryDirectory() as folder:
            store = layout(folder); store.save(counter(3))
            def stop(phase):
                if phase == "data:before_replace": raise OSError("injected failure")
            with self.assertRaises(OSError): store.save(counter(5), hit=stop)
            roots = store.trusted.read_bytes()
            store.bundle.write_bytes(b"unknown")
            with self.assertRaises(ValueError): store.recover()
            self.assertEqual(store.trusted.read_bytes(), roots)
            with self.assertRaises(ValueError): store.save(counter(6))
            self.assertEqual(store.trusted.read_bytes(), roots)

    def test_missing_trust_never_self_enrolls_from_bundle(self):
        with tempfile.TemporaryDirectory() as folder:
            store = layout(folder); store.save(counter(3))
            store.trusted.unlink()
            with self.assertRaises(FileNotFoundError): store.recover()
            with self.assertRaises(ValueError): store.initialize()

    def test_corruption_rollback_and_binding_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            store = layout(folder); store.save(counter(3))
            old = store.bundle.read_bytes()
            store.save(counter(5))
            for bad in (old, old[:len(old)//2], old+b"extra"):
                store.bundle.write_bytes(bad)
                with self.assertRaises(ValueError): store.recover()
            trusted = json.loads(store.trusted.read_bytes())
            trusted["target"] += ".other"
            store.trusted.write_bytes(encode(trusted))
            with self.assertRaises(ValueError): store.recover()

    def test_first_write_and_recovery_ack_can_be_lost(self):
        for phase in ("intent:after_replace", "data:ack_lost_window"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as folder:
                store = layout(folder)
                def stop(at):
                    if at == phase: raise OSError("injected lost acknowledgement")
                with self.assertRaises(OSError): store.save(counter(5), hit=stop)
                def lose_resolution(at):
                    if at == "resolve:after_replace": raise OSError("lost resolution acknowledgement")
                with self.assertRaises(OSError): store.recover(hit=lose_resolution)
                before = store.trusted.read_bytes()
                recovered, _ = store.recover()
                self.assertEqual(recovered is None, phase.startswith("intent:"))
                self.assertEqual(store.trusted.read_bytes(), before)
                store.save(counter(5))

    def test_write_fsync_and_replace_failure_preserve_previous_snapshot(self):
        for operation in ("fsync", "replace"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as folder:
                store = layout(folder); store.save(counter(3))
                old = store.bundle.read_bytes()
                with patch("state_store.os." + operation, side_effect=OSError("injected I/O failure")):
                    with self.assertRaises(OSError): store.save(counter(5))
                self.assertEqual(store.bundle.read_bytes(), old)
                self.assertEqual(store.recover()[0].events, counter(3).events)

    def test_prefix_shortening_and_other_writer_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            store = layout(folder); store.save(counter(3))
            before = store.bundle.read_bytes()
            with self.assertRaises(ValueError): store.save(counter(2))
            other = counter(4); other.events[0]["delta"] = 100
            with self.assertRaises(ValueError): store.save(other)
            self.assertEqual(store.bundle.read_bytes(), before)
            from state_store import locked
            script = """
import sys
from state_store import locked
from pathlib import Path
try:
    with locked(Path(sys.argv[1])): pass
except OSError: raise SystemExit(73)
raise SystemExit(74)
"""
            with locked(store.trusted):
                run = subprocess.run([sys.executable,"-B","-c",script,str(store.trusted)],
                                     cwd=Path(__file__).resolve().parents[1],capture_output=True,timeout=30)
            self.assertEqual(run.returncode,73)


if __name__ == "__main__": unittest.main()

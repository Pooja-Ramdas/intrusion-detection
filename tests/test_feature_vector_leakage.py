import unittest
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "features")))
from feature_safety import safe

class FeatureLeakageTest(unittest.TestCase):
    def test_safe_numeric_names_pass(self):
        self.assertTrue(safe(["duration_seconds", "payload_entropy_nats"]))

    def test_identifier_label_split_names_fail(self):
        for name in ("flow_id", "source_ip", "destination_port", "timestamp", "label", "split"):
            with self.subTest(name=name), self.assertRaises(AssertionError):
                safe([name])

if __name__ == "__main__": unittest.main()

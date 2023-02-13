import unittest

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.processes.process_get_data import *


class TestProcesses(unittest.TestCase):
    def test_process_get_data(self):
        ret = ProcessGetData().run(
            storage=S3Storage(region="us-west-2"),
            bucket="crowemi-trades",
            manifest_key="manifest.json",
        )
        assert ret

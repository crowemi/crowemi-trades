import json
from datetime import datetime, timedelta

from crowemi_trades.storage.base_storage import BaseStorage
from crowemi_trades.utilities.get_daily_data import get_daily_data
from crowemi_trades.processes.process_core import ProcessCore


class ProcessGetData(ProcessCore):
    """Class for processing data."""

    def run(
        self,
        storage: BaseStorage,
        **kwargs,
    ) -> int:
        """Run the process GetData.
        ---
        bucket - the aws bucket used for storage. \n
        manifest_key - the aws key manifest.
        """
        ret: int = 0  # innocent until proven guilty
        self.storage = storage

        # these are specific to AWS! what is the common denominator across cloud storage? (file, file_path, etc.)
        self.bucket: str = kwargs.get("bucket", None)
        self.manifest_key: str = kwargs.get("manifest_key", None)

        if not self.bucket:
            raise Exception("No bucket supplied.")
        if not self.manifest_key:
            raise Exception("No manifest supplied.")

        # get manifest
        manifest = self._get_manifest()
        # process manifest
        if manifest:
            manifest = list(map(self._process_manifest, manifest))
        else:
            raise Exception("Failed to retreive manifest.")
        # update manifest
        ret = self._update_manifest(manifest)
        # return
        return ret

    def _update_manifest(
        self,
        manifest: str,
    ) -> bool:
        if self.storage.type == "aws":
            return self.storage.write(
                self.bucket,
                self.manifest_key,
                bytes(json.dumps(manifest), "utf-8"),
            )
        else:
            assert False, f"Unknown storage type {self.storage.type}"

    def _get_manifest(
        self,
    ) -> object:
        # pull extract instructions from S3
        try:
            ret: object
            if self.storage.type == "aws":
                ret = json.loads(
                    self.storage.read_content(
                        bucket=self.bucket,
                        key=self.manifest_key,
                    )
                )
            else:
                raise Exception("Unknown storage type.")
            return ret
        except Exception as e:
            print(e)

    def _process_manifest(
        self,
        record,
    ) -> list:
        ret: bool = False
        ticker = record.get("ticket", None)
        timespan = record.get("timespan", None)
        interval = record.get("interval", None)
        bucket = record.get("bucket", None)
        # process last_modified
        last_modified = record.get("last_modified", None)
        today = datetime.utcnow()
        start_date = datetime(
            year=int(last_modified[0:4]),
            month=int(last_modified[4:6]),
            day=int(last_modified[6:8]),
        ) + timedelta(days=1)
        end_date = today + timedelta(days=-1)

        if start_date < end_date:
            ret = get_daily_data(
                ticker,
                timespan,
                interval,
                start_date,
                end_date,
                bucket,
            )
        if ret:
            # update manifest last modified date
            record[
                "last_modified"
            ] = f"{end_date.year}{end_date.month:02}{end_date.day:02}"

        return record

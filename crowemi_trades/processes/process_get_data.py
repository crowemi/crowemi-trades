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
        """Run the process."""
        ret: int = 0  # innocent until proven guilty
        self.storage = storage

        manifest_details = kwargs.get("manifest", None)
        if manifest_details:
            self.manifest_key: str = manifest_details.get("manifest_key", None)
            self.manifest_storage: BaseStorage = manifest_details.get(
                "manifest_storage", None
            )

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
        if self.manifest_storage.type == "aws":
            return self.manifest_storage.write(
                key=self.manifest_key,
                content=bytes(json.dumps(manifest), "utf-8"),
            )
        if self.manifest_storage.type == "mongodb":
            return self.manifest_storage.write(
                database="data",
                collection="manifest",
                records=manifest,
            )
        else:
            assert False, f"Unknown storage type {self.manifest_storage.type}"

    def _get_manifest(
        self,
    ) -> object:
        # pull extract instructions from S3
        try:
            ret: object
            if self.manifest_storage.type == "aws":
                if not self.manifest_key:
                    raise Exception("No manifest supplied.")

                ret = json.loads(
                    self.manifest_storage.read_content(
                        key=self.manifest_key,
                    )
                )
            if self.manifest_storage.type == "mongodb":
                status, ret = self.manifest_storage.read(
                    database="data", collection="manifest"
                )
                if not status:
                    raise Exception("Failed to read manifest.")
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
        # process last_modified
        last_modified = record.get("last_modified", None)
        today = datetime.utcnow()
        start_date = datetime(
            year=int(last_modified[0:4]),
            month=int(last_modified[4:6]),
            day=int(last_modified[6:8]),
        ) + timedelta(days=1)
        end_date = datetime(
            year=today.year, month=today.month, day=today.day
        ) + timedelta(days=-1)

        if start_date <= end_date:
            ret = get_daily_data(
                ticker,
                timespan,
                interval,
                start_date,
                end_date,
                self.storage,  # storage
            )
        if ret:
            # update manifest last modified date
            record[
                "last_modified"
            ] = f"{end_date.year}{end_date.month:02}{end_date.day:02}"

        return record

"""Bucket module for ReductStore HTTP API"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import (
    Optional,
    List,
    AsyncIterator,
    Union,
    Dict,
    Tuple,
)

from reduct.error import ReductError
from reduct.http import HttpClient
from reduct.msg.bucket import (
    BucketSettings,
    BucketInfo,
    EntryInfo,
    BucketFullInfo,
    QueryEntry,
    QueryType,
)
from reduct.record import (
    Record,
    parse_batched_records,
    parse_record,
    Batch,
    TIME_PREFIX,
    ERROR_PREFIX,
)
from reduct.time import unix_timestamp_from_any


class Bucket:
    """A bucket of data in Reduct Storage"""

    def __init__(self, name: str, http: HttpClient):
        self._http = http
        self.name = name

    async def get_settings(self) -> BucketSettings:
        """
        Get current bucket settings
        Returns:
             BucketSettings: the bucket settings
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).settings

    async def set_settings(self, settings: BucketSettings):
        """
        Update bucket settings
        Args:
            settings: new settings
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("PUT", f"/b/{self.name}", data=settings.json())

    async def info(self) -> BucketInfo:
        """
        Get statistics about bucket
        Returns:
           BucketInfo: the bucket information
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).info

    async def get_entry_list(self) -> List[EntryInfo]:
        """
        Get list of entries with their stats
        Returns:
            List[EntryInfo]: the list of entries with stats
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).entries

    async def remove(self):
        """
        Remove bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/b/{self.name}")

    async def remove_entry(self, entry_name: str):
        """
        Remove entry from bucket
        Args:
            entry_name: name of entry
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/b/{self.name}/{entry_name}")

    async def remove_record(
        self, entry_name: str, timestamp: Union[int, datetime, float, str]
    ):
        """
        Remove record from entry
        Args:
            entry_name: name of entry
            timestamp: timestamp of record
        Raises:
            ReductError: if there is an HTTP error
        """
        timestamp = unix_timestamp_from_any(timestamp)
        await self._http.request_all(
            "DELETE", f"/b/{self.name}/{entry_name}?ts={timestamp}"
        )

    async def remove_batch(
        self, entry_name: str, batch: Batch
    ) -> Dict[int, ReductError]:
        """
        Remove batch of records from entries in a sole request
        Args:
            entry_name: name of entry in the bucket
            batch: list of timestamps
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with  record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error
        """
        _, record_headers = self._make_headers(batch)
        _, headers = await self._http.request_all(
            "DELETE",
            f"/b/{self.name}/{entry_name}/batch",
            extra_headers=record_headers,
        )

        return self._parse_errors_from_headers(headers)

    async def remove_query(
        self,
        entry_name: str,
        start: Optional[Union[int, datetime, float, str]] = None,
        stop: Optional[Union[int, datetime, float, str]] = None,
        when: Optional[Dict] = None,
        **kwargs,
    ) -> int:
        """
        Query data to remove within a time interval
        The time interval is defined by the start and stop parameters that can be:
        int (UNIX timestamp in microseconds), datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Args:
            entry_name: name of entry in the bucket
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            when: condtion to filter
        Keyword Args:
            include (dict): remove records which have all labels
                from this dict (DEPRECATED use when)
            exclude (dict): remove records which doesn't have all labels
                from this (DEPRECATED use when)
            each_s(Union[int, float]): remove a record for each S seconds
            each_n(int): remove each N-th record
            strict(bool): if True: strict query
        Returns:
            number of removed records
        """
        if (
            self._http.api_version
            and self._http.api_version[0] == 1
            and self._http.api_version[1] >= 13
        ):
            start = unix_timestamp_from_any(start) if start else None
            stop = unix_timestamp_from_any(stop) if stop else None

            query_message = QueryEntry(
                query_type=QueryType.REMOVE, start=start, stop=stop, when=when, **kwargs
            )
            data = query_message.model_dump_json()
            url = f"/b/{self.name}/{entry_name}/q"
            resp, _ = await self._http.request_all(
                "POST",
                url,
                data=data,
            )
        else:
            params = await self._parse_query_params(kwargs, start, stop)
            resp, _ = await self._http.request_all(
                "DELETE", f"/b/{self.name}/{entry_name}/q", params=params
            )

        return json.loads(resp)["removed_records"]

    async def rename_entry(self, old_name: str, new_name: str):
        """
        Rename entry
        Args:
            old_name: old name of entry
            new_name: new name of entry
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all(
            "PUT", f"/b/{self.name}/{old_name}/rename", json={"new_name": new_name}
        )

    async def rename(self, new_name: str):
        """
        Rename bucket
        Args:
            new_name: new name of bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all(
            "PUT", f"/b/{self.name}/rename", json={"new_name": new_name}
        )
        self.name = new_name

    @asynccontextmanager
    async def read(
        self,
        entry_name: str,
        timestamp: Optional[Union[int, datetime, float, str]] = None,
        head: bool = False,
    ) -> AsyncIterator[Record]:
        """
        Read a record from entry
        Args:
            entry_name: name of entry in the bucket. If None: get the latest record.
                The timestamp can be int (UNIX timestamp in microseconds),
                datetime, float (UNIX timestamp in seconds), or str (ISO 8601 string).
            timestamp: UNIX timestamp in microseconds - if None: get the latest record
            head: if True: get only the header of a recod with metadata
        Returns:
            AsyncIterator[Record]: the record object
        Raises:
            ReductError: if there is an HTTP error
        Examples:
            >>> async def reader():
            >>>     async with bucket.read("entry", timestamp=123456789) as record:
            >>>         data = await record.read_all()
        """
        params = {"ts": unix_timestamp_from_any(timestamp)} if timestamp else None
        method = "HEAD" if head else "GET"
        async with self._http.request(
            method, f"/b/{self.name}/{entry_name}", params=params
        ) as resp:
            yield parse_record(resp)

    async def write(
        self,
        entry_name: str,
        data: Union[bytes, AsyncIterator[bytes]],
        timestamp: Optional[Union[int, datetime, float, str]] = None,
        content_length: Optional[int] = None,
        **kwargs,
    ):
        """
        Write a record to entry

        Args:
            entry_name: name of entry in the bucket
            data: bytes to write or async iterator
            timestamp: timestamp of record. int (UNIX timestamp in microseconds),
                datetime, float (UNIX timestamp in seconds), str (ISO 8601 string).
                If None: current time
            content_length: content size in bytes,
                needed only when the data is an iterator
        Keyword Args:
            labels (dict): labels as key-values
            content_type (str): content type of data
        Raises:
            ReductError: if there is an HTTP error

        Examples:
            >>> await bucket.write("entry-1", b"some_data",
            >>>    timestamp="2021-09-10T10:30:00")
            >>>
            >>> # You can write data chunk-wise using an asynchronous iterator and the
            >>> # size of the content:
            >>>
            >>> async def sender():
            >>>     for chunk in [b"part1", b"part2", b"part3"]:
            >>>         yield chunk
            >>> await bucket.write("entry-1", sender(), content_length=15)

        """
        timestamp = unix_timestamp_from_any(
            timestamp if timestamp is not None else int(time.time_ns() / 1000)
        )
        params = {"ts": timestamp}
        await self._http.request_all(
            "POST",
            f"/b/{self.name}/{entry_name}",
            params=params,
            data=data,
            content_length=content_length if content_length is not None else len(data),
            **kwargs,
        )

    async def write_batch(
        self, entry_name: str, batch: Batch
    ) -> Dict[int, ReductError]:
        """
        Write a batch of records to entries in a sole request

        Args:
            entry_name: name of entry in the bucket
            batch: list of records
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with record timestamps as keys
        Raises:
            ReductError: if there is an HTTP  or communication error
        """

        async def iter_body():
            for _, rec in batch.items():
                yield await rec.read_all()

        content_length, record_headers = self._make_headers(batch)
        _, headers = await self._http.request_all(
            "POST",
            f"/b/{self.name}/{entry_name}/batch",
            data=iter_body(),
            extra_headers=record_headers,
            content_length=content_length,
        )

        return self._parse_errors_from_headers(headers)

    async def update(
        self,
        entry_name: str,
        timestamp: Union[int, datetime, float, str],
        labels: Dict[str, str],
    ):
        """Update labels of an existing record
        If a label doesn't exist, it will be created.
        If a label is empty, it will be removed.

        Args:
            entry_name: name of entry in the bucket
            timestamp: timestamp of record in microseconds
            labels: new labels
        Raises:
            ReductError: if there is an HTTP error

        Examples:
            >>> await bucket.update("entry-1", "2022-01-01T01:00:00",
                    {"label1": "value1", "label2": ""})

        """
        timestamp = unix_timestamp_from_any(timestamp)
        await self._http.request_all(
            "PATCH", f"/b/{self.name}/{entry_name}?ts={timestamp}", labels=labels
        )

    async def update_batch(
        self, entry_name: str, batch: Batch
    ) -> Dict[int, ReductError]:
        """Update labels of existing records
        If a label doesn't exist, it will be created.
        If a label is empty, it will be removed.

        Args:
            entry_name: name of entry in the bucket
            batch: dict of timestamps as keys and labels as values
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error

        Examples:
            >>> batch = Batch()
            >>> batch.add(1640995200000000, labels={"label1": "value1", "label2": ""})
            >>> await bucket.update_batch("entry-1", batch)

        """

        content_length, record_headers = self._make_headers(batch)
        _, headers = await self._http.request_all(
            "PATCH",
            f"/b/{self.name}/{entry_name}/batch",
            extra_headers=record_headers,
            content_length=content_length,
        )

        return self._parse_errors_from_headers(headers)

    async def query(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        entry_name: str,
        start: Optional[Union[int, datetime, float, str]] = None,
        stop: Optional[Union[int, datetime, float, str]] = None,
        ttl: Optional[int] = None,
        when: Optional[Dict] = None,
        **kwargs,
    ) -> AsyncIterator[Record]:
        """
        Query data for a time interval
        The time interval is defined by the start and stop parameters that can be:
        int (UNIX timestamp in microseconds), datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Args:
            entry_name: name of entry in the bucket
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            ttl: Time To Live of the request in seconds
            when: condtion to filter records
        Keyword Args:
            include (dict): query records which have all labels
                from this dict (DEPRECATED use when)
            exclude (dict): query records which doesn't have all labels
                from this (DEPRECATED use when)
            head (bool): if True: get only the header of a recod with metadata
            each_s(Union[int, float]): return a record for each S seconds
            each_n(int): return each N-th record
            limit (int): limit the number of records
            strict(bool): if True: strict query
        Returns:
             AsyncIterator[Record]: iterator to the records

        Examples:
            >>> async for record in bucket.query("entry-1", stop=time.time_ns() / 1000):
            >>>     data: bytes = record.read_all()
            >>>     # or
            >>>     async for chunk in record.read(n=1024):
            >>>         print(chunk)
        """

        if (
            self._http.api_version
            and self._http.api_version[0] == 1
            and self._http.api_version[1] >= 13
        ):
            query_id = await self._query_post(
                entry_name, QueryType.QUERY, start, stop, when, ttl, **kwargs
            )
        else:
            query_id = await self._query(entry_name, start, stop, ttl, **kwargs)

        last = False
        method = "HEAD" if kwargs.pop("head", False) else "GET"

        while not last:
            async with self._http.request(
                method, f"/b/{self.name}/{entry_name}/batch?q={query_id}"
            ) as resp:
                if resp.status == 204:
                    return
                async for record in parse_batched_records(resp):
                    last = record.last
                    yield record

    async def get_full_info(self) -> BucketFullInfo:
        """
        Get full information about bucket (settings, statistics, entries)

        Returns:
            BucketFullInfo: the full information about the bucket
        """
        body, _ = await self._http.request_all("GET", f"/b/{self.name}")
        return BucketFullInfo.model_validate_json(body)

    async def subscribe(
        self,
        entry_name: str,
        start: Optional[Union[int, datetime, float, str]] = None,
        poll_interval=1.0,
        when: Optional[Dict] = None,
        **kwargs,
    ) -> AsyncIterator[Record]:
        """
        Query records from the start timestamp and wait for new records
        The time interval is defined by the start and stop parameters
        that can be: int (UNIX timestamp in microseconds) datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Args:
            entry_name: name of entry in the bucket
            start: the beginning timestamp to read records.
                If None, then from the first record.
            poll_interval: inteval to ask new records in seconds
            when: condtion to filter records
        Keyword Args:
            include (dict): query records which have all labels
                from this dict (DEPRECATED use when)
            exclude (dict): query records which doesn't have all labels
                from this (DEPRECATED use when)
            head (bool): if True: get only the header of a recod with metadata
            strict(bool): if True: strict query
        Returns:
             AsyncIterator[Record]: iterator to the records

        Examples:
            >>> async for record in bucket.subscribes("entry-1"):
            >>>     data: bytes = record.read_all()
            >>>     # or
            >>>     async for chunk in record.read(n=1024):
            >>>         print(chunk)
        """
        ttl = poll_interval * 2 + 1
        if (
            self._http.api_version
            and self._http.api_version[0] == 1
            and self._http.api_version[1] >= 13
        ):
            query_id = await self._query_post(
                entry_name,
                QueryType.QUERY,
                start,
                None,
                when,
                ttl,
                continuous=True,
                **kwargs,
            )
        else:
            query_id = await self._query(
                entry_name, start, None, ttl, continuous=True, **kwargs
            )

        method = "HEAD" if kwargs.pop("head", False) else "GET"

        while True:
            async with self._http.request(
                method, f"/b/{self.name}/{entry_name}/batch?q={query_id}"
            ) as resp:
                if resp.status == 204:
                    await asyncio.sleep(poll_interval)
                    continue

                async for record in parse_batched_records(resp):
                    yield record

    async def _query(self, entry_name, start, stop, ttl, **kwargs):
        params = await self._parse_query_params(kwargs, start, stop)

        if "limit" in kwargs:
            params["limit"] = kwargs["limit"]

        if ttl:
            params["ttl"] = int(ttl)

        if "continuous" in kwargs:
            params["continuous"] = "true" if kwargs["continuous"] else "false"

        url = f"/b/{self.name}/{entry_name}"
        data, _ = await self._http.request_all(
            "GET",
            f"{url}/q",
            params=params,
        )
        query_id = json.loads(data)["id"]
        return query_id

    async def _query_post(  # pylint: disable=too-many-positional-arguments, too-many-arguments
        self, entry_name, query_type: QueryType, start, stop, when, ttl, **kwargs
    ):
        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None
        query_message = QueryEntry(
            query_type=query_type, start=start, stop=stop, when=when, ttl=ttl, **kwargs
        )
        data = query_message.model_dump_json()
        url = f"/b/{self.name}/{entry_name}/q"
        data, _ = await self._http.request_all(
            "POST",
            url,
            data=data,
        )
        query_id = json.loads(data)["id"]
        return query_id

    async def _parse_query_params(self, kwargs, start, stop):
        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None
        params = {}
        if start:
            params["start"] = start
        if stop:
            params["stop"] = stop
        if "include" in kwargs:
            for name, value in kwargs["include"].items():
                params[f"include-{name}"] = str(value)
        if "exclude" in kwargs:
            for name, value in kwargs["exclude"].items():
                params[f"exclude-{name}"] = str(value)
        if "each_s" in kwargs:
            params["each_s"] = float(kwargs["each_s"])
        if "each_n" in kwargs:
            params["each_n"] = int(kwargs["each_n"])
        return params

    @staticmethod
    def _make_headers(batch: Batch) -> Tuple[int, Dict[str, str]]:
        """Make headers for batch"""
        record_headers = {}
        content_length = 0
        for time_stamp, record in batch.items():
            content_length += record.size
            header = f"{record.size},{record.content_type}"
            for label, value in record.labels.items():
                if "," in label or "=" in label:
                    header += f',{label}="{value}"'
                else:
                    header += f",{label}={value}"

            record_headers[f"{TIME_PREFIX}{time_stamp}"] = header

        record_headers["Content-Type"] = "application/octet-stream"
        return content_length, record_headers

    @staticmethod
    def _parse_errors_from_headers(headers):
        errors = {}
        for key, value in headers.items():
            if key.startswith(ERROR_PREFIX):
                errors[int(key[len(ERROR_PREFIX) :])] = ReductError.from_header(value)
        return errors

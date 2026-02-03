"""Bucket module for ReductStore HTTP API"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import partial
from typing import (
    AsyncIterator,
)

from reduct.batch.batch_v1 import (
    parse_batched_records_v1,
    make_headers_v1,
    parse_errors_from_headers_v1,
    Batch,
)
from reduct.batch.batch_v2 import (
    parse_batched_records_v2,
    make_headers_v2,
    parse_errors_from_headers_v2,
    RecordBatch,
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
    CreateQueryLinkRequest,
    CreateQueryLinkResponse,
)
from reduct.record import Record, parse_record
from reduct.time import unix_timestamp_from_any, TimestampLike


def _check_deprecated_params(_kwargs):
    # We want to keep the function for future deprecations
    pass
    # if "each_s" in kwargs:
    #     warnings.warn(
    #         "The 'each_s' argument is deprecated and will be removed in v1.18.0,"
    #         " use '$each_t' in 'when' instead.",
    #         DeprecationWarning,
    #     )


class Bucket:  # pylint: disable=too-many-public-methods
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
        await self._http.request_all(
            "PUT", f"/b/{self.name}", data=settings.model_dump_json()
        )

    async def info(self) -> BucketInfo:
        """
        Get statistics about bucket
        Returns:
           BucketInfo: the bucket information
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).info

    async def get_entry_list(self) -> list[EntryInfo]:
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

    async def remove_record(self, entry_name: str, timestamp: TimestampLike):
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
    ) -> dict[int, ReductError]:
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
        _, record_headers = make_headers_v1(batch)
        _, headers = await self._http.request_all(
            "DELETE",
            f"/b/{self.name}/{entry_name}/batch",
            extra_headers=record_headers,
        )

        return parse_errors_from_headers_v1(headers)

    async def remove_record_batch(
        self, batch: RecordBatch
    ) -> dict[str, dict[int, ReductError]]:
        """
        Remove batch of records from entries in a sole request (Multi-entry API)
        Args:
            batch: list of timestamps
        Returns:
            Dict[str, Dict[int, ReductError]]: the dictionary of errors
                with entry names as keys and dictionaries of record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error
        """

        if self._http.api_version[1] < 18:
            raise ReductError(
                -1,
                "Multi-entry batch API is not supported by the server. "
                "Requires server version 1.18 or higher.",
            )

        _, record_headers = make_headers_v2(batch)
        _, headers = await self._http.request_all(
            "DELETE",
            f"/io/{self.name}/remove",
            extra_headers=record_headers,
        )

        return parse_errors_from_headers_v2(headers)

    async def remove_query(
        self,
        entries: str | list[str],
        start: TimestampLike | None = None,
        stop: TimestampLike | None = None,
        when: dict | None = None,
        **kwargs,
    ) -> int:
        """
        Query data to remove within a time interval
        The time interval is defined by the start and stop parameters that can be:
        int (UNIX timestamp in microseconds), datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Since version 1.18: entry_name can be a list of entries to query from
        multiple entries. You can also use wildcards in entry names.


        Args:
            entries: name(s) of entry in the bucket
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            when: condition to filter
        Keyword Args:
            each_s(Union[int, float]): remove a record for each S seconds
                (DEPRECATED use $each_t in when)
            each_n(int): remove each N-th record
                (DEPRECATED use $each_n in when)
            strict(bool): if True: strict query
            ext (dict): extended query parameters
        Returns:
            number of removed records
        """
        _check_deprecated_params(kwargs)

        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None

        query_message = QueryEntry(
            query_type=QueryType.REMOVE,
            entries=entries if isinstance(entries, list) else [entries],
            start=start,
            stop=stop,
            when=when,
            **kwargs,
        )

        batch_api_v2 = self._http.api_version[1] >= 18
        if batch_api_v2:
            url = f"/io/{self.name}/q"
        else:
            if isinstance(entries, list):
                raise ReductError(
                    -1,
                    "Multi-entry remove query is not supported by the server. "
                    "Requires server version 1.18 or higher.",
                )
            url = f"/b/{self.name}/{entries}/q"

        resp, _ = await self._http.request_all(
            "POST",
            url,
            data=query_message.model_dump_json(),
            content_type="application/json",
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
        timestamp: TimestampLike | None = None,
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
            yield parse_record(resp, entry_name)

    async def write(
        self,
        entry_name: str,
        data: bytes | AsyncIterator[bytes],
        timestamp: TimestampLike | None = None,
        content_length: int | None = None,
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
    ) -> dict[int, ReductError]:
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

        content_length, record_headers = make_headers_v1(batch)
        _, headers = await self._http.request_all(
            "POST",
            f"/b/{self.name}/{entry_name}/batch",
            data=iter_body(),
            extra_headers=record_headers,
            content_length=content_length,
        )

        return parse_errors_from_headers_v1(headers)

    async def write_record_batch(
        self, batch: RecordBatch
    ) -> dict[str, dict[int, ReductError]]:
        """
        Write a batch of records to entries in a sole request (Multi-entry API)

        Args:
            batch: list of records
        Returns:
            Dict[str, Dict[int, ReductError]]: the dictionary of errors
                with entry names as keys and dictionaries of record timestamps as keys
        Raises:
            ReductError: if there is an HTTP  or communication error
        """

        if self._http.api_version[1] < 18:
            raise ReductError(
                -1,
                "Multi-entry batch API is not supported by the server. "
                "Requires server version 1.18 or higher.",
            )

        async def iter_body():
            for _, rec in batch.items():
                yield await rec.read_all()

        content_length, record_headers = make_headers_v2(batch)
        _, headers = await self._http.request_all(
            "POST",
            f"/io/{self.name}/write",
            data=iter_body(),
            extra_headers=record_headers,
            content_length=content_length,
        )

        return parse_errors_from_headers_v2(headers)

    async def update(
        self,
        entry_name: str,
        timestamp: TimestampLike,
        labels: dict[str, str],
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
            >>> await bucket.update(
            >>>     "entry-1", "2022-01-01T01:00:00", {"label1": "value1", "label2": ""}
            >>> )

        """
        timestamp = unix_timestamp_from_any(timestamp)
        await self._http.request_all(
            "PATCH", f"/b/{self.name}/{entry_name}?ts={timestamp}", labels=labels
        )

    async def update_batch(
        self, entry_name: str, batch: Batch
    ) -> dict[int, ReductError]:
        """Update labels of existing records

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
            >>> batch = RecordBatch()
            >>> batch.add(
            >>>     "entry-1",
            >>>     1640995200000000,
            >>>     labels={"label1": "value1", "label2": ""},
            >>> )
            >>> await bucket.update_record_batch("entry-1", batch)

        """

        content_length, record_headers = make_headers_v1(batch)
        _, headers = await self._http.request_all(
            "PATCH",
            f"/b/{self.name}/{entry_name}/batch",
            extra_headers=record_headers,
            content_length=content_length,
        )

        return parse_errors_from_headers_v1(headers)

    async def update_record_batch(
        self, batch: RecordBatch
    ) -> dict[str, dict[int, ReductError]]:
        """Update labels of existing records using Multi-entry API
        If a label doesn't exist, it will be created.
        If a label is empty, it will be removed.

        Args:
            batch : dict of entry names as keys and dict of timestamps as keys and
                labels as values
        Returns:
            Dict[str, Dict[int, ReductError]]: the dictionary of errors
                with entry names as keys and dictionaries of record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error

        Examples:
            >>> batch = Batch()
            >>> batch.add(1640995200000000, labels={"label1": "value1", "label2": ""})
            >>> await bucket.update_batch("entry-1", batch)

        """

        if self._http.api_version[1] < 18:
            raise ReductError(
                -1,
                "Multi-entry batch API is not supported by the server. "
                "Requires server version 1.18 or higher.",
            )

        content_length, record_headers = make_headers_v2(batch)
        _, headers = await self._http.request_all(
            "PATCH",
            f"/io/{self.name}/update",
            extra_headers=record_headers,
            content_length=content_length,
        )

        return parse_errors_from_headers_v2(headers)

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    async def query(
        self,
        entries: str | list[str],
        start: TimestampLike | None = None,
        stop: TimestampLike | None = None,
        ttl: int | None = None,
        when: dict | None = None,
        **kwargs,
    ) -> AsyncIterator[Record]:
        """
        Query data for a time interval
        The time interval is defined by the start and stop parameters that can be:
        int (UNIX timestamp in microseconds), datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string),

        Since version 1.18: entry_name can be a list of entries to query from
        multiple entries. You can also use wildcards in entry names.
        Args:
            entries: name(s) of entry in the bucket
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            ttl: Time To Live of the request in seconds
            when: condtion to filter records
        Keyword Args:
            head (bool): if True: get only the header of a recod with metadata
            each_s(Union[int, float]): return a record for each S seconds
                (DEPRECATED use $each_t in when)
            each_n(int): return each N-th record (DEPRECATED use $each_n in when)
            limit (int): limit the number of records (DEPRECATED use $limit in when)
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
        _check_deprecated_params(kwargs)

        async for record in self._query_post(
            entries, QueryType.QUERY, start, stop, when, ttl, **kwargs
        ):
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
        entries: str | list[str],
        start: TimestampLike | None = None,
        poll_interval=1.0,
        when: dict | None = None,
        **kwargs,
    ) -> AsyncIterator[Record]:
        """
        Query records from the start timestamp and wait for new records
        The time interval is defined by the start and stop parameters
        that can be: int (UNIX timestamp in microseconds) datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Since version 1.18: entry_name can be a list of entries to query from
        multiple entries. You can also use wildcards in entry names.

        Args:
            entries: name(s) of entry in the bucket
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
        async for record in self._query_post(
            entries,
            QueryType.QUERY,
            start,
            None,
            when,
            ttl,
            continuous=True,
            poll_interval=poll_interval,
            **kwargs,
        ):
            yield record

    async def create_query_link(
        self,
        entries: str | list[str],
        start: TimestampLike | None = None,
        stop: TimestampLike | None = None,
        when: dict | None = None,
        **kwargs,
    ) -> str:
        """
        Create a link to query data for a time interval

        The time interval is defined by the start and stop parameters that can be:
        int (UNIX timestamp in microseconds), datetime,
        float (UNIX timestamp in seconds) or str (ISO 8601 string).

        Since version 1.18: entry_name can be a list of entries to query from
        multiple entries. You can also use wildcards in entry names.

        Args:
            entries: name(s) of entry in the bucket
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            when: condtiion to filter records

        Keyword Args:
            record_index: if not None, the link will point to a specific record
            expire_at: if None, the link will expire in 24 hours
            file_name: file name for download, if None: entry_name_index.bin
            base_url: base URL for the link, if None: use server URL

        """
        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None

        record_index = kwargs.get("record_index", 0)
        expire_at: datetime | None = kwargs.get("expire_at", None)

        if expire_at is None:
            expire_at = datetime.now() + timedelta(hours=24)

        query_message = QueryEntry(
            query_type=QueryType.QUERY,
            entries=entries if isinstance(entries, list) else [entries],
            start=start,
            stop=stop,
            when=when,
            only_metadata=False,
        )

        query_link_params = CreateQueryLinkRequest(
            bucket=self.name,
            entry=entries if isinstance(entries, str) else "",
            index=record_index,
            query=query_message,
            expire_at=int(expire_at.timestamp()),
            base_url=kwargs.get("base_url", None),
        )

        if isinstance(entries, str):
            entry = entries
        else:
            entry = self.name

        file_name = kwargs.get(
            "file_name",
            (
                f"{entry}_{record_index}.bin"
                if record_index is not None
                else f"{entry}.bin"
            ),
        )

        body, _ = await self._http.request_all(
            "POST",
            f"/links/{file_name}",
            data=query_link_params.model_dump_json(),
            content_type="application/json",
        )

        return CreateQueryLinkResponse.model_validate_json(body).link

    # pylint: disable=too-many-positional-arguments, too-many-arguments, too-many-locals
    async def _query_post(
        self, entries, query_type: QueryType, start, stop, when, ttl, **kwargs
    ):
        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None
        is_continuous = kwargs.get("continuous", False)
        poll_interval = kwargs.get("poll_interval", 1.0)
        query_message = QueryEntry(
            entries=entries if isinstance(entries, list) else [entries],
            query_type=query_type,
            start=start,
            stop=stop,
            when=when,
            ttl=ttl,
            only_metadata=kwargs.get("head", False),
            **kwargs,
        )

        batch_api_v2 = self._http.api_version[1] >= 18

        if batch_api_v2:
            query_url = f"/io/{self.name}/q"
            parse_func = parse_batched_records_v2
        else:
            if isinstance(entries, list):
                raise ReductError(
                    -1,
                    "Multi-entry query is not supported by the server. "
                    "Requires server version 1.18 or higher.",
                )

            query_url = f"/b/{self.name}/{entries}/q"
            parse_func = partial(parse_batched_records_v1, default_entry_name=entries)

        data, _ = await self._http.request_all(
            "POST",
            query_url,
            data=query_message.model_dump_json(),
            content_type="application/json",
        )
        query_id = json.loads(data)["id"]

        method = "HEAD" if kwargs.pop("head", False) else "GET"
        last = False
        while not last:
            if batch_api_v2:
                fetch_url = f"/io/{self.name}/read"
                extra_headers = {"x-reduct-query-id": str(query_id)}
            else:
                fetch_url = f"/b/{self.name}/{entries}/batch?q={query_id}"
                extra_headers = {}

            async with self._http.request(
                method, fetch_url, extra_headers=extra_headers
            ) as resp:
                if resp.status == 204:
                    if is_continuous:
                        await asyncio.sleep(poll_interval)
                        continue
                    return

                async for record in parse_func(resp):
                    last = record.last and not is_continuous
                    yield record

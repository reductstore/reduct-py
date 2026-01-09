"""Bucket module for ReductStore HTTP API"""

from __future__ import annotations

import asyncio
import json
import re
import time
import warnings
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import (
    AsyncIterator,
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
from reduct.record import (
    Record,
    parse_batched_records,
    parse_record,
    Batch,
    BatchItem,
)
from reduct.batch import ERROR_PREFIX, make_headers_v1, make_headers_v2
from reduct.time import unix_timestamp_from_any, TimestampLike


def _check_deprecated_params(kwargs):
    if "each_s" in kwargs:
        warnings.warn(
            "The 'each_s' argument is deprecated and will be removed in v1.18.0,"
            " use '$each_t' in 'when' instead.",
            DeprecationWarning,
        )
    if "each_n" in kwargs:
        warnings.warn(
            "The 'each_n' argument is deprecated and will be removed in v1.18.0,"
            " use '$each_n' in 'when' instead.",
            DeprecationWarning,
        )
    if "limit" in kwargs:
        warnings.warn(
            "The 'limit' argument is deprecated and will be removed in v1.18.0,"
            " use '$limit' in 'when' instead.",
            DeprecationWarning,
        )


def _parse_entry_list(entry_name: str) -> list[str]:
    """Parse comma or whitespace separated entry names preserving order."""
    entries = [part for part in re.split(r"[,\s]+", entry_name) if part]
    if not entries:
        raise ValueError("Entry name must not be empty")
    return entries


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
            batch: list of timestamps. Items without an explicit entry use
                ``entry_name`` as the default.
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with  record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error. Multi-entry batches require
            server >= 1.18 (batch protocol v2); older servers use the legacy
            single-entry endpoint.
        """
        content_length, record_headers, _, protocol = self._make_headers(
            entry_name, batch
        )
        path = self._batch_path(protocol, entry_name, "remove")
        _, headers = await self._http.request_all(
            "DELETE",
            path,
            extra_headers=record_headers,
            content_length=content_length,
        )

        return self._parse_errors_from_headers(headers)

    async def remove_query(
        self,
        entry_name: str,
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

        Args:
            entry_name: name of entry in the bucket. Comma or whitespace separated
                lists use the multi-entry API when supported (server >= 1.18).
            start: the beginning of the time interval.
                If None, then from the first record
            stop: the end of the time interval. If None, then to the latest record
            when: condtion to filter
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

        entries = _parse_entry_list(entry_name)
        use_v2 = self._should_use_v2(entries)
        head = kwargs.pop("head", False)
        query_id = await self._query_post(
            entries,
            QueryType.REMOVE,
            start,
            stop,
            when,
            None,
            use_v2,
            head=head,
            **kwargs,
        )

        url = f"/io/{self.name}/read" if use_v2 else f"/b/{self.name}/{entry_name}/batch"
        extra_headers = {"x-reduct-query-id": query_id} if use_v2 else None
        resp, _ = await self._http.request_all(
            "HEAD" if head else "GET",
            url,
            extra_headers=extra_headers,
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
            yield parse_record(resp)

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
            batch: list of records. Items without an explicit entry use
                ``entry_name`` as the default.
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with record timestamps as keys
        Raises:
            ReductError: if there is an HTTP  or communication error. Multi-entry
            batches require server >= 1.18 (batch protocol v2); older servers use
            the legacy single-entry endpoint.
        """

        content_length, record_headers, items, protocol = self._make_headers(
            entry_name, batch
        )
        path = self._batch_path(protocol, entry_name, "write")

        async def iter_body():
            for record in items:
                if record.data:
                    yield record.data

        _, headers = await self._http.request_all(
            "POST",
            path,
            data=iter_body(),
            extra_headers=record_headers,
            content_length=content_length,
        )

        return self._parse_errors_from_headers(headers)

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
            >>> await bucket.update("entry-1", "2022-01-01T01:00:00",
                    {"label1": "value1", "label2": ""})

        """
        timestamp = unix_timestamp_from_any(timestamp)
        await self._http.request_all(
            "PATCH", f"/b/{self.name}/{entry_name}?ts={timestamp}", labels=labels
        )

    async def update_batch(
        self, entry_name: str, batch: Batch
    ) -> dict[int, ReductError]:
        """Update labels of existing records
        If a label doesn't exist, it will be created.
        If a label is empty, it will be removed.

        Args:
            entry_name: name of entry in the bucket
            batch: dict of timestamps as keys and labels as values. Items without an
                explicit entry use ``entry_name`` as the default.
        Returns:
            Dict[int, ReductError]: the dictionary of errors
                with record timestamps as keys
        Raises:
            ReductError: if there is an HTTP error. Multi-entry batches require
            server >= 1.18 (batch protocol v2); older servers use the legacy
            single-entry endpoint.

        Examples:
            >>> batch = Batch()
            >>> batch.add(1640995200000000, labels={"label1": "value1", "label2": ""})
            >>> await bucket.update_batch("entry-1", batch)

        """

        content_length, record_headers, _, protocol = self._make_headers(
            entry_name, batch
        )
        path = self._batch_path(protocol, entry_name, "update")
        _, headers = await self._http.request_all(
            "PATCH",
            path,
            extra_headers=record_headers,
            content_length=content_length,
        )

        return self._parse_errors_from_headers(headers)

    async def query(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        entry_name: str,
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
        Args:
            entry_name: name of entry in the bucket. Comma or whitespace separated
                lists use the multi-entry API when supported (server >= 1.18).
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

        entries = _parse_entry_list(entry_name)
        use_v2 = self._should_use_v2(entries)

        head = kwargs.pop("head", False)

        query_id = await self._query_post(
            entries, QueryType.QUERY, start, stop, when, ttl, use_v2, head=head, **kwargs
        )

        last = False
        method = "HEAD" if head else "GET"

        while not last:
            if use_v2:
                async with self._http.request(
                    method,
                    f"/io/{self.name}/read",
                    extra_headers={"x-reduct-query-id": query_id},
                ) as resp:
                    if resp.status == 204:
                        return
                    async for record in parse_batched_records(resp):
                        last = record.last
                        yield record
            else:
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

        Args:
            entry_name: name of entry in the bucket. Comma or whitespace separated
                lists use the multi-entry API when supported (server >= 1.18).
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
        entries = _parse_entry_list(entry_name)
        use_v2 = self._should_use_v2(entries)

        head = kwargs.pop("head", False)
        query_id = await self._query_post(
            entries,
            QueryType.QUERY,
            start,
            None,
            when,
            ttl,
            use_v2,
            head=head,
            continuous=True,
            **kwargs,
        )

        method = "HEAD" if head else "GET"
        while True:
            if use_v2:
                async with self._http.request(
                    method,
                    f"/io/{self.name}/read",
                    extra_headers={"x-reduct-query-id": query_id},
                ) as resp:
                    if resp.status == 204:
                        await asyncio.sleep(poll_interval)
                        continue

                    async for record in parse_batched_records(resp):
                        yield record
            else:
                async with self._http.request(
                    method, f"/b/{self.name}/{entry_name}/batch?q={query_id}"
                ) as resp:
                    if resp.status == 204:
                        await asyncio.sleep(poll_interval)
                        continue

                    async for record in parse_batched_records(resp):
                        yield record

    async def create_query_link(
        self,
        entry: str,
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

        Args:
            entry: name of entry in the bucket
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
            start=start,
            stop=stop,
            when=when,
            only_metadata=False,
        )

        query_link_params = CreateQueryLinkRequest(
            bucket=self.name,
            entry=entry,
            index=record_index,
            query=query_message,
            expire_at=int(expire_at.timestamp()),
            base_url=kwargs.get("base_url", None),
        )

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

    async def _query_post(  # pylint: disable=too-many-positional-arguments, too-many-arguments
        self,
        entries: list[str],
        query_type: QueryType,
        start,
        stop,
        when,
        ttl,
        use_v2: bool,
        head: bool = False,
        **kwargs,
    ):
        start = unix_timestamp_from_any(start) if start else None
        stop = unix_timestamp_from_any(stop) if stop else None
        query_message = QueryEntry(
            query_type=query_type,
            start=start,
            stop=stop,
            when=when,
            ttl=ttl,
            only_metadata=head,
            **kwargs,
        )
        payload = query_message.model_dump(mode="json", exclude_none=True)
        if use_v2:
            payload["entries"] = entries
            url = f"/io/{self.name}/q"
        else:
            url = f"/b/{self.name}/{entries[0]}/q"

        data, _ = await self._http.request_all(
            "POST",
            url,
            data=json.dumps(payload),
            content_type="application/json",
        )
        query_id = json.loads(data)["id"]
        return query_id

    def _make_headers(
        self, default_entry: str, batch: Batch
    ) -> tuple[int, dict[str, str], list[BatchItem], str]:
        """Make headers for batch using protocol v1 or v2 depending on entries."""
        items = batch.sorted_items(default_entry)
        entries = {record.entry for record in items}
        protocol = self._select_batch_protocol(len(entries))
        if protocol == "v2":
            content_length, record_headers = make_headers_v2(items)
        else:
            content_length, record_headers = make_headers_v1(items)

        return content_length, record_headers, items, protocol

    def _supports_v2(self) -> bool:
        """Return True when server API version is >=1.18."""
        api_version = self._http.api_version
        if api_version is None:
            return False

        major, minor = api_version
        return major > 1 or (major == 1 and minor >= 18)

    def _should_use_v2(self, entries: list[str]) -> bool:
        """Decide whether to use v2 query protocol."""
        supports_v2 = self._supports_v2()
        if len(entries) > 1 and not supports_v2:
            raise ReductError(
                400,
                "Batch/query protocol v2 is required for multiple entries; "
                "update the server to >= 1.18.0",
            )
        return supports_v2 or len(entries) > 1

    def _select_batch_protocol(self, entry_count: int) -> str:
        """Choose batch protocol based on server version and entry count."""
        if entry_count > 1:
            if not self._supports_v2():
                raise ReductError(
                    400,
                    "Batch protocol v2 is required for multiple entries; "
                    "update the server to >= 1.18.0",
                )
            return "v2"

        return "v1"

    def _batch_path(self, protocol: str, entry_name: str, operation: str) -> str:
        """Resolve batch endpoint path based on protocol."""
        if protocol == "v2":
            return f"/io/{self.name}/{operation}"
        return f"/b/{self.name}/{entry_name}/batch"

    @staticmethod
    def _parse_errors_from_headers(headers):
        errors = {}
        for key, value in headers.items():
            if key.startswith(ERROR_PREFIX):
                errors[int(key[len(ERROR_PREFIX) :])] = ReductError.from_header(value)
        return errors

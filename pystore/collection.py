#!/usr/bin/env python
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2020 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PyStore Collection module"""

import os
import shutil
import time

import dask.dataframe as dd
import multitasking
from pandas import DataFrame

from . import config, utils
from .item import Item


class Collection:
    """PyStore Collection"""

    def __repr__(self):
        cls = self.__class__.__name__
        return f"{cls}(collection={self.collection!r}, datastore={self.datastore!r})"

    def __init__(self, collection, datastore, engine="pyarrow"):
        self.engine = engine
        self.datastore = datastore
        self.collection = collection
        self.path = utils.make_path(self.datastore, self.collection)
        self.items = self.list_items()
        self.snapshots = self.list_snapshots()
        self.metadata = utils.read_metadata(self.path)

    def __getitem__(self, item: Item) -> Item:
        if item not in self.items:
            raise KeyError(f"Item {item} not in collection {self.collection}")
        return self.item(item)

    def __len__(self):
        return len(self.items)

    def __contains__(self, item):
        return item in self.items

    def __iter__(self):
        return iter(self.items)

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.path, item)
        if as_string:
            return str(p)
        return p

    @multitasking.task
    def _list_items_threaded(self, **kwargs):
        self.items = self.list_items(**kwargs)

    def list_items(self, **kwargs):
        dirs = utils.subdirs(self.path)
        if not kwargs:
            return set(dirs)

        matched = []
        for d in dirs:
            meta = utils.read_metadata(utils.make_path(self.path, d))
            del meta["_updated"]

            m = 0
            keys = list(meta.keys())
            for k, v in kwargs.items():
                if k in keys and meta[k] == v:
                    m += 1

            if m == len(kwargs):
                matched.append(d)

        return set(matched)

    def item(self, item, snapshot=None, filters=None, columns=None):
        """Item factory method"""
        return Item(
            item,
            self.datastore,
            self.collection,
            snapshot,
            filters,
            columns,
            engine=self.engine,
        )

    def index(self, item, last=False):
        data = dd.read_parquet(
            self._item_path(item, as_string=True), columns=[], engine=self.engine
        )
        if not last:
            return data.index.compute()

        return float(str(data.index).split("\nName")[0].split("\n")[-1].split(" ")[0])

    def delete_item(self, item: str, reload_items: bool = False) -> bool:
        """delete item from collection

        Args:
            item (str): item name
            reload_items (bool, optional): refresh self.items. Defaults to False.

        Returns:
            bool: True on success
        """
        return self._raw_delete(item, reload_items)

    def _raw_delete(
        self,
        item: str,
        reload_items: bool = False,
        update_items: bool = True,
    ) -> bool:
        """delete internal implementation

        Args:
            item (str): item name
            reload_items (bool): call self._list_items_threaded
            update_items (bool, optional): call self.items.add(item). Defaults to True.

        Returns:
            bool: True on success
        """
        shutil.rmtree(self._item_path(item))
        if update_items:
            self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        return True

    @multitasking.task
    def write_threaded(
        self,
        item,
        data,
        metadata={},
        npartitions=None,
        overwrite=False,
        epochdate=False,
        reload_items=False,
        **kwargs,
    ):
        return self.write(
            item,
            data,
            metadata,
            npartitions,
            overwrite,
            epochdate,
            reload_items,
            **kwargs,
        )

    def write(
        self,
        item: str,
        data: DataFrame | dd.DataFrame,
        metadata: dict = {},
        npartitions: int = None,
        overwrite=False,
        epochdate=False,
        reload_items=False,
        **kwargs,
    ):
        """write item

        Args:
            item (str): item name
            data (DataFrame | dd.DataFrame): data to write
            metadata (dict, optional): additional metadata. Defaults to {}.
            npartitions (int, optional): partition number. Defaults to None.
            overwrite (bool, optional): overwrite existing item. Defaults to False.
            epochdate (bool, optional): index in epochdate format. Defaults to False.
            reload_items (bool, optional): reload collection items from storage.
            Defaults to False.

        """
        if utils.path_exists(self._item_path(item)) and not overwrite:
            raise ValueError(
                """
                Item already exists. To overwrite, use `overwrite=True`.
                Otherwise, use `<collection>.append()`"""
            )

        if isinstance(data, Item):
            data = data.to_pandas()
        else:
            # work on copy
            data = data.copy()

        if epochdate or "datetime" in str(data.index.dtype):
            data = utils.datetime_to_int64(data)
            if (1 == data.index.nanosecond).any() and "times" not in kwargs:
                kwargs["times"] = "int96"

        if data.index.name == "":
            data.index.name = "index"

        if npartitions is None:
            memusage = data.memory_usage(deep=True).sum()
            if isinstance(data, dd.DataFrame):
                npartitions = int(1 + memusage.compute() // config.PARTITION_SIZE)
                data = data.repartition(npartitions=npartitions)
            else:
                npartitions = int(1 + memusage // config.PARTITION_SIZE)
                data = dd.from_pandas(data, npartitions=npartitions)
        else:
            if not isinstance(data, dd.DataFrame):
                data = dd.from_pandas(data, npartitions=npartitions)

        self._raw_write(
            item=item,
            data=data,
            overwrite=overwrite,
            metadata=metadata,
            reload_items=reload_items,
            **kwargs,
        )

    def _raw_write(
        self,
        item: str,
        data: dd.DataFrame,
        overwrite: bool,
        metadata: dict,
        reload_items: bool,
        append: bool = False,
        append_item: str | None = None,
        update_items: bool = True,
        **kwargs,
    ):
        """private method for writing dask data frames"

        Args:
            item (str): item
            data (dd.DataFrame): dask data frame
            overwrite (bool): boll
            metadata (dict): metadata
            reload_items (bool): call self._list_items_threaded
            update_items (bool, optional): call self.items.add(item). Defaults to True.
        """
        if append and append_item is None:
            raise ValueError(
                f"Invalid parameter combination, append{append} and "
                f"append_item {append_item}."
            )

        dd.to_parquet(
            data,
            self._item_path(item, as_string=True),
            overwrite=overwrite,
            compression="snappy",
            engine=self.engine,
            **kwargs,
        )

        utils.write_metadata(utils.make_path(self.path, item), metadata)

        if append and append_item is not None:
            # on append swith "__item" to "item"
            self._raw_delete(item=append_item, reload_items=False, update_items=False)
            shutil.move(self._item_path(item), self._item_path(append_item))

        # update items
        if update_items:
            self.items.add(item)
        if reload_items:
            self._list_items_threaded()

    def append(
        self,
        item: str,
        data: DataFrame | dd.DataFrame,
        npartitions: int | None = None,
        epochdate=False,
        threaded=False,
        **kwargs,
    ):
        """append data to item

        Args:
            item (str): item name
            data (DataFrame | dd.DataFrame): dataframe with data
            npartitions (int, optional): number of partitions. Defaults to None.
            epochdate (bool, optional): index in epochdate format. Defaults to False.
            threaded (bool, optional): write in thread. Defaults to False.

        Raises:
            ValueError: _description_
        """
        if not utils.path_exists(self._item_path(item)):
            raise ValueError("""Item do not exists. Use `<collection>.write(...)`""")

        # work on copy
        data = data.copy()

        try:
            if epochdate or (
                "datetime" in str(data.index.dtype) and any(data.index.nanosecond) > 0
            ):
                data = utils.datetime_to_int64(data)
            old_index = dd.read_parquet(
                self._item_path(item, as_string=True), columns=[], engine=self.engine
            ).index.compute()
            data = data[~data.index.isin(old_index)]
        except Exception:
            return

        if data.empty:
            return

        if data.index.name == "":
            data.index.name = "index"

        # combine old dataframe with new
        current = self.item(item)
        new = dd.from_pandas(data, npartitions=1)
        combined = dd.concat([current.data, new])

        if npartitions is None:
            memusage = combined.memory_usage(deep=True).sum()
            if isinstance(combined, dd.DataFrame):
                memusage = memusage.compute()
            npartitions = int(1 + memusage // config.PARTITION_SIZE)

        combined = combined.repartition(npartitions=npartitions).drop_duplicates(
            keep="last"
        )
        tmp_item = "__" + item
        # write data
        _write = multitasking.task(self._raw_write) if threaded else self._raw_write
        _write(
            item=tmp_item,
            data=combined,
            overwrite=False,
            metadata=current.metadata,
            reload_items=False,
            append=True,
            append_item=item,
            update_items=False,
            **kwargs,
        )

    def create_snapshot(self, snapshot: str | None = None) -> bool:
        """create snapshot

        Args:
            snapshot (str|None, optional): snapshot name.
            if None name time.time() * 10e6.

        Returns:
            boll: True on success
        """
        if snapshot:
            snapshot = "".join(e for e in snapshot if e.isalnum() or e in [".", "_"])
        else:
            snapshot = str(int(time.time() * 1000000))

        dst = utils.make_path(self.path, "_snapshots", snapshot)

        shutil.copytree(self.path, dst, ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        """list snapshots

        Returns:
            set: snapshots names
        """
        snapshots = utils.subdirs(utils.make_path(self.path, "_snapshots"))
        return set(snapshots)

    def delete_snapshot(self, snapshot: str):
        """delete snapshot

        Args:
            snapshot (str): snapshot name

        Returns:
            bool: True on success
        """
        if snapshot not in self.snapshots:
            # raise ValueError("Snapshot `%s` doesn't exist" % snapshot)
            return True

        shutil.rmtree(utils.make_path(self.path, "_snapshots", snapshot))
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        """delete all snapshots at store level"""
        snapshots_path = utils.make_path(self.path, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True

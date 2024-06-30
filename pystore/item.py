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

"""PyStore Item module"""

import dask.dataframe as dd
import pandas as pd

from . import utils


class Item:
    """PyStore Item"""

    def __repr__(self):
        cls = self.__class__.__name__
        return (
            f"{cls}(item={self.item!r}, datastore={self.datastore!r}, "
            f"collection={self.collection!r})"
        )

    def __init__(
        self,
        item,
        datastore,
        collection,
        snapshot=None,
        filters=None,
        columns=None,
        engine="pyarrow",
    ):
        self.engine = engine
        self.datastore = datastore
        self.collection = collection
        self.snapshot = snapshot
        self.item = item

        self.path = utils.make_path(datastore, collection, item)
        if not self.path.exists():
            raise ValueError(
                f"Item `{item}` doesn't exist. "
                f"Create it using collection.write(`{item}`, data, ...)"
            )
        if snapshot:
            snap_path = utils.make_path(datastore, collection, "_snapshots", snapshot)

            self.path = utils.make_path(snap_path, item)

            if not utils.path_exists(snap_path):
                raise ValueError(f"Snapshot `{snapshot}` doesn't exist")

            if not utils.path_exists(self.path):
                raise ValueError(f"Item `{item}` doesn't exist in this snapshot")

        self.metadata = utils.read_metadata(self.path)
        self.data = dd.read_parquet(
            self.path, engine=self.engine, filters=filters, columns=columns
        )

    def to_pandas(self, parse_dates=True) -> pd.DataFrame:
        """return pandas data frame

        Args:
            parse_dates (bool, optional): parse dates in epoch format. Defaults to True.

        Returns:
            _type_: _description_
        """
        df = self.data.compute()

        if parse_dates and "datetime" not in str(df.index.dtype):
            df.index.name = ""
            if str(df.index.dtype) == "float64":
                df.index = pd.to_datetime(
                    df.index, unit="s", infer_datetime_format=True
                )
            elif df.index.values[0] > 1e6:
                df.index = pd.to_datetime(df.index, infer_datetime_format=True)

        return df

    def head(self, n=5) -> dd.DataFrame:
        """data frame head

        Args:
            n (int, optional): top n records. Defaults to 5.

        Returns:
            dd.DataFrame: top n records
        """
        return self.data.head(n)

    def tail(self, n=5) -> dd.DataFrame:
        """data frame tail

        Args:
            n (int, optional): last n records. Defaults to 5.

        Returns:
            dd.DataFrame: last n records
        """
        return self.data.tail(n)

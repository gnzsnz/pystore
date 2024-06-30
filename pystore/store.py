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

"""PyStore Store module"""

import os
import shutil
from typing import Any

from . import utils
from .collection import Collection
from .item import Item


class Store:
    """Pystore"""

    def __repr__(self):
        cls = self.__class__.__name__
        return f"{cls}(datastore={self.datastore.name!r})"

    def __init__(self, datastore, engine="pyarrow"):

        datastore_path = utils.get_path()
        if not utils.path_exists(datastore_path):
            os.makedirs(datastore_path)

        self.datastore = utils.make_path(datastore_path, datastore)
        if not utils.path_exists(self.datastore):
            os.makedirs(self.datastore)
            utils.write_metadata(self.datastore, {"engine": engine})
            self.engine = engine
            self.metadata = utils.read_metadata(self.datastore)
        else:
            self.metadata = utils.read_metadata(self.datastore)
            if self.metadata["engine"] == engine:
                self.engine = self.metadata["engine"]
            else:
                # default / backward compatibility
                self.engine = "pyarrow"
                # utils.write_metadata(self.datastore, {"engine": self.engine})

        self.collections = self.list_collections()

    def __getitem__(self, collection: Collection) -> Collection:
        if collection not in self.collections:
            raise KeyError(f"Key not in store {self.datastore}")
        return self.collection(collection)

    def __len__(self):
        return len(self.collections)

    def __contains__(self, collection):
        return collection in self.collections

    def __iter__(self):
        return iter(self.collections)

    def _create_collection(self, collection, metadata: dict, overwrite=False):
        # create collection (subdir)
        collection_path = utils.make_path(self.datastore, collection)
        if utils.path_exists(collection_path):
            if overwrite:
                self.delete_collection(collection)
            else:
                raise ValueError(
                    "Collection exists! To overwrite, use `overwrite=True`"
                )

        os.makedirs(collection_path)
        os.makedirs(utils.make_path(collection_path, "_snapshots"))

        # update collections
        self.collections = self.list_collections()
        utils.write_metadata(collection_path, metadata)

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection: str) -> bool:
        """delete collection

        Args:
            collection (str): collection name

        Returns:
            bool: True on success
        """
        # delete collection (subdir)
        shutil.rmtree(utils.make_path(self.datastore, collection))

        # update collections
        self.collections = self.list_collections()
        return True

    def list_collections(self) -> list[str | Any]:
        """list store collections by reading from storage

        Returns:
            list[str|Any]: list with collections
        """
        # lists collections (subdirs)
        return utils.subdirs(self.datastore)

    def collection(
        self, collection: str, metadata: dict = None, overwrite=False
    ) -> Collection:
        """collection instance

        Args:
            collection (str): collection name
            metadata (dict, optional): collection metadata . Defaults to None.
            overwrite (bool, optional): overwrite existing collection.
            Defaults to False.

        Returns:
            Collection: collection instance
        """
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore, self.engine)

        # create collection
        _meta = metadata if metadata else {}
        self._create_collection(collection, _meta, overwrite)
        return Collection(collection, self.datastore, self.engine)

    def item(self, collection: str, item: str) -> Item:
        """return item instance

        Args:
            collection (str): collection name
            item (str): item name

        Returns:
            Item: item instance
        """
        # bypasses collection
        return self.collection(collection).item(item)

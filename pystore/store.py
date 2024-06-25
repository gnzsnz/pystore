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

import os
import shutil

from . import utils
from .collection import Collection


class store:
    """Pystore"""

    def __repr__(self):
        return f"PyStore.datastore <{self.datastore}>"

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
        if metadata:
            utils.write_metadata(collection_path, metadata)

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection):
        # delete collection (subdir)
        shutil.rmtree(utils.make_path(self.datastore, collection))

        # update collections
        self.collections = self.list_collections()
        return True

    def list_collections(self):
        # lists collections (subdirs)
        return utils.subdirs(self.datastore)

    def collection(
        self, collection: Collection, metadata: dict = None, overwrite=False
    ) -> Collection:
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore, self.engine)

        # create it
        self._create_collection(collection, metadata, overwrite)
        return Collection(collection, self.datastore, self.engine)

    def item(self, collection, item):
        # bypasses collection
        return self.collection(collection).item(item)

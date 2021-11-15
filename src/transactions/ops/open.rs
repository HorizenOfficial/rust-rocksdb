// Copyright 2019 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::path::Path;

use crate::transactions::open_raw::{OpenRaw, OpenRawInput};
use crate::{ColumnFamilyDescriptor, Error, Options};
use crate::ffi_util::to_cpath;
use std::ffi::CStr;
use core::slice;

pub trait Open: OpenRaw {
    /// Open a database with default options.
    fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Self::open(&opts, path)
    }

    /// Open the database with the specified options.
    fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Self::open_with_descriptor(opts, path, Self::Descriptor::default())
    }

    fn open_with_descriptor<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        descriptor: Self::Descriptor,
    ) -> Result<Self, Error> {
        let input = OpenRawInput {
            options: opts,
            path: path.as_ref(),
            column_families: vec![],
            open_descriptor: descriptor,
        };

        Self::open_raw(input)
    }
}

pub trait OpenCF: OpenRaw {
    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        Self::open_cf_descriptors(opts, path, cfs)
    }

    /// Open a database with the given database options and column family descriptors.
    fn open_cf_descriptors<P, I>(opts: &Options, path: P, cfs: I) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        Self::open_cf_descriptors_with_descriptor(opts, path, cfs, Self::Descriptor::default())
    }

    fn open_cf_descriptors_with_descriptor<P, I>(
        opts: &Options,
        path: P,
        cfs: I,
        descriptor: Self::Descriptor,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let input = OpenRawInput {
            options: opts,
            path: path.as_ref(),
            column_families: cfs.into_iter().collect(),
            open_descriptor: descriptor,
        };

        Self::open_raw(input)
    }

    fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = to_cpath(path)?;
        let mut length = 0;

        unsafe {
            let ptr = ffi_try!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length,
            ));

            let vec = slice::from_raw_parts(ptr, length)
                .iter()
                .map(|ptr| CStr::from_ptr(*ptr).to_string_lossy().into_owned())
                .collect();
            ffi::rocksdb_list_column_families_destroy(ptr, length);
            Ok(vec)
        }
    }

    fn open_cf_default<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Self::open_cf(&opts, path, vec!["default"])
    }

    /// Open a database with the given database options.
    /// All existing column family names are preliminarily retrieved from DB.
    /// Column families opened using this function will be created with default `Options`.
    /// Database should be existent for successful 'list_cf' call
    fn open_cf_all<P>(opts: &Options, path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let cfs_names = Self::list_cf(&Options::default(), &path)?;

        let cfs = cfs_names
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));

        Self::open_cf_descriptors(opts, path, cfs)
    }
}

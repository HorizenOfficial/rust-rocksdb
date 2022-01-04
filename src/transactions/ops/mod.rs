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

// PIGMED operations (Put, Iterate, Get, Merge, Delete)
//----------------------
// transaction.rs
//----------------------
mod delete;
mod get;
mod merge;
mod put;
mod iter;
//----------------------
mod writebatch;
mod open;
mod columnfamily;
mod checkpoint;
mod transaction;

/// Marker trait for operations that leave DB
/// state unchanged
pub trait Read {}

/// Marker trait for operations that mutate
/// DB state
pub trait Write {}

pub use self::delete::{Delete, DeleteCF};
pub use self::get::{Get, GetCF};
pub use self::merge::{Merge, MergeCF};
pub use self::put::{Put, PutCF};
pub use self::iter::{Iterate, IterateCF};
pub use self::writebatch::WriteOps;
pub use self::open::{Open, OpenCF};
pub use self::columnfamily::{CreateCf, DropCf};
pub use self::columnfamily::GetColumnFamilies;
pub use self::transaction::TransactionBegin;
pub use self::checkpoint::CreateCheckpointObject;

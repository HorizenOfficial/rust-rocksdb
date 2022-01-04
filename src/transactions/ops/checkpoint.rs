use crate::{checkpoint::Checkpoint, transactions::handle::Handle, Error};
use ffi;

pub trait CreateCheckpointObject {
    unsafe fn create_checkpoint_object_raw(&self) -> Result<*mut ffi::rocksdb_checkpoint_t, Error>;
    fn create_checkpoint_object(&self) -> Result<Checkpoint, Error> {
        unsafe {
            Checkpoint::new_raw(self.create_checkpoint_object_raw()?)
        }
    }
}

impl<T> CreateCheckpointObject for T
where
    T: Handle<ffi::rocksdb_t>,
{
    unsafe fn create_checkpoint_object_raw(&self) -> Result<*mut ffi::rocksdb_checkpoint_t, Error> {
        Ok(ffi_try!(ffi::rocksdb_checkpoint_object_create(
            self.handle(),
        )))
    }
}

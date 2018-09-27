use serde_json;

use semaphore_common::processor::chunks;
use semaphore_common::v8::Remark;

use core::SemaphoreStr;

ffi_fn! {
    unsafe fn semaphore_split_chunks(
        string: *const SemaphoreStr,
        remarks: *const SemaphoreStr
    ) -> Result<SemaphoreStr> {
        let remarks: Vec<Remark> = serde_json::from_str((*remarks).as_str())?;
        let chunks = chunks::split((*string).as_str(), &remarks);
        let json = serde_json::to_string(&chunks)?;
        Ok(json.into())
    }
}

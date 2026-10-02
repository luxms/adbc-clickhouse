#![cfg(feature = "ffi")]

use adbc_core::constants::{ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK, ADBC_VERSION_1_1_0};
use adbc_ffi::{FFI_AdbcDriver, FFI_AdbcError};

#[test]
fn ffi_initializes_adbc_110_driver() {
    let mut driver = FFI_AdbcDriver::default();
    let mut error = FFI_AdbcError::default();
    // SAFETY: the driver and error pointers refer to correctly aligned, live
    // output structures. The driver owns its state and releases it on drop.
    let status = unsafe {
        adbc_clickhouse::ClickhouseDriverInit(
            ADBC_VERSION_1_1_0,
            std::ptr::from_mut(&mut driver).cast(),
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(driver.release.is_some());
    assert!(driver.ConnectionGetInfo.is_some());
    assert!(driver.ConnectionGetTableSchema.is_some());
    assert!(driver.StatementExecuteQuery.is_some());
}

#[test]
fn ffi_fallback_rejects_unsupported_adbc_version() {
    let mut driver = FFI_AdbcDriver::default();
    let mut error = FFI_AdbcError::default();
    // SAFETY: both output structures remain live for the duration of this call.
    let status = unsafe {
        adbc_clickhouse::AdbcDriverInit(
            adbc_core::constants::ADBC_VERSION_1_0_0,
            std::ptr::from_mut(&mut driver).cast(),
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_NOT_IMPLEMENTED);
    assert!(driver.release.is_none());
}

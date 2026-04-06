use std::{ffi::CString, ptr};

use crate::ffi::{
    error::{error_free, Error, ErrorType},
    future::{
        client::{
            mongoc_future_client_destroy,
            mongoc_future_client_free,
            mongoc_future_client_new,
            mongoc_future_client_tick,
        },
        future::{mongoc_future_destroy, mongoc_future_get_void, mongoc_future_is_finished},
    },
    types::ConnectionSettings,
};

fn make_conn_settings(hosts: &CString) -> ConnectionSettings {
    ConnectionSettings {
        hosts: hosts.as_ptr(),
        app_name: ptr::null(),
        compressors: ptr::null(),
        direct_connection: false,
        load_balanced: false,
        max_pool_size: -1,
        min_pool_size: -1,
        max_idle_time_ms: -1,
        connect_timeout_ms: -1,
        socket_timeout_ms: -1,
        server_selection_timeout_ms: -1,
        local_threshold_ms: -1,
        heartbeat_frequency_ms: -1,
        replica_set: ptr::null(),
        read_preference_mode: 0,
        srv_service_name: ptr::null(),
        srv_max_hosts: -1,
    }
}

#[test]
fn test_client_new_null_settings() {
    unsafe {
        let mut error: *mut Error = ptr::null_mut();
        let client = mongoc_future_client_new(
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            &mut error,
        );
        assert!(client.is_null(), "Client should be null for null settings");
        assert!(!error.is_null(), "Error should be set");
        assert_eq!((*error).error_type, ErrorType::InvalidArgument as u8);
        error_free(error);
    }
}

#[test]
fn test_client_new_valid() {
    let hosts = CString::new("localhost:27017").unwrap();
    let settings = make_conn_settings(&hosts);
    unsafe {
        let client = mongoc_future_client_new(
            &settings,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null_mut(),
        );
        assert!(!client.is_null(), "Client creation should succeed");
        mongoc_future_client_free(client);
    }
}

#[test]
fn test_client_tick_null() {
    unsafe {
        // Should not crash
        mongoc_future_client_tick(ptr::null_mut());
    }
}

#[test]
fn test_client_destroy_null() {
    unsafe {
        let future = mongoc_future_client_destroy(ptr::null_mut());
        assert!(!future.is_null(), "Should return a completed void future");
        assert!(mongoc_future_is_finished(future));

        let mut error_out: *mut Error = ptr::null_mut();
        let ok = mongoc_future_get_void(future, &mut error_out);
        assert!(ok, "Void future for null client should succeed");
        assert!(error_out.is_null());

        mongoc_future_destroy(future);
    }
}

#[test]
fn test_client_free_null() {
    unsafe {
        // Should not crash
        mongoc_future_client_free(ptr::null_mut());
    }
}

use std::{ffi::CString, ptr};

use crate::ffi::{
    error::{error_free, Error, ErrorType},
    future::{
        client::{mongoc_future_client_free, mongoc_future_client_new},
        find_one::mongoc_future_find_one_and_delete,
        future::{mongoc_future_destroy, mongoc_future_get_document, mongoc_future_is_finished},
    },
    types::{ConnectionSettings, OwnedBson},
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
fn test_find_one_and_delete_null_filter() {
    let hosts = CString::new("localhost:27017").unwrap();
    let db = CString::new("test").unwrap();
    let coll = CString::new("c").unwrap();
    let settings = make_conn_settings(&hosts);
    unsafe {
        let client = mongoc_future_client_new(
            &settings,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null_mut(),
        );
        assert!(!client.is_null());

        let future = mongoc_future_find_one_and_delete(
            client,
            ptr::null(),
            db.as_ptr(),
            coll.as_ptr(),
            ptr::null(), // null filter
            ptr::null(),
        );
        assert!(!future.is_null());
        assert!(mongoc_future_is_finished(future));

        let mut result_out: *const OwnedBson = ptr::null();
        let mut error_out: *mut Error = ptr::null_mut();
        let ok = mongoc_future_get_document(future, &mut result_out, &mut error_out);
        assert!(!ok);
        assert!(!error_out.is_null());
        assert_eq!((*error_out).error_type, ErrorType::InvalidArgument as u8);

        error_free(error_out);
        mongoc_future_destroy(future);
        mongoc_future_client_free(client);
    }
}

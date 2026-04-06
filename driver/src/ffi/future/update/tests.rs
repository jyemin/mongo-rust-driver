use std::{ffi::CString, ptr};

use crate::{
    bson::doc,
    ffi::{
        error::{error_free, Error, ErrorType},
        future::{
            client::{mongoc_future_client_free, mongoc_future_client_new},
            future::{mongoc_future_destroy, mongoc_future_get_update, mongoc_future_is_finished},
            update::{mongoc_future_replace_one, mongoc_future_update_one},
        },
        ops::update::UpdateResult,
        types::{Bson, BsonArray, ConnectionSettings},
    },
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

fn make_bson(d: crate::bson::Document) -> (Vec<u8>, Bson) {
    let mut bytes = Vec::new();
    d.to_writer(&mut bytes).unwrap();
    let bson = Bson {
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    (bytes, bson)
}

#[test]
fn test_update_one_null_filter() {
    let hosts = CString::new("localhost:27017").unwrap();
    let db = CString::new("test").unwrap();
    let coll = CString::new("c").unwrap();
    let settings = make_conn_settings(&hosts);
    let (_ub, update) = make_bson(doc! { "$set": { "x": 1 } });
    let empty_pipeline = BsonArray {
        data: ptr::null(),
        len: 0,
    };
    unsafe {
        let client = mongoc_future_client_new(
            &settings,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null_mut(),
        );
        assert!(!client.is_null());

        let future = mongoc_future_update_one(
            client,
            ptr::null(),
            db.as_ptr(),
            coll.as_ptr(),
            ptr::null(), // null filter
            &update,
            empty_pipeline,
            ptr::null(),
        );
        assert!(!future.is_null());
        assert!(mongoc_future_is_finished(future));

        let mut result_out: *const UpdateResult = ptr::null();
        let mut error_out: *mut Error = ptr::null_mut();
        let ok = mongoc_future_get_update(future, &mut result_out, &mut error_out);
        assert!(!ok);
        assert!(!error_out.is_null());
        assert_eq!((*error_out).error_type, ErrorType::InvalidArgument as u8);

        error_free(error_out);
        mongoc_future_destroy(future);
        mongoc_future_client_free(client);
    }
}

#[test]
fn test_replace_one_null_replacement() {
    let hosts = CString::new("localhost:27017").unwrap();
    let db = CString::new("test").unwrap();
    let coll = CString::new("c").unwrap();
    let settings = make_conn_settings(&hosts);
    let (_fb, filter) = make_bson(doc! { "_id": 1 });
    unsafe {
        let client = mongoc_future_client_new(
            &settings,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null_mut(),
        );
        assert!(!client.is_null());

        let future = mongoc_future_replace_one(
            client,
            ptr::null(),
            db.as_ptr(),
            coll.as_ptr(),
            &filter,
            ptr::null(), // null replacement
            ptr::null(),
        );
        assert!(!future.is_null());
        assert!(mongoc_future_is_finished(future));

        let mut result_out: *const UpdateResult = ptr::null();
        let mut error_out: *mut Error = ptr::null_mut();
        let ok = mongoc_future_get_update(future, &mut result_out, &mut error_out);
        assert!(!ok);
        assert!(!error_out.is_null());
        assert_eq!((*error_out).error_type, ErrorType::InvalidArgument as u8);

        error_free(error_out);
        mongoc_future_destroy(future);
        mongoc_future_client_free(client);
    }
}

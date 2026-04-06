//! Shared client option building logic.
//!
//! This module contains the `build_client_options` function used by both the
//! callback and future-based FFI surfaces to construct `ClientOptions` from
//! FFI settings structs.

use std::path::PathBuf;

use crate::{
    client::auth::Credential,
    error::Result,
    options::{ClientOptions, ServerAddress, Tls, TlsOptions},
};

use crate::ffi::{
    types::{AuthSettings, ConnectionSettings, TlsSettings},
    utils::{
        c_char_to_string,
        i32_to_option_u32,
        i64_to_duration_ms,
        parse_auth_mechanism,
        parse_hosts,
        parse_read_preference_mode,
    },
};

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use crate::ffi::utils::parse_compressors;

/// Build ClientOptions from FFI settings.
///
/// # Safety
///
/// - `connection_settings` must be a valid pointer to a ConnectionSettings struct
/// - `auth_settings` can be null or a valid pointer to an AuthSettings struct
/// - `tls_settings` can be null or a valid pointer to a TlsSettings struct
/// - All C string pointers in the settings structs must be valid null-terminated strings
pub(crate) unsafe fn build_client_options(
    connection_settings: *const ConnectionSettings,
    auth_settings: *const AuthSettings,
    tls_settings: *const TlsSettings,
) -> Result<ClientOptions> {
    if connection_settings.is_null() {
        return Err(crate::error::Error::invalid_argument(
            "connection_settings cannot be null",
        ));
    }

    // Fully destructure ConnectionSettings to ensure all fields are handled
    let ConnectionSettings {
        hosts,
        app_name,
        compressors,
        direct_connection,
        load_balanced,
        max_pool_size,
        min_pool_size,
        max_idle_time_ms,
        connect_timeout_ms,
        socket_timeout_ms,
        server_selection_timeout_ms,
        local_threshold_ms,
        heartbeat_frequency_ms,
        replica_set,
        read_preference_mode,
        srv_service_name,
        srv_max_hosts,
    } = &*connection_settings;

    let host_strings = parse_hosts(*hosts)?;
    let parsed_hosts: Result<Vec<ServerAddress>> = host_strings
        .iter()
        .map(|h| ServerAddress::parse(h))
        .collect();

    let credential = if !auth_settings.is_null() {
        // Fully destructure AuthSettings to ensure all fields are handled
        let AuthSettings {
            mechanism,
            username,
            password,
            source,
        } = &*auth_settings;

        Some(Credential {
            username: c_char_to_string(*username)?,
            source: c_char_to_string(*source)?,
            password: c_char_to_string(*password)?,
            mechanism: parse_auth_mechanism(*mechanism)?,
            mechanism_properties: None,
            oidc_callback: Default::default(),
        })
    } else {
        None
    };

    let tls = if !tls_settings.is_null() {
        // Fully destructure TlsSettings to ensure all fields are handled
        let TlsSettings {
            enabled,
            allow_invalid_certificates,
            allow_invalid_hostnames: _allow_invalid_hostnames,
            ca_file,
            cert_file,
            cert_key_file: _cert_key_file,
        } = &*tls_settings;

        if *enabled {
            Some(Tls::Enabled(TlsOptions {
                allow_invalid_certificates: Some(*allow_invalid_certificates),
                ca_file_path: c_char_to_string(*ca_file)?.map(PathBuf::from),
                cert_key_file_path: c_char_to_string(*cert_file)?.map(PathBuf::from),
                #[cfg(feature = "openssl-tls")]
                allow_invalid_hostnames: None,
                #[cfg(feature = "cert-key-password")]
                tls_certificate_key_file_password: None,
            }))
        } else {
            Some(Tls::Disabled)
        }
    } else {
        None
    };

    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    let compressors_parsed = parse_compressors(*compressors)?;
    #[cfg(not(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    )))]
    let _compressors = compressors;

    let selection_criteria = parse_read_preference_mode(*read_preference_mode)?
        .map(crate::selection_criteria::SelectionCriteria::ReadPreference);

    // socket_timeout is deprecated and not supported in ClientOptions, so we ignore it
    let _socket_timeout_ms = socket_timeout_ms;

    Ok(ClientOptions {
        hosts: parsed_hosts?,
        app_name: c_char_to_string(*app_name)?,
        repl_set_name: c_char_to_string(*replica_set)?,
        srv_service_name: c_char_to_string(*srv_service_name)?,
        direct_connection: Some(*direct_connection),
        load_balanced: Some(*load_balanced),
        max_pool_size: i32_to_option_u32(*max_pool_size),
        min_pool_size: i32_to_option_u32(*min_pool_size),
        srv_max_hosts: i32_to_option_u32(*srv_max_hosts),
        max_idle_time: i64_to_duration_ms(*max_idle_time_ms),
        connect_timeout: i64_to_duration_ms(*connect_timeout_ms),
        server_selection_timeout: i64_to_duration_ms(*server_selection_timeout_ms),
        local_threshold: i64_to_duration_ms(*local_threshold_ms),
        heartbeat_freq: i64_to_duration_ms(*heartbeat_frequency_ms),
        credential,
        tls,
        selection_criteria,
        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        compressors: compressors_parsed,
        ..Default::default()
    })
}

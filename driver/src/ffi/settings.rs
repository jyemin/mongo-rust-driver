use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::Duration;

/// Connection/topology settings passed from Java.
/// Must match the layout of ConnectionSettings.java.
#[repr(C)]
pub struct ConnectionSettings {
    /// Array of host:port strings (e.g., "localhost:27017")
    pub hosts: *const *const c_char,
    /// Number of hosts in the array
    pub hosts_len: i32,
    /// Whether to connect directly to a single host
    /// 0 = not set, 1 = false, 2 = true
    pub direct_connection: u8,
    /// Replica set name (null-terminated C string)
    pub repl_set_name: *const c_char,
    /// SRV service name (null-terminated C string)
    pub srv_service_name: *const c_char,
    /// Maximum number of SRV hosts (0 if not set)
    pub srv_max_hosts: u32,
    /// Server selection timeout in milliseconds (0 if not set)
    pub server_selection_timeout_ms: u64,
    /// Local threshold in milliseconds (0 if not set)
    pub local_threshold_ms: u64,
    /// Whether this is a load-balanced topology
    /// 0 = not set, 1 = false, 2 = true
    pub load_balanced: u8,
}

impl ConnectionSettings {
    /// Convert hosts array to Vec<String>
    pub unsafe fn get_hosts(&self) -> Vec<String> {
        let mut hosts = Vec::new();
        for i in 0..self.hosts_len {
            let host_ptr = *self.hosts.offset(i as isize);
            if !host_ptr.is_null() {
                let host_cstr = CStr::from_ptr(host_ptr);
                if let Ok(host_str) = host_cstr.to_str() {
                    hosts.push(host_str.to_string());
                }
            }
        }
        hosts
    }

    /// Convert optional bool (0 = not set, 1 = false, 2 = true)
    pub fn get_optional_bool(value: u8) -> Option<bool> {
        match value {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => None,
        }
    }

    /// Convert optional string pointer
    pub unsafe fn get_optional_string(ptr: *const c_char) -> Option<String> {
        if ptr.is_null() {
            None
        } else {
            CStr::from_ptr(ptr).to_str().ok().map(|s| s.to_string())
        }
    }

    /// Convert optional duration from milliseconds
    pub fn get_optional_duration_ms(ms: u64) -> Option<Duration> {
        if ms == 0 {
            None
        } else {
            Some(Duration::from_millis(ms))
        }
    }
}

/// Authentication settings passed from Java.
/// Must match the layout of AuthSettings.java.
#[repr(C)]
pub struct AuthSettings {
    /// Authentication mechanism (e.g., "SCRAM-SHA-256")
    pub mechanism: *const c_char,
    /// Authentication source database
    pub source: *const c_char,
    /// Username
    pub username: *const c_char,
    /// Password
    pub password: *const c_char,
    /// Array of mechanism properties as key=value strings
    pub mechanism_properties: *const *const c_char,
    /// Number of mechanism properties
    pub mechanism_properties_len: i32,
}

impl AuthSettings {
    /// Check if authentication is configured
    pub fn is_configured(&self) -> bool {
        !self.mechanism.is_null()
    }

    /// Get mechanism properties as a Vec of (key, value) tuples
    pub unsafe fn get_mechanism_properties(&self) -> Vec<(String, String)> {
        let mut props = Vec::new();
        for i in 0..self.mechanism_properties_len {
            let prop_ptr = *self.mechanism_properties.offset(i as isize);
            if !prop_ptr.is_null() {
                let prop_cstr = CStr::from_ptr(prop_ptr);
                if let Ok(prop_str) = prop_cstr.to_str() {
                    // Parse "key=value" format
                    if let Some((key, value)) = prop_str.split_once('=') {
                        props.push((key.to_string(), value.to_string()));
                    }
                }
            }
        }
        props
    }
}

/// TLS/SSL settings passed from Java.
/// Must match the layout of TlsSettings.java.
#[repr(C)]
pub struct TlsSettings {
    /// Whether TLS is enabled (0 = disabled, 1 = enabled)
    pub enabled: u8,
    /// Whether to allow invalid certificates
    /// 0 = not set, 1 = false, 2 = true
    pub allow_invalid_certificates: u8,
    /// Whether to allow invalid hostnames
    /// 0 = not set, 1 = false, 2 = true
    pub allow_invalid_hostnames: u8,
    /// Path to CA certificate file
    pub ca_file_path: *const c_char,
    /// Path to client certificate/key file
    pub cert_key_file_path: *const c_char,
}

impl TlsSettings {
    /// Check if TLS is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled == 1
    }
}

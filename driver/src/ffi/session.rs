// Session management for FFI boundary
//
// This is a lower-level abstraction than Rust driver's ClientSession.
// Java owns transaction state; Rust owns session identity (lsid) and txnNumber.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use crate::bson::{doc, spec::BinarySubtype, Binary, Bson, Document};
use uuid::Uuid;

/// A server session - just identity and txnNumber
#[derive(Debug)]
pub struct FfiSession {
    /// Session identifier (lsid) - { "id": UUID }
    lsid: Document,
    /// Transaction number, incremented for each transaction or retryable write
    txn_number: i64,
    /// Whether the session has been marked dirty (e.g., network error)
    dirty: bool,
}

impl FfiSession {
    fn new() -> Self {
        let binary = Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: Uuid::new_v4().as_bytes().to_vec(),
        });
        
        FfiSession {
            lsid: doc! { "id": binary },
            txn_number: 0,
            dirty: false,
        }
    }
    
    /// Get the session identifier (lsid) as BSON bytes
    pub fn get_lsid_bytes(&self) -> Vec<u8> {
        crate::bson::to_vec(&self.lsid).unwrap_or_default()
    }
    
    /// Get the session identifier document
    pub fn get_lsid(&self) -> &Document {
        &self.lsid
    }
    
    /// Get the current transaction number
    pub fn get_txn_number(&self) -> i64 {
        self.txn_number
    }
    
    /// Advance the transaction number and return the new value
    /// Used for retryable writes and starting transactions
    pub fn advance_txn_number(&mut self) -> i64 {
        self.txn_number += 1;
        self.txn_number
    }
    
    /// Mark the session as dirty (should not be returned to pool)
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }
    
    /// Check if session is dirty
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

/// Session pool that manages server sessions
/// Thread-safe - can be shared across threads
pub struct FfiSessionPool {
    /// Active sessions by handle
    sessions: RwLock<HashMap<u64, FfiSession>>,
    /// Counter for generating session handles
    next_handle: AtomicU64,
    /// Pool of available (released) sessions - stored by handle
    available: RwLock<Vec<u64>>,
}

impl FfiSessionPool {
    pub fn new() -> Self {
        FfiSessionPool {
            sessions: RwLock::new(HashMap::new()),
            next_handle: AtomicU64::new(1), // Start at 1, 0 = no session
            available: RwLock::new(Vec::new()),
        }
    }
    
    /// Acquire a session from the pool (or create a new one)
    /// Returns a session handle (non-zero)
    pub fn acquire(&self) -> u64 {
        // Try to get an available session first
        if let Ok(mut available) = self.available.write() {
            while let Some(handle) = available.pop() {
                // Verify the session exists and is not dirty
                if let Ok(sessions) = self.sessions.read() {
                    if let Some(session) = sessions.get(&handle) {
                        if !session.is_dirty() {
                            return handle;
                        }
                    }
                }
                // Session was dirty or gone, try next
            }
        }
        
        // Create a new session
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let session = FfiSession::new();
        
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.insert(handle, session);
        }
        
        handle
    }
    
    /// Release a session back to the pool
    pub fn release(&self, handle: u64) {
        if let Ok(sessions) = self.sessions.read() {
            if let Some(session) = sessions.get(&handle) {
                if session.is_dirty() {
                    // Don't return dirty sessions to pool - they'll be dropped
                    return;
                }
            }
        }
        
        if let Ok(mut available) = self.available.write() {
            available.push(handle);
        }
    }
    
    /// Get a session by handle (for read operations)
    pub fn with_session<F, R>(&self, handle: u64, f: F) -> Option<R>
    where
        F: FnOnce(&FfiSession) -> R,
    {
        self.sessions.read().ok()?.get(&handle).map(f)
    }
    
    /// Get a mutable session by handle (for write operations)
    pub fn with_session_mut<F, R>(&self, handle: u64, f: F) -> Option<R>
    where
        F: FnOnce(&mut FfiSession) -> R,
    {
        self.sessions.write().ok()?.get_mut(&handle).map(f)
    }

    /// Get the session lsid as a Document (for adding to commands)
    pub fn get_session_lsid(&self, handle: u64) -> Option<Document> {
        self.with_session(handle, |s| s.get_lsid().clone())
    }

    /// Get the current transaction number for a session
    pub fn get_txn_number(&self, handle: u64) -> i64 {
        self.with_session(handle, |s| s.get_txn_number()).unwrap_or(0)
    }

    /// Advance and return the transaction number for a session
    pub fn advance_txn_number(&self, handle: u64) -> i64 {
        self.with_session_mut(handle, |s| s.advance_txn_number()).unwrap_or(0)
    }

    /// Mark a session as dirty
    pub fn mark_dirty(&self, handle: u64) {
        self.with_session_mut(handle, |s| s.mark_dirty());
    }
}

impl Default for FfiSessionPool {
    fn default() -> Self {
        Self::new()
    }
}


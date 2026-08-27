//! WSARB — websocket arbitration proxy for `order_book_server` feeds.
//!
//! Exposed as a library so the load-test binary can drive the very same frame
//! parsing the proxy itself uses. A second implementation would drift, and the
//! test would end up checking its own reading of the wire rather than wsarb's.

pub mod client;
pub mod state;
pub mod stats;
pub mod upstream;

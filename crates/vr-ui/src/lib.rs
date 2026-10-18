//! Static UI assets, embedded at compile time.
//!
//! Plain HTML/CSS/vanilla JS — no framework, no build step, no Node. The
//! whole dashboard is a handful of files here; `rust-embed` bakes them into
//! the binary so the single-binary promise holds: there is nothing to install
//! alongside it and no directory to get wrong on a VPS.
//!
//! In release builds the bytes are a static slice with zero runtime cost; in
//! debug builds rust-embed reads from disk so UI edits show up on refresh
//! without recompiling. Both modes return the same `Cow<'static, [u8]>`, so
//! callers cannot tell the difference.

#![forbid(unsafe_code)]

use std::borrow::Cow;

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "assets/"]
pub struct Assets;

/// An embedded asset: its bytes and the content type to serve them with.
pub struct Asset {
    pub data: Cow<'static, [u8]>,
    pub mime: &'static str,
}

/// Look up an embedded asset by path (leading `/` tolerated).
pub fn get(path: &str) -> Option<Asset> {
    let file = Assets::get(path.trim_start_matches('/'))?;
    Some(Asset {
        data: file.data,
        mime: mime_for(path),
    })
}

/// The single-page dashboard, always served at `/`.
pub fn index() -> Option<Asset> {
    get("index.html")
}

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_is_embedded() {
        assert!(
            index().is_some(),
            "index.html must be compiled into the binary"
        );
    }

    #[test]
    fn known_assets_resolve_with_types() {
        let css = get("app.css").expect("app.css embedded");
        assert!(css.mime.starts_with("text/css"));
        assert!(!css.data.is_empty());

        let js = get("app.js").expect("app.js embedded");
        assert!(js.mime.starts_with("application/javascript"));
        // The estimate marker contract: the UI must flag estimated usage.
        assert!(
            String::from_utf8_lossy(&js.data).contains("usage_estimated"),
            "app.js must render the estimated-usage marker"
        );
    }

    #[test]
    fn unknown_paths_miss() {
        assert!(get("nope.html").is_none());
    }

    #[test]
    fn paths_tolerate_leading_slash() {
        assert!(get("/index.html").is_some());
    }
}

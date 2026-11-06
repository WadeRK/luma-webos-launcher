//! Stremio for Home stacks. Unlike Jellyfin/Plex, there is no sign-in and no
//! single library API: content comes from whichever addons the user has
//! configured, each exposing a catalog over a small HTTP protocol. Luma only
//! reads catalog metadata to populate stacks; playback is handed off to the
//! installed Stremio app itself (see platform::Request::Launch), which lets
//! the user pick the actual stream source the way they always do.
use crate::stacks::StackItem;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Duration;

/// webOS app id for Stremio Origin. Confirmed via `ares-install --list`;
/// change this if a different Stremio client is installed instead.
pub const APP_ID: &str = "tv.stremioorigin.app";

/// One configured addon. `collection` strings on a "stremio" stack encode
/// `"<base_url>|<catalog_type>|<catalog_id>"` so no separate per-stack addon
/// index is needed; see `parse_collection` below.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct StremioAddon {
    pub name: String,
    pub base_url: String,
}

/// Splits a stack's `collection` field into (base_url, catalog_type, catalog_id).
/// Returns `None` for anything that doesn't match, so a malformed or
/// not-yet-configured stack is skipped rather than panicking.
pub fn parse_collection(collection: &str) -> Option<(String, String, String)> {
    let mut parts = collection.splitn(3, '|');
    let base_url = parts.next()?.to_string();
    let catalog_type = parts.next()?.to_string();
    let catalog_id = parts.next()?.to_string();
    if base_url.is_empty() || catalog_type.is_empty() || catalog_id.is_empty() {
        return None;
    }
    Some((base_url, catalog_type, catalog_id))
}

pub enum Command {
    /// Cards for (stack index, collection string).
    Fetch(Vec<(usize, String)>),
}

pub enum Response {
    Items(usize, Vec<StackItem>),
    /// A fetch failed; `usize` is the stack index so the UI can leave that
    /// row's previous contents alone instead of clearing it.
    Failed(usize, String),
}

pub struct Service {
    sender: Mutex<mpsc::Sender<Command>>,
    pub receiver: Mutex<mpsc::Receiver<Response>>,
}

impl Service {
    pub fn new() -> Self {
        let (sender, commands) = mpsc::channel::<Command>();
        let (reply, receiver) = mpsc::channel();
        thread::spawn(move || {
            for command in commands {
                match command {
                    Command::Fetch(wanted) => fetch(&reply, wanted),
                }
            }
        });
        Self {
            sender: Mutex::new(sender),
            receiver: Mutex::new(receiver),
        }
    }
    pub fn send(&self, command: Command) {
        let _ = self.sender.lock().unwrap().send(command);
    }
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

/// Reserved id bit for Stremio cards. Jellyfin uses `1 << 52`, Plex uses
/// `1 << 51`; this must not collide with either (or with any future source).
const CARD_TAG: i64 = 1 << 50;
static CARD_IDS: Mutex<BTreeMap<i64, String>> = Mutex::new(BTreeMap::new());

/// Stremio ids are strings (IMDB-style "tt1254207" or addon-prefixed); the
/// title card and card grid key everything by i64, so hash and tag like
/// Jellyfin/Plex do. FNV-1a: stable across runs, so cached posters keep
/// their file names.
pub fn card_id(item: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in item.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let id = (hash as i64 & (CARD_TAG - 1)) | CARD_TAG;
    CARD_IDS.lock().unwrap().insert(id, item.to_owned());
    id
}

/// Whether a card id belongs to Stremio. There is no title-card flow for
/// Stremio (see state.rs's Target::StackCard handling, which launches the
/// app directly instead of calling open_movie), but this is kept for
/// symmetry with jellyfin::owns/plex::owns and in case that changes later.
pub fn owns(id: i64) -> bool {
    id & CARD_TAG != 0 && CARD_IDS.lock().unwrap().contains_key(&id)
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(20)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn fetch_one(
    base_url: &str,
    catalog_type: &str,
    catalog_id: &str,
) -> Result<Vec<StackItem>, String> {
    let url = format!(
        "{}/catalog/{catalog_type}/{catalog_id}.json",
        base_url.trim_end_matches('/')
    );
    let mut response = agent()
        .get(&url)
        .call()
        .map_err(|e| format!("{base_url}: {e}"))?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(format!("{base_url}: HTTP {}", response.status()));
    }
    let text = response
        .body_mut()
        .with_config()
        .limit(8 * 1024 * 1024)
        .read_to_string()
        .map_err(|e| format!("{base_url}: {e}"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("{base_url}: bad response ({e})"))?;
    let metas = parsed["metas"].as_array().cloned().unwrap_or_default();

    Ok(metas
        .iter()
        .filter_map(|m| {
            let id_str = m["id"].as_str()?;
            Some(StackItem {
                title: m["name"].as_str().unwrap_or_default().to_string(),
                poster: m["poster"].as_str().map(str::to_string),
                id: card_id(id_str),
            })
        })
        .collect())
}

fn fetch(reply: &mpsc::Sender<Response>, wanted: Vec<(usize, String)>) {
    for (index, collection) in wanted {
        let Some((base_url, catalog_type, catalog_id)) = parse_collection(&collection) else {
            let _ = reply.send(Response::Failed(index, "Stack not configured".into()));
            continue;
        };
        match fetch_one(&base_url, &catalog_type, &catalog_id) {
            Ok(items) => {
                let _ = reply.send(Response::Items(index, items));
            }
            Err(message) => {
                let _ = reply.send(Response::Failed(index, message));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_collection() {
        assert_eq!(
            parse_collection("https://v3-cinemeta.strem.io|movie|top"),
            Some((
                "https://v3-cinemeta.strem.io".into(),
                "movie".into(),
                "top".into()
            ))
        );
    }

    #[test]
    fn rejects_malformed_collection() {
        assert_eq!(parse_collection(""), None);
        assert_eq!(parse_collection("https://example.com"), None);
        assert_eq!(parse_collection("https://example.com|movie"), None);
        assert_eq!(parse_collection("|movie|top"), None);
    }

    #[test]
    fn card_id_is_stable_and_tagged() {
        let a = card_id("tt1254207");
        let b = card_id("tt1254207");
        assert_eq!(a, b);
        assert!(owns(a));
        assert!(!owns(42));
    }
}

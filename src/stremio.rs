//! Stremio integration for Home stacks. Reads local library and watch state
//! from Stremio Origin on webOS, and fetches catalog metadata for trending
//! and configured addons.
use crate::stacks::StackItem;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Duration;

pub const APP_ID: &str = "tv.stremioorigin.app";

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct StremioAddon {
    pub name: String,
    pub base_url: String,
}

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
    Fetch(Vec<(usize, String)>),
}

#[allow(dead_code)]
pub enum Response {
    Items(usize, Vec<StackItem>),
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

const CARD_TAG: i64 = 1 << 50;
static CARD_TARGETS: Mutex<BTreeMap<i64, (String, String)>> = Mutex::new(BTreeMap::new());

pub fn card_id(media_type: &str, item_id: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in item_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let id = (hash as i64 & (CARD_TAG - 1)) | CARD_TAG;
    CARD_TARGETS
        .lock()
        .unwrap()
        .insert(id, (media_type.to_owned(), item_id.to_owned()));
    id
}

pub fn target(id: i64) -> Option<(String, String)> {
    CARD_TARGETS.lock().unwrap().get(&id).cloned()
}

#[allow(dead_code)]
pub fn owns(id: i64) -> bool {
    id & CARD_TAG != 0 && CARD_TARGETS.lock().unwrap().contains_key(&id)
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(20)))
        .http_status_as_error(false)
        .build()
        .into()
}

fn load_stremio_state() -> Option<serde_json::Value> {
    let path = Path::new(
        "/media/developer/apps/usr/palm/applications/tv.stremioorigin.app/data/state.json",
    );
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn fetch_continue() -> Vec<StackItem> {
    let Some(data) = load_stremio_state() else {
        return vec![];
    };
    let mut entries: Vec<(&serde_json::Value, &str, &str)> = Vec::new();
    if let Some(lib) = data.get("lib").and_then(|v| v.as_object()) {
        for (id, item) in lib {
            if item
                .get("removed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                continue;
            }
            let time_offset = item
                .pointer("/state/timeOffset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let duration = item
                .pointer("/state/duration")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let last_watched = item
                .pointer("/state/lastWatched")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if time_offset > 0 && (duration == 0 || time_offset < duration.saturating_sub(60_000)) {
                entries.push((item, last_watched, id));
            }
        }
    }
    entries.sort_by(|a, b| b.1.cmp(a.1));

    entries
        .into_iter()
        .take(crate::stacks::HOME_ROW_MAX)
        .map(|(item, _, id)| {
            let title = item
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let poster = item
                .get("poster")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let media_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("movie");
            StackItem {
                title,
                poster,
                id: card_id(media_type, id),
            }
        })
        .collect()
}

fn fetch_library() -> Vec<StackItem> {
    let Some(data) = load_stremio_state() else {
        return vec![];
    };
    let mut entries: Vec<(&serde_json::Value, &str, &str)> = Vec::new();
    if let Some(lib) = data.get("lib").and_then(|v| v.as_object()) {
        for (id, item) in lib {
            if item
                .get("removed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                continue;
            }
            let mtime = item.get("_mtime").and_then(|v| v.as_str()).unwrap_or("");
            entries.push((item, mtime, id));
        }
    }
    entries.sort_by(|a, b| b.1.cmp(a.1));

    entries
        .into_iter()
        .take(crate::stacks::HOME_ROW_MAX)
        .map(|(item, _, id)| {
            let title = item
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let poster = item
                .get("poster")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let media_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("movie");
            StackItem {
                title,
                poster,
                id: card_id(media_type, id),
            }
        })
        .collect()
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
            let media_type = m["type"].as_str().unwrap_or(catalog_type);
            Some(StackItem {
                title: m["name"].as_str().unwrap_or_default().to_string(),
                poster: m["poster"].as_str().map(str::to_string),
                id: card_id(media_type, id_str),
            })
        })
        .collect())
}

fn fetch(reply: &mpsc::Sender<Response>, wanted: Vec<(usize, String)>) {
    for (index, collection) in wanted {
        match collection.as_str() {
            "continue" => {
                let items = fetch_continue();
                let _ = reply.send(Response::Items(index, items));
            }
            "library" => {
                let items = fetch_library();
                let _ = reply.send(Response::Items(index, items));
            }
            "trending_movies" => match fetch_one("https://v3-cinemeta.strem.io", "movie", "top") {
                Ok(items) => {
                    let _ = reply.send(Response::Items(index, items));
                }
                Err(msg) => {
                    let _ = reply.send(Response::Failed(index, msg));
                }
            },
            "trending_series" => match fetch_one("https://v3-cinemeta.strem.io", "series", "top") {
                Ok(items) => {
                    let _ = reply.send(Response::Items(index, items));
                }
                Err(msg) => {
                    let _ = reply.send(Response::Failed(index, msg));
                }
            },
            custom => {
                let Some((base_url, catalog_type, catalog_id)) = parse_collection(custom) else {
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
        let a = card_id("movie", "tt1254207");
        let b = card_id("movie", "tt1254207");
        assert_eq!(a, b);
        assert!(owns(a));
        assert!(!owns(42));
        assert_eq!(target(a), Some(("movie".into(), "tt1254207".into())));
    }
}

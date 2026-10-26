//! Provider registry: builds providers from config records and answers
//! resolution requests.
//!
//! This is where "add a provider without touching the routing core" lives:
//! a provider is a `ProviderConfig` record, and the generic OpenAI-compatible
//! transport serves it. Nothing here is provider-specific; a provider with
//! genuinely custom behaviour would be a new `UpstreamClient` impl selected
//! by `ProviderKind` at the server's wiring point — an extension point, not
//! an existing code path.
//!
//! Depends on: vibe-router-core, vibe-router-config.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use vibe_router_config::{AppConfig, ProviderConfig};
use vibe_router_core::{
    Candidate, Combo, ModelInfo, ModelRegistry, ModelSpec, Resolution, RouterError, Transport,
};

mod http;
pub use http::HttpUpstream;

pub mod pool;
pub use pool::{KeySlot, ProviderKeyPool};

pub mod proxy;
pub use proxy::{
    fnv1a_hash, sticky_proxy_index, ProxyEntry, ProxyPool, ProxyTag, CLOUDFLARE_WORKER_SCRIPT,
};

#[cfg(any(test, feature = "test-mock"))]
pub mod mock;

#[cfg(test)]
mod transport_tests;

/// A provider instance built from config.
#[derive(Debug, Clone)]
pub struct ConfigProvider {
    pub id: String,
    pub display_name: String,
    pub transport: Transport,
    pub auth: vibe_router_core::Auth,
    pub models: Vec<ModelSpec>,
    /// Whether undeclared models are forwarded rather than rejected.
    pub passthrough: bool,
    /// Rotation/cooldown state for this provider's API keys. Shared across
    /// clones, so a 429 on one in-flight request cools the key for every
    /// later one.
    pub key_pool: Arc<ProviderKeyPool>,
}

/// The live registry. Cloned cheaply (Arc inside); swapped wholesale on
/// config reload so a hot reload never mutates a list a request is reading.
///
/// Combos come from two places: the config file, and runtime edits (the UI's
/// combo editor, persisted by the server behind its own store). Runtime
/// combos win by name, and *survive* a config reload — otherwise saving a
/// combo in the dashboard would silently lose it the moment the TOML file
/// was touched. vibe-router-providers never learns where runtime combos are stored;
/// the server hands them in as values.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<RwLock<RegistryData>>,
    runtime: Arc<RwLock<HashMap<String, Combo>>>,
    runtime_providers: Arc<RwLock<HashMap<String, Arc<ConfigProvider>>>>,
    aliases: Arc<RwLock<HashMap<String, String>>>,
    proxies: Arc<proxy::ProxyPool>,
}

struct RegistryData {
    providers: HashMap<String, Arc<ConfigProvider>>,
    file_providers: HashMap<String, Arc<ConfigProvider>>,
    /// File combos merged with runtime combos (runtime wins) — what
    /// resolution actually reads.
    combos: HashMap<String, Combo>,
    /// The config file's combos, unmerged. Kept because the merged map
    /// cannot answer "what did the file say?" once a runtime combo shadows
    /// one by name — and deleting a runtime combo must reveal it again.
    file_combos: HashMap<String, Combo>,
    /// Set of `provider/model` strings declared by providers with model lists.
    known: HashSet<String>,
}

impl Registry {
    pub fn from_config(cfg: &AppConfig) -> Self {
        let this = Self {
            inner: Arc::new(RwLock::new(RegistryData {
                providers: HashMap::new(),
                file_providers: HashMap::new(),
                combos: HashMap::new(),
                file_combos: HashMap::new(),
                known: HashSet::new(),
            })),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            runtime_providers: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
            proxies: Arc::new(proxy::ProxyPool::new()),
        };
        this.load(cfg);
        this
    }

    fn load(&self, cfg: &AppConfig) {
        let mut file_providers = HashMap::new();
        for p in &cfg.providers {
            let provider = build_provider(p);
            file_providers.insert(provider.id.clone(), Arc::new(provider));
        }
        let file_combos: HashMap<String, Combo> = cfg
            .combos
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    Combo {
                        name: c.name.clone(),
                        description: c.description.clone(),
                        models: c.models.clone(),
                        strategy: Default::default(),
                    },
                )
            })
            .collect();
        let runtime_combos = self.runtime.read().clone();
        let combos = merge_combos(file_combos.clone(), &runtime_combos);
        let runtime_provs = self.runtime_providers.read().clone();
        let mut providers = file_providers.clone();
        for (id, p) in runtime_provs {
            providers.insert(id, p);
        }
        let mut known = HashSet::new();
        for p in providers.values() {
            for m in &p.models {
                known.insert(format!("{}/{}", p.id, m.id));
            }
        }
        *self.inner.write() = RegistryData {
            providers,
            file_providers,
            combos,
            file_combos,
            known,
        };
    }

    /// Replace the config-derived half of the registry, keeping runtime combos and providers.
    pub fn reload(&self, cfg: &AppConfig) {
        self.load(cfg);
    }

    /// Add or replace a runtime provider.
    pub fn set_runtime_provider(&self, provider: ConfigProvider) {
        let id = provider.id.clone();
        self.runtime_providers
            .write()
            .insert(id, Arc::new(provider));
        self.remerge();
    }

    /// Remove a runtime provider, unmasking a file provider of the same id if one exists.
    pub fn remove_runtime_provider(&self, id: &str) {
        self.runtime_providers.write().remove(id);
        self.remerge();
    }

    /// True when `id` was declared in the config file.
    pub fn is_file_provider(&self, id: &str) -> bool {
        self.inner.read().file_providers.contains_key(id)
    }

    /// Providers authored at runtime rather than in the config file.
    pub fn runtime_provider_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.runtime_providers.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Add or replace a runtime combo. The server validates the model list
    /// against [`Registry::known_models`] before calling this.
    pub fn set_runtime_combo(&self, combo: Combo) {
        let name = combo.name.clone();
        self.runtime.write().insert(name, combo);
        self.remerge();
    }

    /// Remove a runtime combo, unmasking a file combo of the same name if one
    /// exists.
    pub fn remove_runtime_combo(&self, name: &str) {
        self.runtime.write().remove(name);
        self.remerge();
    }

    fn remerge(&self) {
        let runtime_combos = self.runtime.read().clone();
        let runtime_provs = self.runtime_providers.read().clone();
        let mut data = self.inner.write();
        data.combos = merge_combos(data.file_combos.clone(), &runtime_combos);
        let mut providers = data.file_providers.clone();
        for (id, p) in runtime_provs {
            providers.insert(id, p);
        }
        let mut known = HashSet::new();
        for p in providers.values() {
            for m in &p.models {
                known.insert(format!("{}/{}", p.id, m.id));
            }
        }
        data.providers = providers;
        data.known = known;
    }

    /// Combos authored at runtime rather than in the config file.
    pub fn runtime_combo_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.runtime.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// True when `name` was declared in the config file.
    pub fn is_file_combo(&self, name: &str) -> bool {
        self.inner.read().file_combos.contains_key(name)
    }

    pub fn provider(&self, id: &str) -> Option<Arc<ConfigProvider>> {
        self.inner.read().providers.get(id).cloned()
    }

    pub fn combo(&self, name: &str) -> Option<Combo> {
        self.inner.read().combos.get(name).cloned()
    }

    /// Every provider id.
    pub fn provider_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.inner.read().providers.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// The known-model set, for config-side combo validation.
    pub fn known_models(&self) -> HashSet<String> {
        self.inner.read().known.clone()
    }

    /// Register a virtual model alias (e.g. `gpt-4o -> openai/gpt-4o` or `claude -> anthropic-combo`).
    pub fn set_alias(&self, alias: impl Into<String>, target: impl Into<String>) {
        self.aliases.write().insert(alias.into(), target.into());
    }

    /// Remove a virtual model alias.
    pub fn remove_alias(&self, alias: &str) -> bool {
        self.aliases.write().remove(alias).is_some()
    }

    /// Look up the immediate target of an alias.
    pub fn get_alias(&self, alias: &str) -> Option<String> {
        self.aliases.read().get(alias).cloned()
    }

    /// Return snapshot of all virtual aliases.
    pub fn aliases(&self) -> HashMap<String, String> {
        self.aliases.read().clone()
    }

    /// Recursively resolve an alias up to 5 hops to its ultimate target.
    pub fn resolve_alias(&self, model: &str) -> String {
        let mut current = model.to_string();
        let g = self.aliases.read();
        for _ in 0..5 {
            if let Some(target) = g.get(&current) {
                current = target.clone();
            } else {
                break;
            }
        }
        current
    }

    /// The smart proxy pool.
    pub fn proxies(&self) -> Arc<proxy::ProxyPool> {
        self.proxies.clone()
    }
}

/// File combos with runtime combos layered on top, runtime winning by name.
fn merge_combos(
    mut file: HashMap<String, Combo>,
    runtime: &HashMap<String, Combo>,
) -> HashMap<String, Combo> {
    for (name, combo) in runtime {
        file.insert(name.clone(), combo.clone());
    }
    file
}

pub fn build_provider(p: &ProviderConfig) -> ConfigProvider {
    let auth = p.auth();
    let key_pool = match auth.key_pool() {
        Some(kp) => ProviderKeyPool::new(kp.keys.clone(), kp.strategy),
        None => ProviderKeyPool::empty(),
    };
    ConfigProvider {
        id: p.id.clone(),
        display_name: p.display_name.clone().unwrap_or_else(|| p.id.clone()),
        transport: vibe_router_core::Transport {
            base_url: p.base_url.clone(),
            format: vibe_router_core::WireFormat::OpenAi, // v0.1: only one format exists
            headers: p.headers.clone(),
        },
        auth,
        models: p
            .models
            .iter()
            .map(|m| ModelSpec {
                id: m.id.clone(),
                name: m.name.clone(),
                context: m.context,
                max_output: m.max_output,
                capabilities: Vec::new(),
            })
            .collect(),
        passthrough: p.passthrough,
        key_pool: Arc::new(key_pool),
    }
}

impl ModelRegistry for Registry {
    fn resolve(&self, model: &str) -> Result<Resolution, RouterError> {
        let effective = self.resolve_alias(model);
        let target = if effective != model {
            &effective
        } else {
            model
        };

        let data = self.inner.read();
        // Combo name?
        if let Some(combo) = data.combos.get(target) {
            if combo.models.is_empty() {
                return Err(RouterError::ModelNotFound {
                    requested: model.to_string(),
                    detail: "combo is empty".into(),
                });
            }
            let cands: Vec<Candidate> = combo
                .models
                .iter()
                .enumerate()
                .map(|(i, m)| Candidate::combo(&combo.name, m).with_index(i))
                .collect();
            return Ok(Resolution::Chain(cands));
        }
        // provider/model?
        if let Some((provider_id, model_id)) = target.split_once('/') {
            let Some(p) = data.providers.get(provider_id) else {
                return Err(RouterError::ModelNotFound {
                    requested: model.to_string(),
                    detail: format!("no provider `{provider_id}` is configured"),
                });
            };
            let known = p.passthrough
                || p.models.is_empty()
                || data.known.contains(&format!("{provider_id}/{model_id}"));
            if !known {
                return Err(RouterError::ModelNotFound {
                    requested: model.to_string(),
                    detail: format!("provider `{provider_id}` does not declare model `{model_id}`"),
                });
            }
            return Ok(Resolution::Single(Candidate::direct(target)));
        }
        Err(RouterError::ModelNotFound {
            requested: model.to_string(),
            detail: "expected `provider/model` or a configured combo name".into(),
        })
    }

    fn list(&self) -> Vec<ModelInfo> {
        let data = self.inner.read();
        let mut out = Vec::new();
        // Declared models of every provider.
        for p in data.providers.values() {
            for m in &p.models {
                out.push(ModelInfo {
                    id: format!("{}/{}", p.id, m.id),
                    owned_by: p.id.clone(),
                });
            }
        }
        // Combos appear as virtual models — that is how clients address them.
        for c in data.combos.values() {
            out.push(ModelInfo {
                id: c.name.clone(),
                owned_by: "combo".to_string(),
            });
        }
        // Virtual model aliases
        for alias in self.aliases.read().keys() {
            out.push(ModelInfo {
                id: alias.clone(),
                owned_by: "alias".to_string(),
            });
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibe_router_config::schema::{ComboConfig, ModelConfig};

    fn provider(id: &str, models: Vec<&str>, passthrough: bool) -> ProviderConfig {
        ProviderConfig {
            id: id.into(),
            display_name: None,
            base_url: format!("https://api.{id}.test/v1/chat/completions"),
            format: Default::default(),
            auth: vibe_router_config::AuthConfig {
                scheme: Default::default(),
                secret: format!("env:{id}_KEY"),
                ..Default::default()
            },
            api_keys: vec![],
            key_strategy: None,
            headers: vec![],
            models: models
                .into_iter()
                .map(|m| ModelConfig {
                    id: m.into(),
                    name: None,
                    context: None,
                    max_output: None,
                })
                .collect(),
            passthrough,
        }
    }

    fn cfg(providers: Vec<ProviderConfig>, combos: Vec<ComboConfig>) -> AppConfig {
        AppConfig {
            server: Default::default(),
            storage: Default::default(),
            limits: Default::default(),
            strategy: Default::default(),
            cache: Default::default(),
            providers: providers.clone(),
            combos: combos.clone(),
            roles: None,
            router: vibe_router_config::RouterConfig {
                server: Default::default(),
                storage: Default::default(),
                limits: Default::default(),
                strategy: Default::default(),
                providers,
                combos,
                error_rules: vec![],
                roles: None,
                cache: Default::default(),
            },
            source_path: None,
        }
    }

    #[test]
    fn resolves_declared_models() {
        let r = Registry::from_config(&cfg(
            vec![provider("openai", vec!["gpt-4o-mini"], false)],
            vec![],
        ));
        assert!(r.resolve("openai/gpt-4o-mini").is_ok());
        assert!(r.resolve("openai/gpt-99999").is_err());
    }

    #[test]
    fn provider_without_model_list_accepts_anything() {
        let r = Registry::from_config(&cfg(vec![provider("myvps", vec![], false)], vec![]));
        assert!(r.resolve("myvps/whatever-we-run").is_ok());
    }

    #[test]
    fn passthrough_accepts_undeclared_alongside_declared() {
        let r = Registry::from_config(&cfg(
            vec![provider("gw", vec!["known-model"], true)],
            vec![],
        ));
        assert!(r.resolve("gw/known-model").is_ok());
        assert!(r.resolve("gw/brand-new-model").is_ok());
    }

    #[test]
    fn resolves_combo_to_ordered_chain() {
        let r = Registry::from_config(&cfg(
            vec![
                provider("a", vec!["x"], false),
                provider("b", vec!["y"], false),
            ],
            vec![ComboConfig {
                name: "cheap-first".into(),
                description: None,
                models: vec!["b/y".into(), "a/x".into()],
            }],
        ));
        let res = r.resolve("cheap-first").unwrap();
        let cands = match res {
            Resolution::Chain(c) => c,
            _ => panic!("expected chain"),
        };
        assert_eq!(cands.len(), 2);
        assert_eq!(cands[0].model, "b/y");
        assert_eq!(cands[1].model, "a/x");
        assert_eq!(cands[0].combo.as_deref(), Some("cheap-first"));
    }

    #[test]
    fn unknown_provider_is_not_found() {
        let r = Registry::from_config(&cfg(vec![], vec![]));
        let err = r.resolve("ghost/model").unwrap_err();
        assert_eq!(err.status(), 404);
    }

    #[test]
    fn list_includes_models_and_combos() {
        let r = Registry::from_config(&cfg(
            vec![provider("a", vec!["x", "y"], false)],
            vec![ComboConfig {
                name: "c1".into(),
                description: None,
                models: vec!["a/x".into()],
            }],
        ));
        let ids: Vec<_> = r.list().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec!["a/x", "a/y", "c1"]);
    }

    #[test]
    fn runtime_combo_resolves_without_being_in_the_file() {
        let r = Registry::from_config(&cfg(vec![provider("a", vec!["x", "y"], false)], vec![]));
        assert!(r.resolve("ui-chain").is_err(), "not authored yet");
        r.set_runtime_combo(Combo::new("ui-chain", vec!["a/y".into(), "a/x".into()]));
        let res = r.resolve("ui-chain").unwrap();
        let models: Vec<_> = match res {
            Resolution::Chain(cs) => cs.iter().map(|c| c.model.clone()).collect(),
            Resolution::Single(c) => vec![c.model],
        };
        assert_eq!(models, vec!["a/y", "a/x"]);
        assert_eq!(r.combo("ui-chain").unwrap().models.len(), 2);
    }

    #[test]
    fn runtime_combo_overrides_file_combo_by_name() {
        let r = Registry::from_config(&cfg(
            vec![provider("a", vec!["x", "y"], false)],
            vec![ComboConfig {
                name: "chain".into(),
                description: None,
                models: vec!["a/x".into()],
            }],
        ));
        assert_eq!(r.combo("chain").unwrap().models, vec!["a/x"]);
        assert!(r.is_file_combo("chain"));

        r.set_runtime_combo(Combo::new("chain", vec!["a/y".into(), "a/x".into()]));
        assert_eq!(r.combo("chain").unwrap().models, vec!["a/y", "a/x"]);
        assert_eq!(r.runtime_combo_names(), vec!["chain".to_string()]);
    }

    #[test]
    fn deleting_a_runtime_combo_falls_back_to_the_file_version() {
        let r = Registry::from_config(&cfg(
            vec![provider("a", vec!["x", "y"], false)],
            vec![ComboConfig {
                name: "chain".into(),
                description: None,
                models: vec!["a/x".into()],
            }],
        ));
        r.set_runtime_combo(Combo::new("chain", vec!["a/y".into()]));
        assert_eq!(r.combo("chain").unwrap().models, vec!["a/y"]);

        r.remove_runtime_combo("chain");
        assert_eq!(
            r.combo("chain").unwrap().models,
            vec!["a/x"],
            "the file combo must reappear, not vanish"
        );
        assert!(r.runtime_combo_names().is_empty());
    }

    #[test]
    fn deleting_an_unshadowed_runtime_combo_removes_it_entirely() {
        let r = Registry::from_config(&cfg(vec![provider("a", vec!["x"], false)], vec![]));
        r.set_runtime_combo(Combo::new("temp", vec!["a/x".into()]));
        assert!(r.resolve("temp").is_ok());
        r.remove_runtime_combo("temp");
        assert!(r.resolve("temp").is_err());
        assert!(r.combo("temp").is_none());
    }

    #[test]
    fn runtime_combos_survive_a_config_reload() {
        // The reload path is what the file watcher drives; losing UI-authored
        // chains there would make the editor feel broken.
        let mut c = cfg(vec![provider("a", vec!["x"], false)], vec![]);
        let r = Registry::from_config(&c);
        r.set_runtime_combo(Combo::new("ui", vec!["a/x".into()]));

        // Config file changes: providers replaced, runtime kept.
        c.providers = vec![provider("b", vec!["z"], false)];
        c.combos = vec![];
        c.router.providers = c.providers.clone();
        r.reload(&c);

        assert!(r.resolve("ui").is_ok(), "runtime combo must survive reload");
        assert_eq!(r.combo("ui").unwrap().models, vec!["a/x"]);
        assert!(r.provider("a").is_none(), "file providers were replaced");
        assert!(r.provider("b").is_some());
    }

    #[test]
    fn reload_replaces_file_combos_that_runtime_does_not_shadow() {
        let mut c = cfg(
            vec![provider("a", vec!["x"], false)],
            vec![ComboConfig {
                name: "file-chain".into(),
                description: None,
                models: vec!["a/x".into()],
            }],
        );
        let r = Registry::from_config(&c);
        assert!(r.resolve("file-chain").is_ok());
        c.router.combos = vec![];
        c.combos = vec![];
        r.reload(&c);
        assert!(
            r.resolve("file-chain").is_err(),
            "a deleted file combo must disappear from resolution"
        );
    }

    #[test]
    fn virtual_model_alias_resolution() {
        let r = Registry::from_config(&cfg(
            vec![provider("openai", vec!["gpt-4o-mini"], false)],
            vec![ComboConfig {
                name: "smart-chain".into(),
                description: None,
                models: vec!["openai/gpt-4o-mini".into()],
            }],
        ));

        // Alias to direct model
        r.set_alias("fast", "openai/gpt-4o-mini");
        let res = r.resolve("fast").unwrap();
        match res {
            Resolution::Single(c) => assert_eq!(c.model, "openai/gpt-4o-mini"),
            _ => panic!("expected Single"),
        }

        // Alias to combo
        r.set_alias("smart", "smart-chain");
        let res2 = r.resolve("smart").unwrap();
        match res2 {
            Resolution::Chain(cs) => {
                assert_eq!(cs.len(), 1);
                assert_eq!(cs[0].model, "openai/gpt-4o-mini");
            }
            _ => panic!("expected Chain"),
        }

        // Alias chaining (alias -> alias -> target)
        r.set_alias("flagship", "smart");
        let res3 = r.resolve("flagship").unwrap();
        match res3 {
            Resolution::Chain(cs) => assert_eq!(cs[0].model, "openai/gpt-4o-mini"),
            _ => panic!("expected Chain"),
        }

        // Models list includes aliases
        let list = r.list();
        assert!(list.iter().any(|m| m.id == "fast" && m.owned_by == "alias"));
        assert!(list
            .iter()
            .any(|m| m.id == "smart" && m.owned_by == "alias"));
        assert!(list
            .iter()
            .any(|m| m.id == "flagship" && m.owned_by == "alias"));
    }
}

//! Configuration errors.

use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ConfigError {
    #[error("cannot read config `{path}`: {reason}")]
    Read { path: String, reason: String },

    #[error("cannot parse config `{path}`: {reason}")]
    Parse { path: String, reason: String },

    #[error("invalid config: {0}")]
    Invalid(String),

    #[error("combo `{combo}` refers to unknown model `{model}`")]
    UnknownModel { combo: String, model: String },
}

impl ConfigError {
    pub fn read(path: &str, source: impl std::fmt::Display) -> Self {
        ConfigError::Read {
            path: path.to_string(),
            reason: source.to_string(),
        }
    }

    pub fn parse(path: &str, source: impl std::fmt::Display) -> Self {
        ConfigError::Parse {
            path: path.to_string(),
            reason: source.to_string(),
        }
    }

    pub fn invalid(msg: impl Into<String>) -> Self {
        ConfigError::Invalid(msg.into())
    }

    pub fn unknown_model(combo: &str, model: &str) -> Self {
        ConfigError::UnknownModel {
            combo: combo.to_string(),
            model: model.to_string(),
        }
    }
}

pub type ConfigResult<T> = Result<T, ConfigError>;

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        ConfigError::Parse {
            path: "<unknown>".into(),
            reason: e.to_string(),
        }
    }
}

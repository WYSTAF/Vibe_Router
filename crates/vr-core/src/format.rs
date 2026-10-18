//! Wire formats understood by Vibe Router.
//!
//! v0.1 speaks OpenAI-shaped traffic in and out. Other formats are declared
//! here so that `vr-translate` can grow without the enum becoming a stringly
//!typed surface elsewhere.

/// A request/response wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WireFormat {
    /// OpenAI Chat Completions (`/v1/chat/completions`). The default and the
    /// only format v0.1 translates.
    #[default]
    OpenAi,
    // Reserved, deliberately unimplemented in v0.1:
    //   AnthropicMessages,  // /v1/messages
    //   OpenAiResponses,    // /v1/responses
    //   Gemini,             // /v1beta/models/*:generateContent
}

impl WireFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            WireFormat::OpenAi => "openai",
        }
    }
}

impl std::fmt::Display for WireFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for WireFormat {
    type Err = UnknownFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai-chat" | "chat" => Ok(WireFormat::OpenAi),
            other => Err(UnknownFormat(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownFormat(pub String);

impl std::fmt::Display for UnknownFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown wire format: {} (expected `openai`)", self.0)
    }
}

impl std::error::Error for UnknownFormat {}

use reqwest::Client;

use crate::summarizer::Summarizer;

const DEFAULT_MODEL: &str = "gemini-2.5-flash";

pub struct GeminiClient {
    client: Client,
    api_key: String,
    model: String,
}

impl GeminiClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            ..Default::default()
        }
    }

    pub fn model(mut self, model: String) -> Self {
        self.model = model;
        self
    }
}

impl Default for GeminiClient {
    fn default() -> Self {
        Self {
            client: Client::new(),
            api_key: Default::default(),
            model: DEFAULT_MODEL.to_string(),
        }
    }
}

impl Summarizer for GeminiClient {
    fn summarize(&self, _content: String) -> anyhow::Result<()> {
        todo!()
    }
}

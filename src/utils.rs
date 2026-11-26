use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::http;
use hyper::{Response, StatusCode};

use base64::{Engine as _, engine::general_purpose::STANDARD};

pub type Reply = Response<BoxBody<Bytes, hyper::Error>>;

pub fn response(status: StatusCode, message: &'static str) -> Reply {
    Response::builder()
        .status(status)
        .body(full(message))
        .unwrap()
}

pub fn host_addr(uri: &http::Uri) -> Option<String> {
    uri.authority().map(|auth| auth.to_string())
}

pub fn header_value<T: hyper::header::AsHeaderName>(
    req: &http::Request<hyper::body::Incoming>,
    key: T,
) -> Option<&str> {
    req.headers().get(key).and_then(|h| h.to_str().ok())
}

pub fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

pub fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[derive(Clone, Debug)]
pub struct UriParts {
    host_addr: String,
    username: String,
    password: String,
}

impl UriParts {
    pub fn host_addr(&self) -> &str {
        &self.host_addr
    }

    pub fn proxy_authorization_header(&self) -> String {
        format!(
            "Bearer {}",
            STANDARD.encode(format!("{}:{}", self.username, self.password))
        )
    }
}

impl TryFrom<String> for UriParts {
    type Error = eyre::Report;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.parse::<hyper::Uri>() {
            Ok(uri) => uri.try_into(),
            Err(err) => Err(eyre::eyre!("Error {err}")),
        }
    }
}

impl std::fmt::Display for UriParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}:{}@{}",
            self.username, self.password, self.host_addr
        ))
    }
}

impl TryFrom<hyper::Uri> for UriParts {
    type Error = eyre::Report;

    fn try_from(value: hyper::Uri) -> Result<Self, Self::Error> {
        match value.authority() {
            Some(authority) => {
                let creds = {
                    let authority = authority.as_str();
                    authority.rfind('@').map(|i| &authority[..i])
                }
                .and_then(|v| v.rfind(':').map(|i| (&v[..i], &v[i + 1..])));

                match creds {
                    Some((username, password)) => Ok(Self {
                        host_addr: format!(
                            "{}:{}",
                            authority.host(),
                            authority.port_u16().unwrap_or(80)
                        ),
                        username: username.to_string(),
                        password: password.to_string(),
                    }),
                    None => Err(eyre::eyre!("Missing creds")),
                }
            }
            None => Err(eyre::eyre!("Missing authority")),
        }
    }
}

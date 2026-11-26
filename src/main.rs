use std::net::SocketAddr;

use log::{error, info, trace};
use tokio::{sync::mpsc, try_join};

use crate::{control::Control, proxy::Proxy, utils::UriParts};

use color_eyre::eyre::Result;

mod control;
mod proxy;
mod utils;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Config {
    #[serde(deserialize_with = "de_upstream")]
    upstream: hyper::Uri,
}

fn de_upstream<'de, D>(deserializer: D) -> Result<hyper::Uri, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let uri: String = Deserialize::deserialize(deserializer)?;
    uri.parse::<hyper::Uri>().map_err(serde::de::Error::custom)
}

#[derive(Debug)]
enum Command {
    Switch(UriParts),
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    env_logger::try_init()?;

    dotenvy::dotenv()?;
    let config = envy::from_env::<Config>()?;
    info!("{:?}", config);

    let proxy_addr = SocketAddr::from(([127, 0, 0, 1], 8100));
    let control_addr = SocketAddr::from(([127, 0, 0, 1], 8101));

    let (tx, rx) = mpsc::channel::<Command>(4);

    let mut p = Proxy::new(rx, config.upstream.try_into()?);
    let proxy_srv = p.run(&proxy_addr);

    let c = Control::new(tx);
    let control_srv = c.run(&control_addr);

    info!("Proxy listening on http://{}", proxy_addr);
    info!("Control listening on http://{}", control_addr);

    match try_join!(proxy_srv, control_srv) {
        Ok(_) => info!("Done"),
        Err(err) => error!("Error: {err}"),
    }

    Ok(())
}

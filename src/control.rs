use std::net::SocketAddr;

use bytes::Buf;
use eyre::Result;
use http_body_util::BodyExt;
use hyper::{Method, Request, Response, StatusCode, service::service_fn};
use hyper_util::rt::TokioIo;
use log::warn;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::mpsc::Sender, time::Instant};

use crate::{
    Command,
    utils::{Reply, empty, full, response},
};

type ServerBuilder = hyper::server::conn::http1::Builder;

fn method_not_allowed<S: AsRef<str>>(methods: S) -> Reply {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(hyper::header::ALLOW, methods.as_ref())
        .body(empty())
        .unwrap()
}

fn not_found() -> Reply {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(empty())
        .unwrap()
}

fn unprocessable_entity() -> Reply {
    Response::builder()
        .status(StatusCode::UNPROCESSABLE_ENTITY)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(empty())
        .unwrap()
}

#[derive(Serialize)]
struct InternalError {
    error: String,
}

fn internal_error<S: std::fmt::Display>(err: S) -> Reply {
    let body = serde_json::to_string(&InternalError {
        error: err.to_string(),
    })
    .unwrap_or("Internal server error".to_string());

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(full(body))
        .unwrap()
}

pub struct Control {
    tx: Sender<Command>,
}

impl Control {
    pub fn new(tx: Sender<Command>) -> Self {
        Self { tx }
    }

    pub async fn run(&self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(err) => {
                    warn!("Unable to accept connection: {err}");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let tx = self.tx.clone();

            tokio::task::spawn(async move {
                if let Err(err) = ServerBuilder::new()
                    .preserve_header_case(true)
                    .serve_connection(
                        io,
                        service_fn(|req: Request<hyper::body::Incoming>| {
                            let tx = tx.clone();
                            async move { main(req, &tx).await }
                        }),
                    )
                    .with_upgrades()
                    .await
                {
                    println!("Failed to serve connection: {:?}", err);
                }
            });
        }
    }
}

async fn main(req: Request<hyper::body::Incoming>, tx: &Sender<Command>) -> Result<Reply> {
    let time = Instant::now();

    let method = req.method().to_string();
    let uri = req.uri().to_string();

    let response = match req.uri().path() {
        "/switch" => handle_switch(req, &tx)
            .await
            .or_else(|err| Ok(internal_error(err))),
        _ => Ok(not_found()),
    };

    eprintln!("{} {} {:?}", method, uri, time.elapsed(),);

    response
}

#[derive(Serialize, Deserialize)]
struct HandleSwitchRequest {
    uri: String,
}

async fn handle_switch(req: Request<hyper::body::Incoming>, tx: &Sender<Command>) -> Result<Reply> {
    if req.method() != Method::POST {
        return Ok(method_not_allowed("POST"));
    }

    let body = req.collect().await?.aggregate();
    let data: HandleSwitchRequest = match serde_json::from_reader(body.reader()) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("Error: {err}");
            return Ok(unprocessable_entity());
        }
    };

    tx.send(Command::Switch(data.uri.try_into()?)).await?;

    Ok(response(StatusCode::OK, "OK"))
}

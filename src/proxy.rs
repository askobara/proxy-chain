use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use eyre::{Context, Result};
use fast_socks5::server::{
    DnsResolveHelper, Socks5ServerProtocol, SocksServerError, run_udp_proxy,
};
use fast_socks5::util::target_addr::TargetAddr;
use fast_socks5::{ReplyError, Socks5Command, parse_udp_request};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, Response};

use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use hyper_util::rt::TokioIo;
use tokio::sync::mpsc::Receiver;
use tokio::try_join;

use crate::utils::{UriParts, empty};
use crate::{Command, utils};

type ClientBuilder = hyper::client::conn::http1::Builder;
type ServerBuilder = hyper::server::conn::http1::Builder;

pub struct Proxy {
    rx: Receiver<Command>,
    uri: UriParts,
}

impl Proxy {
    pub fn new(rx: Receiver<Command>, uri: UriParts) -> Self {
        Self { rx, uri }
    }

    pub async fn run(&mut self, addr: &SocketAddr) -> Result<()> {
        let proxy_listener = TcpListener::bind(addr).await?;

        loop {
            tokio::select! {
                connection = proxy_listener.accept() => {
                    let stream = match connection {
                        Ok((stream, _)) => stream,
                        Err(err) => {
                            error!("Unable to accept connection: {err}");
                            continue;
                        }
                    };

                    let peer_addr = stream.peer_addr()?;

                    info!("Incoming connection: {:?}", &peer_addr);

                    let mut first = [0u8; 1];
                    stream.peek(&mut first).await?;

                    match first[0] {
                        0x05 => {
                            if let Err(err) = self.socks_proxy(stream).await  {
                                error!("Socks error: {err}");
                            } else {
                                debug!("Socks closed")
                            }
                        },
                        _ => self.http_proxy(stream).await?,
                    }

                    debug!("Incoming connection handled: {:?}", &peer_addr);
                }
                command = self.rx.recv() => {
                    match command {
                        Some(Command::Switch(uri)) => {
                            eprintln!("Switch proxy to: {uri}");
                            self.uri = uri;
                        }
                        None => {}
                    }
                }
            }
        }
    }

    async fn http_proxy(&self, stream: TcpStream) -> Result<()> {
        let io = TokioIo::new(stream);
        let uri = self.uri.clone();

        tokio::task::spawn(async move {
            let conn = ServerBuilder::new()
                .preserve_header_case(true)
                .serve_connection(
                    io,
                    service_fn(|req: Request<hyper::body::Incoming>| {
                        let uri = uri.clone();
                        async move { http_request(req, &uri).await }
                    }),
                )
                .with_upgrades();

            if let Err(err) = conn.await {
                println!("Failed to serve connection: {:?}", err);
            }
        });

        Ok(())
    }

    async fn socks_proxy(&self, stream: TcpStream) -> Result<()> {
        let reply_ip = stream.local_addr()?.ip();

        let (proto, cmd, target_addr) = {
            Socks5ServerProtocol::accept_no_auth(stream)
                .await?
                .read_command()
                .await?
                .resolve_dns()
                .await?
        };

        let uri = self.uri.clone();
        let target_addr = target_addr.clone();

        match cmd {
            Socks5Command::TCPConnect => {
                tokio::task::spawn(async move {
                    let _ = socks_tcp_request(proto, uri, target_addr).await;
                });
            }
            Socks5Command::UDPAssociate => {
                tokio::task::spawn(async move {
                    let _ = socks_udp_request(proto, reply_ip, uri, target_addr).await;
                });
            }
            Socks5Command::TCPBind => {
                error!("TCPBind not supported");
                proto.reply_error(&ReplyError::CommandNotSupported).await?;
                return Err(ReplyError::CommandNotSupported.into());
            }
        };

        Ok(())
    }
}

async fn http_request(
    req: Request<hyper::body::Incoming>,
    uri: &UriParts,
) -> Result<utils::Reply, hyper::Error> {
    println!("req: {:?}", req);

    if Method::CONNECT == req.method() {
        // Connect to the upstream proxy.
        // let upstream_stream = match TcpStream::connect(uri.host_addr()).await {
        //     Ok(stream) => stream,
        //     Err(err) => {
        //         eprintln!("Upstream connection failed: {err}");
        //         return Ok(utils::response(
        //             StatusCode::BAD_GATEWAY,
        //             "Upstream connection failed",
        //         ));
        //     }
        // };
        //
        // // Establish an HTTP/1 connection to the upstream proxy.
        // let upstream_io = TokioIo::new(upstream_stream);
        //
        // // `handshake()` splits the upstream HTTP connection into two parts:
        // // `sender` sends HTTP requests and receives their responses.
        // // `connection` is a future that drives the HTTP connection itself: it reads
        // // from and writes to the socket, processes HTTP messages, and manages connection state.
        // let (mut sender, connection) =
        //     match ClientBuilder::new()
        //         .preserve_header_case(true)
        //         .handshake(upstream_io)
        //         .await
        //      {
        //         Ok(parts) => parts,
        //         Err(err) => {
        //             eprintln!("Upstream HTTP handshake failed: {err}");
        //             return Ok(utils::response(
        //                 StatusCode::BAD_GATEWAY,
        //                 "Upstream HTTP handshake failed",
        //             ));
        //         }
        //     };
        //
        // tokio::task::spawn(async move {
        //     if let Err(err) = connection.with_upgrades().await {
        //         eprintln!("Upstream connection error: {err}");
        //     }
        // });
        //
        // let authority = match utils::host_addr(req.uri()) {
        //     Some(authority) => authority.to_string(),
        //     None => {
        //         return Ok(utils::response(
        //             StatusCode::BAD_REQUEST,
        //             "Expected CONNECT host:port",
        //         ));
        //     }
        // };
        //
        // let proxy_authorization = match utils::header_value(&req, hyper::header::PROXY_AUTHORIZATION) {
        //     Some(authorization) => authorization.to_string(),
        //     None => uri.proxy_authorization_header(),
        // };
        //
        // let upstream_req = match Request::builder()
        //     .method(Method::CONNECT)
        //     .uri(&authority)
        //     .header(hyper::header::HOST, &authority)
        //     .header(hyper::header::PROXY_AUTHORIZATION, &proxy_authorization)
        //     .body(Full::new(Bytes::new()))
        // {
        //     Ok(req) => req,
        //     Err(err) => {
        //         eprintln!("Building upstream request failed: {err}");
        //         return Ok(utils::response(
        //             StatusCode::BAD_GATEWAY,
        //             "Could not build upstream request",
        //         ));
        //     }
        // };
        //
        // let upstream_response = match sender.send_request(upstream_req).await {
        //     Ok(res) => res,
        //     Err(err) => {
        //         eprintln!("Upstream CONNECT failed: {err}");
        //         return Ok(utils::response(
        //             StatusCode::BAD_GATEWAY,
        //             "Upstream CONNECT failed",
        //         ));
        //     }
        // };
        //
        // if !upstream_response.status().is_success() {
        //     eprintln!("Upstream rejected CONNECT: {}", upstream_response.status());
        //
        //     return Ok(utils::response(
        //         StatusCode::BAD_GATEWAY,
        //         "Upstream proxy rejected CONNECT",
        //     ));
        // }

        let target = req.uri().clone();
        let uri = uri.clone();
        let upstream_upgrade = upstream_http_connect(uri, target);

        // Capture the client connection's pending upgrade.
        let client_upgrade = hyper::upgrade::on(req);

        // Capture the upstream raw connection before moving the response.
        // let upstream_upgrade = hyper::upgrade::on(upstream_response);

        // Both sides are ready. Start relaying once the client receives 200.
        tokio::task::spawn(async move {
            match (client_upgrade.await, upstream_upgrade.await) {
                (Ok(client_upgraded), Ok(upstream_upgraded)) => {
                    if let Err(err) = tunnel(
                        TokioIo::new(client_upgraded),
                        TokioIo::new(upstream_upgraded),
                    )
                    .await
                    {
                        error!("Tunnel error: {err}");
                    }
                }
                (Err(err), _) => error!("Client upgrade error: {err}"),
                (_, Err(err)) => error!("Upsteam upgrade error: {err}"),
            }
        });

        Ok(Response::new(utils::empty()))
    } else {
        todo!();
        // let authority = match utils::host_addr(req.uri()) {
        //     Some(authority) => authority.to_string(),
        //     None => {
        //         return Ok(utils::response(
        //             StatusCode::BAD_REQUEST,
        //             "Expected CONNECT host:port",
        //         ));
        //     }
        // };
        //
        // let stream = match TcpStream::connect(authority).await {
        //     Ok(stream) => stream,
        //     Err(err) => {
        //         eprintln!("Upstream connection failed: {err}");
        //         return Ok(utils::response(
        //             StatusCode::BAD_GATEWAY,
        //             "Upstream connection failed",
        //         ));
        //     }
        // };
        // let io = TokioIo::new(stream);
        //
        // let (mut sender, conn) = ClientBuilder::new()
        //     .preserve_header_case(true)
        //     .title_case_headers(true)
        //     .handshake(io)
        //     .await?;
        //
        // tokio::task::spawn(async move {
        //     if let Err(err) = conn.await {
        //         println!("Connection failed: {:?}", err);
        //     }
        // });
        //
        // let resp = sender.send_request(req).await?;
        // Ok(resp.map(|b| b.boxed()))
    }
}

// fn udp_bind_random_port() -> io::Result<Socket> {
//         const V4_UNSPEC: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
//         const V6_UNSPEC: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
//
//         Socket::new(Domain::IPV6, Type::DGRAM, None)
//             .and_then(|socket| socket.set_only_v6(false).map(|_| socket))
//             .and_then(|socket| socket.bind(&V6_UNSPEC.into()).map(|_| socket))
//             .or_else(|_| {
//                 Socket::new(Domain::IPV4, Type::DGRAM, None)
//                     .and_then(|socket| socket.bind(&V4_UNSPEC.into()).map(|_| socket))
//             })
//     .and_then(|socket| socket.set_nonblocking(true).map(|_| socket))
// }

pub async fn wait_on_tcp<I>(stream: &mut I) -> Result<()>
where
    I: AsyncRead + Unpin,
{
    let mut buf = [0; 1];
    match stream.read(&mut buf).await {
        Ok(0) => Err(SocksServerError::EOF).wrap_err("eof"),
        Ok(_) => Err(SocksServerError::UnexpectedUdpControlGarbage(buf[0])).wrap_err(""),
        Err(err) => Err(eyre::eyre!(err)),
    }
}

async fn socks_udp_request<T>(
    proto: Socks5ServerProtocol<T, fast_socks5::server::states::CommandRead>,
    reply_ip: IpAddr,
    uri: UriParts,
    target_addr: TargetAddr,
) -> Result<()>
where
    T: tokio::io::AsyncWrite + tokio::io::AsyncRead + Unpin,
{
    let peer_sock = UdpSocket::bind("0.0.0.0:0").await?;
    let peer_addr = peer_sock.local_addr()?;
    let reply_port = peer_addr.port();
    debug!("peer_addr = {}, reply_ip = {}", peer_addr, reply_ip);

    // inner is a tcp connection which is accepted by the server
    // we just notify a client about successful associate with ip:port
    let mut inner = proto
        .reply_success(SocketAddr::new(reply_ip, reply_port))
        .await?;

    let upstream = upstream_http_connect(uri, target_addr.to_string()).await?;

    let udp_fut = socks_transfer(peer_sock, upstream);
    let tcp_fut = wait_on_tcp(&mut inner);

    match try_join!(udp_fut, tcp_fut) {
        Ok(_) => warn!("unreachable"),
        Err(err) => {
            match err.downcast_ref::<SocksServerError>() {
                Some(SocksServerError::EOF) => {
                    debug!("EOF on controlling TCP stream, closed UDP proxy")
                }
                Some(err) => warn!("while UDP proxying: {err}"),
                None => warn!("while UDP proxying: {err}"),
            };
        }
    }

    Ok(())
}

async fn socks_transfer(peer_sock: UdpSocket, upstream: Upgraded) -> Result<()> {
    let mut buf = vec![0u8; 8192];
    let (len, addr) = peer_sock.recv_from(&mut buf).await?;
    debug!("{} {}", len, addr);

    peer_sock.connect(addr).await?;

    let (frag, target_addr, payload) = parse_udp_request(&buf).await?;

    debug!("{} {}", frag, target_addr);

    TokioIo::new(upstream).read_to_end(&mut buf).await?;

    // todo: handle close of inner

    loop {
        // Wait for the socket to be writable
        peer_sock.writable().await?;

        // Try to send data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match peer_sock.try_send(&buf) {
            Ok(n) => {
                break Ok(());
            }
            Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(eyre::eyre!(e));
            }
        }
    }
}

async fn socks_tcp_request<T>(
    proto: Socks5ServerProtocol<T, fast_socks5::server::states::CommandRead>,
    uri: UriParts,
    target_addr: TargetAddr,
) -> Result<()>
where
    T: tokio::io::AsyncWrite + tokio::io::AsyncRead + Unpin,
{
    let upstream_upgraded = upstream_http_connect(uri, target_addr.to_string()).await?;

    let inbound = proto
        // We use port 0 to let the operating system allocate an available port for us.
        .reply_success(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await?;

    if let Err(err) = tunnel(inbound, TokioIo::new(upstream_upgraded)).await {
        error!("Socks tunnel error: {err}");
    }

    Ok(())
}

async fn tunnel<A, B>(mut client: A, mut upstream: B) -> Result<()>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (sent, received) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;

    debug!("Tunnel closed: client -> upstream {sent} bytes, upstream -> client {received} bytes");

    Ok(())
}

async fn upstream_http_connect<T>(uri: UriParts, target: T) -> Result<Upgraded>
where
    T: TryInto<hyper::Uri>,
    <T as TryInto<hyper::Uri>>::Error: Into<hyper::http::Error>,
{
    // Connect to the upstream proxy.
    let upstream_stream =
        tokio::time::timeout(Duration::from_secs(30), TcpStream::connect(uri.host_addr()))
            .await??;

    // Establish an HTTP/1 connection to the upstream proxy.
    let upstream_io = TokioIo::new(upstream_stream);

    // `handshake()` splits the upstream HTTP connection into two parts:
    // `sender` sends HTTP requests and receives their responses.
    // `connection` is a future that drives the HTTP connection itself: it reads
    // from and writes to the socket, processes HTTP messages, and manages connection state.
    let (mut sender, connection) = tokio::time::timeout(
        Duration::from_secs(10),
        ClientBuilder::new()
            .preserve_header_case(true)
            .handshake(upstream_io),
    )
    .await??;

    tokio::task::spawn(async move {
        if let Err(err) = connection.with_upgrades().await {
            eprintln!("Upstream connection error: {err}");
        }
    });

    let upstream_req = Request::builder()
        .method(Method::CONNECT)
        .uri(target)
        .header(
            hyper::header::PROXY_AUTHORIZATION,
            &uri.proxy_authorization_header(),
        )
        .body(empty())?;

    let upstream_response =
        tokio::time::timeout(Duration::from_secs(30), sender.send_request(upstream_req)).await??;

    debug!("upstream response {}", upstream_response.status());

    if !upstream_response.status().is_success() {
        return Err(eyre::eyre!(
            "Upsteam rejected CONNECT: {}",
            upstream_response.status()
        ));
    }

    // Capture the upstream raw connection before moving the response.
    hyper::upgrade::on(upstream_response)
        .await
        .map_err(|err| eyre::Report::new(err))
}

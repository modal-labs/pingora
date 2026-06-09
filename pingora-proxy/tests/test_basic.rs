// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod utils;

use bytes::Bytes;
use h2::client;
use http::Request;
use hyper::{body::HttpBody, header::HeaderValue, Body, Client};
#[cfg(unix)]
use hyperlocal::{UnixClientExt, Uri};
use reqwest::{header, StatusCode};
use std::future::poll_fn;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};

use utils::server_utils::init;

fn is_specified_port(port: u16) -> bool {
    (1..65535).contains(&port)
}

#[tokio::test]
async fn test_origin_alive() {
    init();
    let res = reqwest::get("http://127.0.0.1:8000/").await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
async fn test_simple_proxy() {
    init();
    let res = reqwest::get("http://127.0.0.1:6147").await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers["x-server-addr"], "127.0.0.1:6147");
    let sockaddr = headers["x-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.1");
    assert!(is_specified_port(sockaddr.port()));

    assert_eq!(headers["x-upstream-server-addr"], "127.0.0.1:8000");
    let sockaddr = headers["x-upstream-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.2");
    assert!(is_specified_port(sockaddr.port()));

    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_h2_to_h1() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let res = client
        .get("https://127.0.0.1:6150")
        .header("sni", "openrusty.org")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);

    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers["x-server-addr"], "127.0.0.1:6150");

    let sockaddr = headers["x-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.1");
    assert!(is_specified_port(sockaddr.port()));

    assert_eq!(headers["x-upstream-server-addr"], "127.0.0.1:8443");
    let sockaddr = headers["x-upstream-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.2");
    assert!(is_specified_port(sockaddr.port()));

    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_h2_to_h2() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let res = client
        .get("https://127.0.0.1:6150")
        .header("sni", "openrusty.org")
        .header("x-h2", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);

    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers["x-server-addr"], "127.0.0.1:6150");
    let sockaddr = headers["x-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.1");
    assert!(is_specified_port(sockaddr.port()));

    assert_eq!(headers["x-upstream-server-addr"], "127.0.0.1:8443");
    let sockaddr = headers["x-upstream-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.2");
    assert!(is_specified_port(sockaddr.port()));

    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
async fn test_h2c_to_h2c() {
    init();

    let client = hyper::client::Client::builder()
        .http2_only(true)
        .build_http();

    let mut req = hyper::Request::builder()
        .uri("http://127.0.0.1:6146")
        .body(Body::empty())
        .unwrap();
    req.headers_mut()
        .insert("x-h2", HeaderValue::from_bytes(b"true").unwrap());
    let res = client.request(req).await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);

    let body = res.into_body().data().await.unwrap().unwrap();
    assert_eq!(body.as_ref(), b"Hello World!\n");
}

const H2C_PROXY_ADDR: &str = "127.0.0.1:6146";
const H2_BODY_CHUNK_SIZE: usize = 16 * 1024;
const DEFAULT_H2_STREAM_WINDOW: usize = 65_535;
const DOWNSTREAM_RST_OBSERVATION_WINDOW: Duration = Duration::from_millis(200);

struct StallingH2Origin {
    port: u16,
    upstream_window_full_rx: oneshot::Receiver<()>,
    upstream_reset_rx: oneshot::Receiver<()>,
    shutdown_tx: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

async fn spawn_stalling_h2_origin() -> StallingH2Origin {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let (upstream_window_full_tx, upstream_window_full_rx) = oneshot::channel();
    let (upstream_reset_tx, upstream_reset_rx) = oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = h2::server::handshake(stream).await.unwrap();
        let Some(request) = conn.accept().await else {
            return;
        };
        let (request, _send_response) = request.unwrap();
        let mut body = request.into_body();
        let mut bytes_read = 0;
        let mut upstream_window_full_tx = Some(upstream_window_full_tx);
        let mut upstream_reset_tx = Some(upstream_reset_tx);

        loop {
            tokio::select! {
                data = body.data() => {
                    match data {
                        Some(Ok(data)) => {
                            bytes_read += data.len();
                            if bytes_read >= DEFAULT_H2_STREAM_WINDOW {
                                if let Some(tx) = upstream_window_full_tx.take() {
                                    let _ = tx.send(());
                                }
                            }
                            // Intentionally do not release flow-control capacity.
                        }
                        Some(Err(_)) => {
                            if let Some(tx) = upstream_reset_tx.take() {
                                let _ = tx.send(());
                            }
                            return;
                        }
                        None => return,
                    }
                }
                _ = &mut shutdown_rx => return,
            }
        }
    });

    StallingH2Origin {
        port,
        upstream_window_full_rx,
        upstream_reset_rx,
        shutdown_tx,
        handle,
    }
}

async fn send_downstream_reset_after_upstream_write_stalls(origin: StallingH2Origin, path: &str) {
    let StallingH2Origin {
        port,
        mut upstream_window_full_rx,
        mut upstream_reset_rx,
        shutdown_tx,
        handle,
    } = origin;

    let tcp = TcpStream::connect(H2C_PROXY_ADDR).await.unwrap();
    let (mut client, connection) = client::handshake(tcp).await.unwrap();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{H2C_PROXY_ADDR}{path}"))
        .header("x-h2", "true")
        .header("x-port", port.to_string())
        .body(())
        .unwrap();
    let (_response, mut body) = client.send_request(request, false).unwrap();

    timeout(Duration::from_secs(5), async {
        loop {
            body.reserve_capacity(H2_BODY_CHUNK_SIZE);
            tokio::select! {
                result = &mut upstream_window_full_rx => {
                    result.unwrap();
                    break;
                }
                capacity = poll_fn(|cx| body.poll_capacity(cx)) => {
                    let capacity = capacity
                        .expect("downstream request body stream closed")
                        .expect("downstream request body capacity error");
                    let len = capacity.min(H2_BODY_CHUNK_SIZE);
                    if len > 0 {
                        body.send_data(Bytes::from(vec![b'a'; len]), false).unwrap();
                    }
                }
            }
        }
    })
    .await
    .expect("upstream h2 receive window did not fill");

    body.send_reset(h2::Reason::CANCEL);

    assert!(
        timeout(DOWNSTREAM_RST_OBSERVATION_WINDOW, &mut upstream_reset_rx)
            .await
            .is_err(),
        "upstream saw a reset immediately after downstream RST_STREAM"
    );

    let _ = shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(1), handle).await;
    drop(body);
    drop(client);
    let _ = timeout(Duration::from_secs(1), connection_handle).await;
}

#[tokio::test]
async fn test_h2_downstream_reset_while_blocked_sending_upstream_body() {
    init();

    let origin = spawn_stalling_h2_origin().await;
    send_downstream_reset_after_upstream_write_stalls(origin, "/blocked-h2-upstream").await;
}

#[tokio::test]
async fn test_h2_downstream_reset_while_blocked_sending_upstream_body_under_load() {
    init();

    let mut handles = Vec::new();
    for i in 0..4 {
        let origin = spawn_stalling_h2_origin().await;
        handles.push(tokio::spawn(async move {
            send_downstream_reset_after_upstream_write_stalls(
                origin,
                &format!("/blocked-h2-upstream-load-{i}"),
            )
            .await;
        }));
    }

    for handle in handles {
        timeout(Duration::from_secs(10), handle)
            .await
            .expect("load repro task timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn test_h1_on_h2c_port() {
    init();

    let client = hyper::client::Client::builder()
        .http2_only(false)
        .build_http();

    let mut req = hyper::Request::builder()
        .uri("http://127.0.0.1:6146")
        .body(Body::empty())
        .unwrap();
    req.headers_mut()
        .insert("x-h2", HeaderValue::from_bytes(b"true").unwrap());
    let res = client.request(req).await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_11);

    let body = res.into_body().data().await.unwrap().unwrap();
    assert_eq!(body.as_ref(), b"Hello World!\n");
}

#[tokio::test]
#[cfg(feature = "openssl_derived")]
async fn test_h2_to_h2_host_override() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let res = client
        .get("https://127.0.0.1:6150")
        .header("x-h2", "true")
        .header("host-override", "test.com")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);
    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_h2_to_h2_upload() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let payload = "test upload";

    let res = client
        .get("https://127.0.0.1:6150/echo")
        .header("sni", "openrusty.org")
        .header("x-h2", "true")
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);
    let body = res.text().await.unwrap();
    assert_eq!(body, payload);
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_h2_to_h1_upload() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let payload = "test upload";

    let res = client
        .get("https://127.0.0.1:6150/echo")
        .header("sni", "openrusty.org")
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);
    let body = res.text().await.unwrap();
    assert_eq!(body, payload);
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_h2_head() {
    init();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let res = client
        .head("https://127.0.0.1:6150/set_content_length")
        .header("sni", "openrusty.org")
        .header("x-h2", "true")
        .header("x-set-content-length", "11")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);
    let body = res.text().await.unwrap();
    // should not be any body, despite content-length
    assert_eq!(body, "");
}

#[cfg(unix)]
#[tokio::test]
async fn test_simple_proxy_uds() {
    init();
    let url = Uri::new("/tmp/pingora_proxy.sock", "/").into();
    let client = Client::unix();

    let res = client.get(url).await.unwrap();

    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let (resp, body) = res.into_parts();

    let headers = &resp.headers;
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers["x-server-addr"], "/tmp/pingora_proxy.sock");
    assert_eq!(headers["x-client-addr"], "unset"); // unnamed UDS

    assert_eq!(headers["x-upstream-server-addr"], "127.0.0.1:8000");
    let sockaddr = headers["x-upstream-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.2");
    assert!(is_specified_port(sockaddr.port()));

    let body = hyper::body::to_bytes(body).await.unwrap();
    assert_eq!(body.as_ref(), b"Hello World!\n");
}

#[cfg(unix)]
#[tokio::test]
async fn test_simple_proxy_uds_peer() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6147")
        .header("x-uds-peer", "1") // force upstream peer to be UDS
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);

    let headers = &res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers["x-server-addr"], "127.0.0.1:6147");
    let sockaddr = headers["x-client-addr"]
        .to_str()
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    assert_eq!(sockaddr.ip().to_string(), "127.0.0.1");
    assert!(is_specified_port(sockaddr.port()));

    assert_eq!(headers["x-upstream-client-addr"], "unset"); // unnamed UDS
    assert_eq!(
        headers["x-upstream-server-addr"],
        "/tmp/pingora_nginx_test.sock"
    );

    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

async fn test_dropped_conn_get() {
    init();
    let client = reqwest::Client::new();
    let port = "8001"; // special port to avoid unexpected connection reuse from other tests

    for _ in 1..3 {
        // load conns into pool
        let res = client
            .get("http://127.0.0.1:6147")
            .header("x-port", port)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let res = client
        .get("http://127.0.0.1:6147/bad_lb")
        .header("x-port", port)
        .send()
        .await
        .unwrap();

    // retry gives 200
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert_eq!(body, "dog!\n");
}

async fn test_dropped_conn_post_empty_body() {
    init();
    let client = reqwest::Client::new();
    let port = "8001"; // special port to avoid unexpected connection reuse from other tests

    for _ in 1..3 {
        // load conn into pool
        let res = client
            .get("http://127.0.0.1:6147")
            .header("x-port", port)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let res = client
        .post("http://127.0.0.1:6147/bad_lb")
        .header("x-port", port)
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert_eq!(body, "dog!\n");
}

async fn test_dropped_conn_post_body() {
    init();
    let client = reqwest::Client::new();
    let port = "8001"; // special port to avoid unexpected connection reuse from other tests

    for _ in 1..3 {
        // load conn into pool
        let res = client
            .get("http://127.0.0.1:6147")
            .header("x-port", port)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let res = client
        .post("http://127.0.0.1:6147/bad_lb")
        .header("x-port", port)
        .body("cat!")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.text().await.unwrap();
    assert_eq!(body, "cat!\n");
}

async fn test_dropped_conn_post_body_over() {
    init();
    let client = reqwest::Client::new();
    let port = "8001"; // special port to avoid unexpected connection reuse from other tests
    let large_body = String::from_utf8(vec![b'e'; 1024 * 64 + 1]).unwrap();

    for _ in 1..3 {
        // load conn into pool
        let res = client
            .get("http://127.0.0.1:6147")
            .header("x-port", port)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    let res = client
        .post("http://127.0.0.1:6147/bad_lb")
        .header("x-port", port)
        .body(large_body)
        .send()
        .await
        .unwrap();

    // 502, body larger than buffer limit
    assert_eq!(res.status(), StatusCode::from_u16(502).unwrap());
}

#[tokio::test]
async fn test_dropped_conn() {
    // These tests can race with each other
    // So force run them sequentially
    test_dropped_conn_get().await;
    test_dropped_conn_post_empty_body().await;
    test_dropped_conn_post_body().await;
    test_dropped_conn_post_body_over().await;
}

// currently not supported with Rustls implementation
#[cfg(feature = "openssl_derived")]
#[tokio::test]
async fn test_tls_no_verify() {
    init();
    let client = reqwest::Client::new();
    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_verify_sni_not_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

// currently not supported with Rustls implementation
#[cfg(feature = "openssl_derived")]
#[tokio::test]
async fn test_tls_none_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_verify_sni_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_underscore_sub_sni_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "d_g.openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_underscore_non_sub_sni_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "open_rusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let headers = res.headers();
    assert_eq!(headers[header::CONNECTION], "close");
}

#[cfg(feature = "openssl_derived")]
#[tokio::test]
async fn test_tls_alt_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "open_rusty.org")
        .header("alt", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "openssl_derived")]
#[tokio::test]
async fn test_tls_underscore_sub_alt_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "open_rusty.org")
        .header("alt", "d_g.openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_underscore_non_sub_alt_verify_host() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("sni", "open_rusty.org")
        .header("alt", "open_rusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn test_upstream_compression() {
    init();

    // disable reqwest gzip support to check compression headers and body
    // otherwise reqwest will decompress and strip the headers
    let client = reqwest::ClientBuilder::new().gzip(false).build().unwrap();
    let res = client
        .get("http://127.0.0.1:6147/no_compression")
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("Content-Encoding").unwrap(), "gzip");
    let body = res.bytes().await.unwrap();
    assert!(body.len() < 32);

    // Next let reqwest decompress to validate the data
    let client = reqwest::ClientBuilder::new().gzip(true).build().unwrap();
    let res = client
        .get("http://127.0.0.1:6147/no_compression")
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &[b'B'; 32]);
}

#[tokio::test]
async fn test_downstream_compression() {
    init();

    // disable reqwest gzip support to check compression headers and body
    // otherwise reqwest will decompress and strip the headers
    let client = reqwest::ClientBuilder::new().gzip(false).build().unwrap();
    let res = client
        .get("http://127.0.0.1:6147/no_compression")
        // tell the test proxy to use downstream compression module instead of upstream
        .header("x-downstream-compression", "1")
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("Content-Encoding").unwrap(), "gzip");
    let body = res.bytes().await.unwrap();
    assert!(body.len() < 32);

    // Next let reqwest decompress to validate the data
    let client = reqwest::ClientBuilder::new().gzip(true).build().unwrap();
    let res = client
        .get("http://127.0.0.1:6147/no_compression")
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &[b'B'; 32]);
}

#[tokio::test]
async fn test_connect_close() {
    init();

    // default keep-alive
    let client = reqwest::ClientBuilder::new().build().unwrap();
    let res = client.get("http://127.0.0.1:6147").send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers[header::CONNECTION], "keep-alive");
    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");

    // close
    let client = reqwest::ClientBuilder::new().build().unwrap();
    let res = client
        .get("http://127.0.0.1:6147")
        .header("connection", "close")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "13");
    assert_eq!(headers[header::CONNECTION], "close");
    let body = res.text().await.unwrap();
    assert_eq!(body, "Hello World!\n");
}

#[tokio::test]
async fn test_connect_proxying_disallowed_h1() {
    init();

    let mut stream = TcpStream::connect("127.0.0.1:6147").await.unwrap();
    let request = b"CONNECT pingora.org:443 HTTP/1.1\r\nHost: pingora.org:443\r\n\r\n";
    stream.write_all(request).await.unwrap();

    let mut buf = [0u8; 1024];
    let read = stream.read(&mut buf).await.unwrap();
    let resp = std::str::from_utf8(&buf[..read]).unwrap();
    let status_line = resp.lines().next().unwrap_or("");
    assert!(status_line.contains(" 405 "));
}

#[tokio::test]
async fn test_connect_proxying_disallowed_h2() {
    init();

    let tcp = TcpStream::connect("127.0.0.1:6146").await.unwrap();
    let (mut h2, connection) = client::handshake(tcp).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });

    let request = Request::builder()
        .method("CONNECT")
        .uri("http://pingora.org:443/")
        .body(())
        .unwrap();
    let (response, _body) = h2.send_request(request, true).unwrap();
    let (head, mut body) = response.await.unwrap().into_parts();
    assert_eq!(head.status.as_u16(), 405);
    while let Some(chunk) = body.data().await {
        assert!(chunk.unwrap().is_empty());
    }
}

#[tokio::test]
async fn test_connect_proxying_allowed_h1() {
    init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();

    // Note per RFC CONNECT 2xx responses are not allowed to have response
    // bodies, so this is non-standard behavior.
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await.unwrap();
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        socket.write_all(response).await.unwrap();
        let _ = socket.shutdown().await;
    });

    let mut stream = TcpStream::connect("127.0.0.1:6160").await.unwrap();
    let request = format!(
        "CONNECT pingora.org:443 HTTP/1.1\r\nHost: pingora.org:443\r\nX-Port: {}\r\n\r\n",
        upstream_addr.port()
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let read = stream.read(&mut buf).await.unwrap();
    let resp = std::str::from_utf8(&buf[..read]).unwrap();
    let status_line = resp.lines().next().unwrap_or("");
    assert!(status_line.contains(" 200 "));
    assert!(resp.ends_with("ok"));
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_mtls_no_client_cert() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("x-port", "8444")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();

    // 400: because no cert
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_mtls_no_intermediate_cert() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/tls_verify")
        .header("x-port", "8444")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .header("client_cert", "1")
        .send()
        .await
        .unwrap();

    // 400: because no intermediate cert
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[cfg(feature = "any_tls")]
async fn test_mtls() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/")
        .header("x-port", "8444")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .header("client_cert", "1")
        .header("client_intermediate", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "any_tls")]
async fn assert_reuse(req: reqwest::RequestBuilder) {
    req.try_clone().unwrap().send().await.unwrap();
    let res = req.send().await.unwrap();
    let headers = res.headers();
    assert!(headers.get("x-conn-reuse").is_some());
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_mtls_diff_cert_no_reuse() {
    init();
    let client = reqwest::Client::new();

    let req = client
        .get("http://127.0.0.1:6149/")
        .header("x-port", "8444")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .header("client_cert", "1")
        .header("client_intermediate", "1");

    // pre check re-use
    assert_reuse(req).await;

    // different cert no re-use
    let res = client
        .get("http://127.0.0.1:6149/")
        .header("x-port", "8444")
        .header("sni", "openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1")
        .header("client_cert", "2")
        .header("client_intermediate", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert!(headers.get("x-conn-reuse").is_none());
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_diff_verify_no_reuse() {
    init();
    let client = reqwest::Client::new();

    let req = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "dog.openrusty.org")
        .header("verify", "1");

    // pre check re-use
    assert_reuse(req).await;

    // disable 'verify' no re-use
    let res = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "dog.openrusty.org")
        .header("verify", "0")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert!(headers.get("x-conn-reuse").is_none());
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_diff_verify_host_no_reuse() {
    init();
    let client = reqwest::Client::new();

    let req = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "cat.openrusty.org")
        .header("verify", "1")
        .header("verify_host", "1");

    // pre check re-use
    assert_reuse(req).await;

    // disable 'verify_host' no re-use
    let res = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "cat.openrusty.org")
        .header("verify", "1")
        .header("verify_host", "0")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert!(headers.get("x-conn-reuse").is_none());
}

#[cfg(feature = "any_tls")]
#[tokio::test]
async fn test_tls_diff_alt_cnt_no_reuse() {
    init();
    let client = reqwest::Client::new();

    let req = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "openrusty.org")
        .header("alt", "cat.com")
        .header("verify", "1")
        .header("verify_host", "1");

    // pre check re-use
    assert_reuse(req).await;

    // use alt-cn no reuse
    let res = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "openrusty.org")
        .header("alt", "dog.com")
        .header("verify", "1")
        .header("verify_host", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert!(headers.get("x-conn-reuse").is_none());
}

#[cfg(feature = "s2n")]
#[tokio::test]
async fn test_tls_psk() {
    use crate::utils::server_utils::TEST_PSK_IDENTITY;

    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "openrusty.org")
        .header("psk_identity", TEST_PSK_IDENTITY)
        .header("x-port", "6151")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[cfg(feature = "s2n")]
#[tokio::test]
async fn test_tls_psk_invalid() {
    init();
    let client = reqwest::Client::new();

    let res = client
        .get("http://127.0.0.1:6149/")
        .header("sni", "openrusty.org")
        .header("psk_identity", "BAD_IDENTITY")
        .header("x-port", "6151")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn test_error_before_headers_sent() {
    init();
    let url = "http://127.0.0.1:6146/sleep/test_error_before_headers_sent.txt";

    let tcp = TcpStream::connect("127.0.0.1:6146").await.unwrap();
    let (mut client, h2) = client::handshake(tcp).await.unwrap();

    tokio::spawn(async move {
        h2.await.unwrap();
    });

    let request = Request::builder()
        .uri(url)
        .header("x-set-sleep", "0")
        .header("x-abort", "true")
        .body(())
        .unwrap();

    let (response, mut _stream) = client.send_request(request, true).unwrap();

    let response = response.await.unwrap();
    let mut body = response.into_body();

    while let Some(chunk) = body.data().await {
        assert_eq!(chunk.unwrap(), Bytes::new());
    }
}

#[tokio::test]
async fn test_error_after_headers_sent_rst_received() {
    init();
    let url = "http://127.0.0.1:6146/connection_die/test_error_after_headers_sent_rst_received.txt";

    let tcp = TcpStream::connect("127.0.0.1:6146").await.unwrap();
    let (mut client, h2) = client::handshake(tcp).await.unwrap();

    tokio::spawn(async move {
        h2.await.unwrap();
    });

    let request = Request::builder().uri(url).body(()).unwrap();

    let (response, mut _stream) = client.send_request(request, true).unwrap();

    let response = response.await.unwrap();
    let mut body = response.into_body();

    let chunk = body.data().await.unwrap();
    assert_eq!(chunk.unwrap(), Bytes::from_static(b"AAAAA"));

    let err = body.data().await.unwrap().err().unwrap();
    assert_eq!(err.reason().unwrap(), h2::Reason::CANCEL);
}

#[tokio::test]
async fn test_103() {
    init();
    let res = reqwest::get("http://127.0.0.1:6147/103").await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let headers = res.headers();
    assert_eq!(headers[header::CONTENT_LENGTH], "6");
    let body = res.text().await.unwrap();
    assert_eq!(body, "123456");
}

#[tokio::test]
async fn test_103_die() {
    init();
    let res = reqwest::get("http://127.0.0.1:6147/103-die").await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

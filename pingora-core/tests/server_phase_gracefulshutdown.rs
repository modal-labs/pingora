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

// NOTE: This test sends a shutdown signal to itself,
// so it needs to be in an isolated test to prevent concurrency.

use async_trait::async_trait;
use http::{Response, StatusCode};
use pingora_core::apps::http_app::{HttpServer, ServeHttp};
use pingora_core::protocols::http::ServerSession;
use pingora_core::server::{configuration::ServerConf, ExecutionPhase, RunArgs, Server};
use pingora_core::services::listening::Service;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const LISTEN_ADDR: &str = "127.0.0.1:6193";

/// How long each request takes to be served. The request started before SIGTERM must still
/// be answered, so shutdown cannot complete before this.
const REQUEST_DURATION: Duration = Duration::from_secs(2);

/// The configured grace period. Far above what the in-flight request needs, so that a
/// regression back to sleeping for the whole period is unmistakable.
const GRACE_PERIOD: Duration = Duration::from_secs(30);

/// Budget for the whole shutdown: the in-flight request plus a generous allowance for a
/// loaded CI machine, well under [`GRACE_PERIOD`].
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);

struct SlowApp;

#[async_trait]
impl ServeHttp for SlowApp {
    async fn response(&self, _session: &mut ServerSession) -> Response<Vec<u8>> {
        tokio::time::sleep(REQUEST_DURATION).await;
        Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_LENGTH, 2)
            .body(b"ok".to_vec())
            .unwrap()
    }
}

// Sends one request and returns the raw response once the server has answered it.
fn slow_request() -> String {
    let mut stream = TcpStream::connect(LISTEN_ADDR).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

// Ensure that execution phases are reported correctly, that a request in flight when SIGTERM
// arrives is still served, and that the grace period ends as soon as that request is done
// rather than only once the configured period has elapsed.
#[test]
fn test_server_execution_phase_monitor_graceful_shutdown() {
    let conf = ServerConf {
        grace_period_seconds: Some(GRACE_PERIOD.as_secs()),
        graceful_shutdown_timeout_seconds: Some(1),
        ..Default::default()
    };
    let mut server = Server::new_with_opt_and_conf(None, conf);

    let mut phase = server.watch_execution_phase();

    let mut service = Service::new("slow".to_string(), HttpServer::new_app(SlowApp));
    service.add_tcp(LISTEN_ADDR);
    server.add_service(service);

    let join = std::thread::spawn(move || {
        server.bootstrap();
        server.run(RunArgs::default());
    });

    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::Bootstrap
    ));
    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::BootstrapComplete,
    ));
    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::Running,
    ));

    // Need to wait for startup, otherwise the signal handler is not
    // installed yet.
    //
    // TODO: signal handlers are installed after Running phase
    // message is sent, sleep for now to avoid test flake
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Start a request that will still be in flight when the shutdown signal arrives.
    let request = std::thread::spawn(slow_request);
    std::thread::sleep(Duration::from_millis(500));

    let shutdown_started = Instant::now();
    unsafe {
        libc::raise(libc::SIGTERM);
    }

    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::GracefulTerminate,
    ));

    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::ShutdownStarted,
    ));

    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::ShutdownGracePeriod,
    ));

    assert!(matches!(
        dbg!(phase.blocking_recv().unwrap()),
        ExecutionPhase::ShutdownRuntimes,
    ));
    let grace_period_took = shutdown_started.elapsed();

    join.join().unwrap();

    assert!(matches!(
        phase.blocking_recv().unwrap(),
        ExecutionPhase::Terminated,
    ));
    let shutdown_took = shutdown_started.elapsed();

    let response = request.join().unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200") && response.ends_with("ok"),
        "the request in flight during shutdown was not served: {response:?}"
    );

    assert!(
        grace_period_took >= REQUEST_DURATION - Duration::from_millis(500),
        "the grace period ended after {grace_period_took:?}, before the in-flight \
         {REQUEST_DURATION:?} request could have finished"
    );
    assert!(
        shutdown_took < SHUTDOWN_BUDGET,
        "shutdown took {shutdown_took:?} with a single {REQUEST_DURATION:?} request in flight, \
         over the {SHUTDOWN_BUDGET:?} budget, against a {GRACE_PERIOD:?} grace period: the \
         grace period is being served as a fixed delay rather than ending once drained"
    );
}

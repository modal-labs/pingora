# Graceful restart and shutdown

Graceful restart, upgrade, and shutdown mechanisms are very commonly used to avoid errors or downtime when releasing new versions of Pingora servers.

Pingora graceful upgrade mechanism guarantees the following:
* A request is guaranteed to be handled either by the old server instance or the new one. No request will see connection refused when trying to connect to the server endpoints.
* A request that can finish within the grace period is guaranteed not to be terminated.

## How to graceful upgrade
### Step 0
Configure the upgrade socket. The old and new server need to agree on the same path to this socket. See configuration manual for details.

### Step 1
Start the new instance with the `--upgrade` CLI option. The new instance will not try to listen to the service endpoint right away. It will try to acquire the listening socket from the old instance instead.

### Step 2
Send SIGQUIT signal to the old instance. The old instance will start to transfer the listening socket to the new instance.

Once step 2 is successful, the new instance will start to handle new incoming connections right away. Meanwhile, the old instance will enter its graceful shutdown mode. It waits a short period of time (to give the new instance time to initialize and prepare to handle traffic), after which it will not accept any new connections.

## Grace period

Both graceful shutdown (SIGTERM) and graceful upgrade (SIGQUIT) end with a grace period, configured by `grace_period_seconds` (default 5 minutes). Once the services are told to shut down, idle keepalive connections are closed and no new connections are accepted, but requests already in flight are allowed to finish. The server exits as soon as every service has drained, so a server with no in-flight requests exits right away; the grace period only bounds how long the server waits for slow requests. Requests still running when the grace period expires are interrupted during the final runtime shutdown, which is bounded by `graceful_shutdown_timeout_seconds`.

A custom service should therefore only return from `start_service()` once its in-flight work is done, otherwise the server may consider it drained early.

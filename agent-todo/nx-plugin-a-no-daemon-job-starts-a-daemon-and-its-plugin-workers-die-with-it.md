# A no-daemon job starts a daemon anyway, and its plugin workers die with it

`patches/nx@24.0.6.patch` holds two pieces that disagree inside one job:

- the client's `enabled()` branch on `NX_DAEMON_START=never && !recordedDaemonRuns()`;
- the plugin workers' terminal `handleConnectionError`
  (`Plugin worker lost its daemon connection and cannot reconnect`).

## What is measured

On 2026-11-25 a background-class job ran `cowshed exec <ws> -- bun nx run-many -t lint,typecheck,typecheck-tests -p …`
over four projects in a consumer workspace whose `nx.json` registers isolated plugins. Nx refused it after 19 s, before
running a single task:

```
NX   Failed to process project graph.
An error occurred while processing files for the ./tooling/nx-linked-package-sources.ts plugin (Defined at nx.json#plugins[1]).
  - Plugin worker lost its daemon connection and cannot reconnect: Daemon process terminated and closed the connection
```

The plugin `./tooling/nx-test-reads.ts` (nx.json#plugins[6]) failed the same way.

What the workspace's `.nx/workspace-data/d/daemon.log` records:

```
04:36:19.885Z [Client] NX_DAEMON_START=never and the recorded daemon is not running: running without the daemon
04:36:22.308Z [Messenger] Setting up socket listeners            (×3, the plugin workers)
04:36:22.333Z [Messenger] Sending message type: GET_CONTEXT_FILE_DATA
04:36:29.111Z [Messenger] Socket error: write EPIPE
04:36:29.115Z [Reconnect] Connection error detected: Daemon process terminated and closed the connection
```

The log holds no `Daemon Server` line, so the daemon itself said nothing about why it stopped.

The system log (`log show --predicate 'processID == 71199'`) shows a `node` daemon, pid 71199, inside the job's sandbox:

- it started at 04:36:19.99Z, 0.1 s after the client decided to run without a daemon;
- its record was written: `d/server-process.json` names pid 71199, and the socket is `/tmp/cs-54aeea8f3b85/nx/d.sock`;
- the sandbox refused it `file-link <build volume>/nx/workspace-data/d/server-process.json.71199`, and also
  `mach-lookup com.apple.logd`, `coreservicesd` and `diskarbitrationd`;
- it was gone 9 s later, with no exit reason in the system log.

## What is wrong

A client under `NX_DAEMON_START=never` that has decided to run without a daemon:

- still gets a daemon started inside its own job (the case the patch's comment says must not happen: "a daemon it
  started would serve every client of the checkout through the shared record while belonging to this client's job, and
  end with it");
- leaves its plugin workers dialing that daemon instead of computing in-process as the client does;
- turns the daemon's death into a terminal failure of the whole graph.

Not checked:

- who spawned pid 71199 (the CLI, a plugin worker, or the isolated-plugin host);
- whether the refused record link or the job's sandbox is what ended it.

## What to change

Once the client has decided to run without a daemon, that decision holds for every worker it hosts: they take the
in-process path too, and nothing in the job starts a daemon. Find which process starts pid 71199's daemon and name it
here first.

## Done when

A test with no recorded daemon runs a cowshed job (`NX_DAEMON_START=never`) on a workspace whose graph uses isolated
plugins. The test shows:

- the graph computes;
- no daemon process is started under the job;
- the plugin workers never dial a daemon socket.

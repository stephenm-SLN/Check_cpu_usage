# Check CPU Usage

Checks CPU busy/idle status across all `TA-*` and `AC-*` servers, grouping results by NUMA socket and isolated CPU set. Results are written to `Threaded_cpu_usage.csv` and served via `gui_cpu_usage.py`.

## How it works

1. `check_cpu_usage.py` queries a PostgreSQL database for the current list of running servers and their metadata (team, owner, instance type).
2. It passes the server list to a compiled Go binary (`ssh_cpu_check`) via stdin as JSON.
3. The Go binary SSHs into every server **concurrently** (up to 50 at once), samples `/proc/stat` twice with a 1-second gap, and returns CPU busy/idle percentages per NUMA socket as JSON on stdout.
4. Python merges the SSH results with the DB metadata and writes the CSV.

The Go binary replaces a Python `ThreadPoolExecutor(max_workers=7)` loop. With Go goroutines all servers are checked in parallel, reducing total runtime from 1–2 minutes to roughly the time of a single SSH check (~3–5 seconds).

## Requirements

| Tool | Purpose |
|---|---|
| Python 3.10+ | Runs `check_cpu_usage.py` and `gui_cpu_usage.py` |
| Go 1.21+ | Compiles `ssh_cpu_check` (only needed on the dev machine) |
| SSH key | Unencrypted private key in `~/.ssh/` (see [SSH auth](#ssh-authentication)) |

Python dependencies are managed via pixi (`pixi.toml`). Install with:
```bash
pixi install
```

## Repository layout

```
check_cpu_usage.py     # Main script — DB query, calls Go binary, writes CSV
gui_cpu_usage.py       # Flask web UI served by PM2
ssh_cpu_check.go       # Go source for the SSH concurrent checker
go.mod / go.sum        # Go module files
Makefile               # Build and deploy commands
```

## Building the Go binary

Go must be installed on the machine where you build. The binary is platform-specific and is **not** committed to git — build it from source.

### macOS (local dev / testing)
```bash
make mac
# or directly:
go build -o ssh_cpu_check ./ssh_cpu_check.go
```

### Linux x86_64 (cross-compile for the PM2 server)
```bash
make linux
# or directly:
GOOS=linux GOARCH=amd64 go build -o ssh_cpu_check_linux ./ssh_cpu_check.go
```

### Build both at once
```bash
make
```

## Deploying to the PM2 server

The PM2 server (`SGP-C-RUNNING-STATE`) runs the script under the `archy` user. After any change to `ssh_cpu_check.go`:

```bash
make deploy
```

This cross-compiles the Linux binary and copies it to the server in one step:
```
GOOS=linux GOARCH=amd64 go build -o ssh_cpu_check_linux ./ssh_cpu_check.go
scp ssh_cpu_check_linux archy@Run_state:/home/archy/local/python_server/gui_server/gui_cpu_usage/ssh_cpu_check
```

The binary is placed at:
```
/home/archy/local/python_server/gui_server/gui_cpu_usage/ssh_cpu_check
```

## SSH authentication

The Go binary discovers SSH keys automatically, trying in this order:

1. **SSH agent** — uses keys loaded in `$SSH_AUTH_SOCK` if set.
2. **`SSH_IDENTITY_FILE` env var** — explicit override, e.g. `SSH_IDENTITY_FILE=~/.ssh/my_key ./ssh_cpu_check`.
3. **`~/.ssh/id_*` and `~/.ssh/*.pem`** — all unencrypted private key files found under `~/.ssh/`.

The servers use `~/.ssh/id_ed25519`. No passphrase is required. Paramiko in `check_cpu_usage.py` uses the same key.

On the PM2 server (`SGP-C-RUNNING-STATE`) the key must exist at `/home/archy/.ssh/id_ed25519` (or another file matched by the patterns above).

## Running locally

```bash
# Run directly
python check_cpu_usage.py

# Run via pixi environment
pixi run python check_cpu_usage.py
```

Output is written to `Threaded_cpu_usage.csv` in the working directory.

## PM2 process management

The script is managed by PM2 on `SGP-C-RUNNING-STATE` using `ecosystem.config.cjs`. To restart after a deploy:

```bash
ssh Run_state "pm2 restart ecosystem.config.cjs"
```

## Adding a new server

New servers are picked up automatically — the server list is queried live from the PostgreSQL database (`steampipe_cache.aws_ec2_instance` and `steampipe_cache.alicloud_ecs_instance`) each time the script runs. No manual list update is needed.

The server name must:
- Start with `TA-` (AWS) or `AC-` (Alibaba Cloud)
- Have a `team` tag matching one of the known teams in `teamsList`
- Be reachable via SSH from `SGP-C-RUNNING-STATE` using the `archy` user

## Makefile reference

| Command | Description |
|---|---|
| `make` | Build both mac and linux binaries |
| `make mac` | Build macOS binary only |
| `make linux` | Cross-compile Linux x86_64 binary only |
| `make deploy` | Build linux binary and copy to PM2 server |
| `make clean` | Remove compiled binaries |

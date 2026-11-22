# Installing embedsync

Installers for every release are attached to the
[GitHub releases page](https://github.com/raunaksahahere/postgres-embedding-sync/releases).
Each one installs the `embedsync` command and a bundled embedding service, a
self-contained Python runtime with PyTorch and sentence-transformers. `embedsync`
starts that service when it needs it and stops it on exit. There is nothing else to
install apart from PostgreSQL.

## Pick a file

| system | file |
|---|---|
| Windows 10/11, x64 | `embedsync-<version>-<variant>-windows-x64-setup.exe` |
| macOS on Apple silicon | `embedsync-<version>-<variant>-macos-arm64.dmg` |
| Debian / Ubuntu with glibc 2.28+, x64 | `embedsync-<version>-<variant>-linux-amd64.deb` |
| same, ARM64 | `embedsync-<version>-<variant>-linux-arm64.deb` |

CI installs and tests them on Windows Server 2025, macOS 15, Debian 12, and Ubuntu
22.04 and 24.04. The Linux bundle needs nothing newer than glibc 2.28, so older
Debian/Ubuntu releases should work, but they are not tested.

The variant only decides which models are included:

* **minilm** (recommended): `sentence-transformers/all-MiniLM-L6-v2`. Works offline right away.
* **nomodel**: no model. `embedsync setup` asks for any Hugging Face model id
  (for example `BAAI/bge-base-en-v1.5`), pins it to its current commit and downloads it
  once into your user cache.
* **full**: all-MiniLM-L6-v2, BAAI/bge-small-en-v1.5 and paraphrase-MiniLM-L3-v2, the
  three models evaluated in the README. Use it on machines without internet access, or
  to compare models with `embedsync migrate`.

You can always add more models later with `embedsync model add <owner/name>`.
`SHA256SUMS` lists the checksum of every file: `sha256sum -c SHA256SUMS --ignore-missing`.

## Install

**Windows.** Run the `setup.exe`. By default it installs for your user only, into
`%LOCALAPPDATA%\Programs\embedsync`, without administrator rights, and adds that folder
to your PATH. Open a *new* terminal afterwards. The installer is not code-signed, so
SmartScreen may say "Windows protected your PC": click *More info → Run anyway*.
Silent install: `setup.exe /VERYSILENT /CURRENTUSER`.

**macOS.** Open the `.dmg` and double-click *Install embedsync.pkg*. It installs
`/usr/local/embedsync` and the `/usr/local/bin/embedsync` command. The package is not
signed with an Apple Developer ID, so macOS may block it the first time. Go to
*System Settings → Privacy & Security*, click *Open Anyway* and confirm. Alternatively,
in Terminal:

```sh
sudo installer -pkg "/Volumes/embedsync/Install embedsync.pkg" -target /
```

**Debian / Ubuntu.**

```sh
sudo apt install ./embedsync-0.6.1-minilm-linux-amd64.deb
```

It installs to `/opt/embedsync` with `/usr/bin/embedsync`. The three variants are
separate packages (`embedsync-minilm`, `embedsync-nomodel`, `embedsync-full`), and
installing one replaces another.

## First run

```sh
embedsync setup
```

The wizard:

1. Connects to PostgreSQL as an administrator. The admin password is used for this step
   only and is never stored. Setup checks the version (16–18), `wal_level` and whether
   pgvector is available. If PostgreSQL is not installed or not running, it prints the
   commands for your OS. If `wal_level` is not `logical`, it can set it, and then asks
   you to restart PostgreSQL and run setup again.
2. Lists the tables that have a supported primary key (single column: integer, bigint,
   smallint, text, varchar or uuid). You pick a table and the columns to embed. Only
   those columns are ever read or published. Setup configures one table (the pipeline
   `default`); for several tables or composite keys, edit the config as in
   [`examples/embedsync-multi.yaml`](../examples/embedsync-multi.yaml).
3. Asks for the embedding model: a bundled one, or any Hugging Face model.
4. Creates three least-privilege roles (`embedsync_replicator`, `embedsync_sink`,
   `embedsync_reader`) with random passwords, the publication, and the `embedsync`
   schemas. It writes:
   * the config: `~/.config/embedsync/embedsync.yaml` (Linux),
     `~/Library/Application Support/embedsync/embedsync.yaml` (macOS),
     `%AppData%\embedsync\embedsync.yaml` (Windows)
   * the credentials: `embedsync.env` next to the config, readable only by you

Then:

```sh
embedsync run                       # keep it running; Ctrl-C stops it cleanly
embedsync search "your question"    # from another terminal
embedsync status                    # checkpoint, replication slot, generations
embedsync doctor                    # checks everything, with fixes for what fails
```

To run without prompts (scripts, CI):
`embedsync setup --yes --admin-url postgres://postgres:secret@localhost:5432/app --table public.docs --columns title,body --model minilm`.

### PostgreSQL

If you have no PostgreSQL yet, setup suggests the native packages for your OS
(`apt`, `dnf`, Homebrew, the EDB installer on Windows). If Docker is installed, this one
command starts a ready-made server:

```sh
docker run -d --name embedsync-postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres \
    -v embedsync-pgdata:/var/lib/postgresql pgvector/pgvector:pg17 -c wal_level=logical
```

`embedsync run` holds a logical replication slot. While embedsync is stopped,
PostgreSQL keeps the WAL it has not yet confirmed. Keep `run` running, or see
[operations](operations.md) for limits (`max_slot_wal_keep_size`) and alerts.

## Models and disk

Models that are not bundled are downloaded once into your user cache
(`~/.cache/embedsync/hf`, `~/Library/Caches/embedsync/hf`,
`%LocalAppData%\embedsync\hf`). The embedder log is `embedder.log` in the same
`embedsync` cache folder. Private or gated Hugging Face models need `HF_TOKEN` set.

## Upgrading

A new release can change embedsync's tables in your database. To upgrade:

1. Stop `embedsync run` (Ctrl-C, or stop the service that runs it).
2. Install the new package the same way as above. Your config, credentials and cache
   stay in place.
3. Upgrade the database with an administrator connection to the database you chose in
   setup. Like in setup, the password is used for this step only and is never stored:

   ```sh
   embedsync init --admin-url 'postgres://postgres:...@host/db'
   ```

   It prints what it applied, or `up to date`. Instead of the flag you can set
   `EMBEDSYNC_ADMIN_URL`. If `EMBEDSYNC_SOURCE_ADMIN_DSN` and `EMBEDSYNC_SINK_ADMIN_DSN`
   are set (as in the Docker Compose stack), plain `embedsync init` uses those.
4. Start `embedsync run` again.

Nothing is re-embedded: the checkpoint, generations and DLQ carry over. Until `init` has
run, every command stops and asks you to "run `embedsync init`". Downgrading after
`init` is not supported: the older version refuses to start against the upgraded
database. Details: [operations](operations.md#upgrades).

## Uninstall

* **Windows:** *Settings → Apps → embedsync → Uninstall*. This also removes the PATH entry.
* **macOS:** `sudo /usr/local/embedsync/uninstall.sh`
* **Debian/Ubuntu:** `sudo apt remove embedsync-minilm` (or `-nomodel` / `-full`)

Your config and cache stay in place. To remove embedsync from the database as well,
stop `embedsync run` and run, as an administrator:

```sql
SELECT pg_drop_replication_slot('embedsync_slot') FROM pg_replication_slots WHERE slot_name = 'embedsync_slot';
DROP PUBLICATION embedsync_pub;
DROP SCHEMA embedsync CASCADE;
DROP SCHEMA embedsync_control CASCADE;
DROP ROLE embedsync_replicator, embedsync_sink, embedsync_reader;
```

## CLI-only archives

`embedsync_<version>_<os>_<arch>.tar.gz` and `.zip` contain only the `embedsync` binary
(Linux, macOS Intel and Apple silicon, Windows). Use them when you run the embedder
yourself, for example with the Docker Compose stack, and set `embedder.url` in the config.

# Wayang Hackathon Development Environment — Community over Code 2026, Glasgow

This folder is a self-contained Docker development environment for the Wayang
hackathon. It gets a contributor from a fresh checkout to a working build of
Wayang with the **Java**, **Spark**, and **PostgreSQL** platforms all
runnable inside the container.

This is a *developer* environment, not just a runtime: the full source tree
is bind-mounted into the container, so you edit code on your host machine
with your usual editor/IDE, and build/run it inside the container. Nothing
beyond Git and Docker Desktop is required on the host.

**Timing**: building the Wayang distribution from source on a cold Maven
cache takes 30-50+ minutes depending on network speed and machine specs —
that's just how long compiling Wayang and its dependency tree (including
Spark) genuinely takes the first time. That is NOT what hackathon
participants are expected to do. Instead, a multi-architecture image
supporting both linux/amd64 and linux/arm64 is built and pushed to
GitHub Container Registry ahead of the event (see
`.github/workflows/hackathon-image.yml`), with its Maven cache
already warmed at build time. Participants just pull the image:

```bash
docker compose pull
docker compose up -d
```

This is the intended 10-15 minute setup path — it's a Docker image pull
plus container startup, not a build. `docker compose up -d --build`
(building from source locally) is still available and is what this README
documents step by step below, for contributors actively working on the
Dockerfile/build itself — participants on hackathon day shouldn't need it.

## What's here

```
hackathon-CoC2026/
├── Dockerfile              # JDK 17 + git + Maven Wrapper + psql client
├── compose.yaml            # the "wayang" dev container + a real "postgres" service
├── verify.sh               # one-command build + cross-platform verification
├── docker/
│   ├── entrypoint.sh       # normalizes mvnw line endings on every start
│   └── postgres/
│       └── init.sql        # seeds a small table used by the Postgres smoke test
├── examples/
│   └── PostgresSmokeTest.java   # a Wayang plan spanning Java, Spark, and Postgres
└── README.md                # this file
```

## Prerequisites

Install on your host machine:

- [Git](https://git-scm.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes
  Docker Compose)

Verify:

```bash
docker --version
docker compose version
```

## 1. Get the source

```bash
git clone https://github.com/apache/wayang.git
cd wayang
```

(If you're working from a fork/branch, clone that instead.)

## 2. Start the environment

From this folder:

```bash
cd hackathon-CoC2026
docker compose pull   # gets the pre-built, cache-warmed image (fast)
docker compose up -d
```

If you're actively changing the Dockerfile or want a from-source build
instead, use `docker compose up -d --build` (see Timing above — this is
slow on a cold cache).

Either way, this starts two services:

- `wayang` — the container you'll work in (Java 17 JDK, Maven Wrapper, git)
- `postgres` — a real PostgreSQL 16 instance, seeded with a small sample
  table (`word_counts_seed`)

Check both containers are up:

```bash
docker compose ps
```

`postgres` should show as `healthy` before moving on (it has a healthcheck
that waits for it to accept connections).

## 3. Run the verification script

```bash
docker compose exec wayang bash hackathon-CoC2026/verify.sh
```
**Note:** `verify.sh` performs a full Wayang distribution build to validate
the development environment end-to-end. This build can take 30-50+ minutes
on a cold Maven cache. It is not required to start the pre-built hackathon
environment and is not part of the 10-15 minute participant setup target.

This single command: builds `wayang-assembly`, extracts the distribution,
runs the bundled WordCount app on the Java platform, resolves the full
Maven runtime classpath, and compiles + runs `PostgresSmokeTest.java` — a
Wayang plan that reads from Postgres, filters on Java, and maps on Spark.

The distribution's extracted layout is `bin/`, `conf/`, `jars/`, and
`libs/`. Both `jars/*` and `libs/*` are needed on the classpath to compile
and run against Wayang's own modules — `jars/` alone is not enough. Neither
directory contains Spark's own transitive runtime dependencies (e.g.
`spark-sql`), though — those are only resolved through Maven's local
repository, via `./mvnw -pl wayang-assembly dependency:build-classpath`,
which is why `verify.sh` builds the classpath from both sources combined.

You do not need to hand-edit `bin/wayang-submit` or add `--add-opens`/
`--add-exports` flags yourself — the script uses `wayang-submit` as-is for
the Java check, and pulls the same flags straight out of that script for its
own direct `java` invocation of the smoke test.

Confirmed working output looks like this:

```
Wayang plan executed across Postgres, Java, and Spark. Results:
WAYANG -> 42
SPARK -> 17
POSTGRES -> 9
Total rows: 3
```

If it fails, see Troubleshooting below.

You can also poke at the database directly from inside the container:

```bash
psql -h postgres -U wayang -d wayang_hackathon -c "select * from word_counts_seed;"
# password: wayang
```

## Troubleshooting

- **`mvnw: command not found` / permission denied** — the entrypoint fixes
  line endings and permissions on `mvnw` on every container start, but if
  you edited/replaced it from the host afterward, re-run
  `sed -i 's/\r$//' mvnw && chmod +x mvnw` inside the container.
- **Postgres connection refused** — wait for `docker compose ps` to show
  `postgres` as `healthy`; the `wayang` service is configured to start after
  that, but a manual `docker compose exec` shell you opened earlier may have
  started before it. Just retry.
- **Slow first build** — this is the Maven dependency download, not a bug
  (see Timing above). It's cached in the `wayang-maven-cache` volume;
  `docker compose down` (without `-v`) keeps it, `docker compose down -v`
  deletes it.
- **Out of memory during the build** — increase Docker Desktop's memory
  limit (Settings → Resources).
- **`ClassNotFoundException`/`NoClassDefFoundError` for a Spark class** —
  confirm `/tmp/wayang-classpath.txt` from `verify.sh` actually contains
  `spark-sql` and `spark-sql-api` jars; if not, re-run the `dependency:
  build-classpath` step and check for Maven resolution errors above it.
- **`no matching manifest for linux/arm64/v8`** — the published image does
  not contain an ARM64 variant. Re-publish the image using the
  multi-architecture workflow above and pull it again with
  `docker compose pull`.
- **`tar: ... Function not implemented` on Apple Silicon** — this can occur
  when an AMD64-only image is forced to run on an ARM64 host through
  emulation. Do not force `linux/amd64`; use the multi-architecture image
  so Docker pulls the native ARM64 variant.

## Resetting everything

```bash
docker compose down -v
```

This removes both containers and both named volumes (Maven cache and
Postgres data), so the next `docker compose up -d --build` starts completely
fresh.

## For maintainers: publishing the image

`.github/workflows/hackathon-image.yml` builds `hackathon-CoC2026/Dockerfile`
natively for both `linux/amd64` and `linux/arm64`, publishes the
architecture-specific images, and then creates a single multi-architecture
manifest at `ghcr.io/apache/wayang-hackathon:latest`.

The workflow is manual (`workflow_dispatch`), not automatic on every push —
trigger it from the Actions tab whenever the environment needs to pick up
new commits, and definitely once shortly before the event so the pushed
image reflects the current environment.

This needs GHCR (GitHub Container Registry) enabled for this repository
under the ASF GitHub org. If the workflow's push step fails with a
permissions error, that's an ASF INFRA setting to request, not a bug in the
workflow.

## Later platforms

This environment currently exercises Java, Spark, and PostgreSQL, per the
initial hackathon scope. Adding another platform (e.g. SQLite, GraphChi)
only requires seeding/service changes here in `hackathon-CoC2026/` — the
`Dockerfile` and `compose.yaml` at the repository root are untouched.

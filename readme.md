# Migrate

Migrates keys matching a pattern from one Redis to another. This procedure involves two scripts:
1. Download keys and its values matching a pattern from **origin** Redis.
2. Upload keys to **destination** Redis

Be aware that this procedure will remove all expires.

### Installation

```bash
npm install
chmod +x ./download.js
chmod +x ./upload.js
```

### download.js usage

```bash
./download.js [-h] [-p] [-a] [-t] [--pattern] [--filename]
```
### download.js options [default]
* `-h` **origin** Redis hostname [127.0.0.1]
* `-p` **origin** Redis port [6379]
* `-a` **origin** Redis auth ['']
* `-t` **origin** Redis TLS [false]
* `--pattern` Glob pattern [\*]
* `--filename` Filename of the json [dump.json]

### upload.js usage

```bash
./upload.js [-h] [-p] [-a] [-t] [--filename]
```
### upload.js options [default]
* `-h` **destination** Redis hostname [127.0.0.1]
* `-p` **destination** Redis port [6379]
* `-a` **destination** Redis auth ['']
* `-t` **destination** Redis TLS [false]
* `--filename` Filename of the json [dump.json]

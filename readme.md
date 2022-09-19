
### Installation

```bash
npm install
chmod +x downloadFromRedis.js
```

### downloadFromRedis.js usage

```bash
./downloadFromRedis.js [-h] [-p] [-a] [-t] [--pattern] [--filename]
```
### downloadFromRedis.js options [default]
* `-h` **origin** Redis hostname [127.0.0.1]
* `-p` **origin** Redis port [6379]
* `-a` **origin** Redis auth ['']
* `-t` **origin** Redis TLS [false]
* `--pattern` Glob pattern [\*]
* `--filename` Filename of the json [dump.json]

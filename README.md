# Network File System Coursework Prototype

A C client/server project with a naming server that registers storage paths and directs clients to storage servers. This public repository preserves the three-file implementation in its original layout.

## Components

| File | Responsibility |
|---|---|
| [naming.c](naming.c) | Naming-server listener, storage registration, path lookup, and request coordination |
| [store.c](store.c) | Storage registration and file-operation handling |
| [client.c](client.c) | Interactive client and connections to the naming/storage servers |

```text
Client ── path lookup / coordination ──> Naming server
Client ── file-operation connection ───> Storage server
Storage server ── path registration ───> Naming server
```

## Build

The sources target Linux/POSIX interfaces and use GCC and pthreads. There is no checked-in build script; the corresponding compile commands are:

```bash
gcc naming.c -o naming -pthread
gcc store.c -o store -pthread
gcc client.c -o client
```

These instructions describe the checked-in source layout; this historical snapshot has not been rebuilt or integration-tested as part of its documentation cleanup.

## Start the three components

1. Start the naming server in one terminal:

   ```bash
   ./naming
   ```

   It listens on TCP port **5050** and prints the system IP.

2. In a second terminal, start a storage server with that IP:

   ```bash
   ./store <naming-server-ip>
   ```

   Enter existing file/directory paths when prompted, then enter `done`. The storage server advertises its system IP and dynamically assigned client port.

3. In a third terminal, start the client:

   ```bash
   ./client <naming-server-ip>
   ```

   Enter `paths` to inspect registered paths, `stored <path>` to query a location, or `quit` to end the client session. Use the exact paths registered by the storage server.

## Commands represented in the source

The client handles `create` and `create_dir` through follow-up prompts, as well as `delete <path>`, `read <path>`, `write <path>`, `info <path>`, and `stream <path>`. These are implemented command paths rather than a claim that every operation has passed end-to-end validation.

## Implementation limits

- The naming-server port is fixed at 5050.
- Registration and path lookup are held in memory.
- The source defines limits of 10 registered storage servers and 10 clients.
- System-IP discovery uses the Linux command `hostname -I`.
- Compiler compatibility, concurrent request handling, and failure/reconnection behavior need separate validation before extending this prototype.

# Torc‑Lite

Torc‑Lite is a lightweight task-based runtime for MPI applications that provides a master/worker execution model and local multithreading (pthreads). It exposes a small C API to create tasks, synchronize, and transfer data across MPI processes while the runtime handles scheduling, communication and (optional) work-stealing.

Key goals:
- Provide a compact, portable runtime for task-parallel MPI programs.
- Support multiple POSIX synchronization strategies (mutexes or spinlocks).
- Offer a simple API that integrates with existing MPI applications and Fortran code.

For details and historical notes see the original INSTALL and COPYING files included in the repository.

---

## Features

- Task creation and scheduling via `torc_task` / `torc_create` API.
- Server thread to handle MPI communication and optional non-blocking server.
- Configurable synchronization backends: `mutex`, `mutex_try`, `spin`, `spin_try`.
- Small demo programs illustrating common patterns (async tasks, broadcast, master/slave, benchmarks).
- Optional null-MPI stub (nullmpi/) for builds on systems without MPI.

## Stack
- Language: C (with optional Fortran wrappers)
- Concurrency: POSIX threads (pthreads)
- Message passing: MPI (MPICH / Open MPI expected)
- Build: GNU Autotools (autoconf, automake)

## Quick repository map
```
configure*                 Autotools-generated configure script
configure.ac               Autoconf configuration
Makefile.in, Makefile.am   Top-level build files
src/                       Core runtime sources (torc.c, torc_runtime.c, torc_queue.c, torc_comm.c, torc_server.c, torc_thread.c)
include/                   Public and internal headers (include/torc.h, torc_config.h, torc_internal.h, ...)
demo/                      Example programs (async.c, broadcast.c, fibo.c, masterslave.c, mbench1.c, pipe.c, struct.c, zerolength.c)
scripts/                   configure-generated helper scripts (torc_cflags, torc_libs)
nullmpi/                   Lightweight MPI stub for non-MPI builds
INSTALL, README, COPYING   Documentation, license and install notes
```

How it fits together
- Applications call `torc_init(...)`, register tasks, and create them with `torc_task(...)` / `torc_create(...)`.
- The runtime uses pthreads for local concurrency and a dedicated server thread to mediate MPI messages.
- Task queues and the scheduler (src/torc_queue.c and src/torc_runtime.c) decide task placement; work-stealing can be enabled.

## Build & install (short path)
These commands assume you have a thread-safe MPI (mpicc) available and Autotools present.

```bash
# Bootstrap (if you are developing or the autotools files are not already generated):
# autoreconf -i    # only if modifying configure.ac or missing generated files

# Configure and build (example)
./configure CC=mpicc F77=mpif90
make

# Build demos
cd demo
make

# Optional install
# ./configure --prefix=/opt/torc CC=mpicc F77=mpif90
# make && make install
```

Notes on configure options (see configure.ac for full details):
- --with-sync=mutex|mutex_try|spin|spin_try — choose POSIX synchronization mechanism
- --with-maxnodes=NUM — set maximum MPI processes (default 1024)
- --with-maxvps=NUM — set maximum virtual processors (default 64)
- --with-mpi/--with-mpiincdir/--with-mpilibdir — point to a non-standard MPI installation
- --enable-debug — enable debug build flags

The configure step generates small helper scripts in `scripts/`:
- `scripts/torc_cflags` prints the correct include flags for user programs
- `scripts/torc_libs` prints the correct linker flags (e.g. `-L${prefix}/lib -ltorc -lpthread`)

## Running a demo
Use your MPI launcher (mpirun, mpiexec) with the desired process count. Example:

```bash
mpirun -np 4 ./demo/masterslave
```

If you built and installed the library to a custom prefix, use the generated scripts when compiling an application:

```bash
mpicc `./scripts/torc_cflags` -o myprog myprog.c `./scripts/torc_libs`
```

## API quick reference
See `include/torc.h` for full declarations. Key functions:
- `int torc_init(int argc, char *argv[], int ms);` — initialize runtime
- `void torc_task(int queue, void (*f)(), int narg, ...);` — create a task
- `void torc_task_detached(...)`, `torc_task_ex(...)`, `torc_task_direct(...)` — variants
- `void torc_waitall();` — wait for outstanding tasks
- `void torc_finalize();` — shut down runtime
- `void torc_broadcast(void *a, long count, MPI_Datatype dtype);` — helper for broadcasting data

Public API is declared in `include/torc.h` and additional internal helpers live in the `include/` headers.

## Development notes
- The runtime assumes a thread-safe MPI. If your MPI implementation is not thread-safe, consider using the `nullmpi/` stub for local testing or configure carefully.
- Synchronization backend and cache-line size are determined at configure time and written to `include/ps_config.h`.
- The main scheduler and queue logic are in `src/torc_runtime.c` and `src/torc_queue.c`.

## License
Torc‑Lite is distributed under the GNU General Public License. See `COPYING` and `LICENSE` for the exact terms.

## Authors / Contact
See the `AUTHORS` file for contributors and historical credits. The repository originally created by Panagiotis Hadjidoukas and collaborators.

---

If you'd like, I can:
- Replace the existing top-level `README` file with this `README.md` (or update that file instead),
- Add a short Quick Start example that compiles and runs one of the demo programs verbatim,
- Or open a small PR that adds usage examples and badges.

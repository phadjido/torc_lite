# Torc‑Lite

Torc‑Lite is a lightweight task-based runtime for MPI applications that provides a master/worker execution model and local multithreading (pthreads). It exposes a small C API to create tasks, synchronize, and exchange data across MPI nodes.

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

Functions that may execute on another MPI process must be registered with
`torc_register_task()` on every MPI process. Tasks must be registered in the
same order on every process because the runtime transfers registered-function
IDs between processes. Unregistered functions may only be used for tasks that
are guaranteed to execute locally.


## Development notes
- For standards-compliant execution, the runtime requires `MPI_THREAD_SERIALIZED` or higher. See “Portability and MPI requirements” below for the tested Linux compatibility behavior.
- Synchronization backend and cache-line size are determined at configure time and written to `include/ps_config.h`.
- The main scheduler and queue logic are in `src/torc_runtime.c` and `src/torc_queue.c`.

### Portability and MPI requirements

#### Task-descriptor mutexes

Each task descriptor contains a POSIX mutex or spinlock used for local
dependency synchronization. Although complete task descriptors are transferred
between MPI processes, a received lock representation is process-local state
and must not be used directly. Torc-Lite discards and reinitializes the lock
locally before a received task is queued or executed.

Reinitializing task locks has been tested successfully on Linux. The current
implementation primarily targets Linux POSIX environments; applications
requiring portability to other operating systems or pthread implementations
should verify this behavior.

#### MPI thread support

For standards-compliant execution, Torc-Lite requires
`MPI_THREAD_SERIALIZED` or `MPI_THREAD_MULTIPLE`. When
`MPI_THREAD_SERIALIZED` is provided, Torc-Lite serializes MPI calls with an
internal communication lock.

Torc-Lite has also been observed to work on Linux with Open MPI and MPICH when
the reported level is `MPI_THREAD_SINGLE`. In that configuration, MPI calls
remain serialized, but calls may originate from different Torc-Lite threads.
This behavior is implementation-specific and is not guaranteed by the MPI
standard.


## License
Torc‑Lite is distributed under the GNU General Public License. See `COPYING` and `LICENSE` for the exact terms.

## Authors / Contact
See the `AUTHORS` file for contributors and historical credits. The repository originally created by Panagiotis Hadjidoukas and collaborators.

---

## Example: Simple masterslave program

The following minimal example demonstrates a typical Torc‑Lite program that registers a task, initializes the runtime, creates a set of tasks and waits for their completion. It is a compact version of `demo/masterslave.c` included in this repository.

```c
/* masterslave_example.c
 * Minimal Torc-Lite example: compute square roots in parallel
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <torc.h>

void slave(double *pin, double *out)
{
    double in = *pin;
    /* simulate work */
    sleep(1);
    *out = sqrt(in);
    printf("slave: in=%f out=%f\n", in, *out);
}

int main(int argc, char *argv[])
{
    int n = 4; /* number of tasks */
    double *inputs = malloc(n * sizeof(double));
    double *results = malloc(n * sizeof(double));

    for (int i = 0; i < n; i++) {
        inputs[i] = (double)(i + 1);
        results[i] = 0.0;
    }

    /* Tell the runtime which functions may be invoked as tasks */
    torc_register_task(slave);

    /* Initialize the runtime (MODE_MS selects master/slave mode used in demos) */
    torc_init(argc, argv, MODE_MS);

    /* Optionally enable work stealing for better load balance on many threads */
    torc_enable_stealing();

    /* Create tasks (queue = -1 lets the runtime choose local queue) */
    for (int i = 0; i < n; i++) {
        torc_create(-1, slave, 2,
                    1, MPI_DOUBLE, CALL_BY_COP,
                    1, MPI_DOUBLE, CALL_BY_RES,
                    &inputs[i], &results[i]);
    }

    /* Wait for all tasks to complete */
    torc_waitall();

    /* Print results */
    for (int i = 0; i < n; i++) {
        printf("result[%d] = sqrt(%g) = %g\n", i, inputs[i], results[i]);
    }

    torc_finalize();
    free(inputs);
    free(results);

    return 0;
}
```

Build and run the example (from the repository root):

```bash
# build the library and the demos first (see Build & install above)
# compile the example using the helper scripts generated by configure
mpicc `./scripts/torc_cflags` -o masterslave_example demo/masterslave_example.c `./scripts/torc_libs`

# run with 4 MPI processes
mpirun -np 4 ./masterslave_example
```

Notes:
- The demos in `demo/` provide several more complete examples (async, broadcast, fibo, pipe, etc.).
- See `include/torc.h` for detailed API usage and the demos for realistic patterns and argument passing modes (CALL_BY_COP, CALL_BY_RES, CALL_BY_REF).

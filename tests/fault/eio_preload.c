/*
 * eio_preload.c — §12 fsync-failure injection shim (M8 / §14.8 H3).
 *
 * LD_PRELOAD this library to make the WAL's commit data sync (libc `fdatasync`)
 * deterministically return EIO, so a test can exercise the §12 *poison state
 * machine* on real, durable storage:
 *
 *     fdatasync EIO  ->  do not advance durable_lsn past the failed segment
 *                    ->  surface FsyncFailed  ->  poison the handle
 *                    ->  subsequent append/commit return Poisoned
 *
 * SCOPE — read this before trusting a green result. This is an APPLICATION-LOGIC
 * test of how the WAL *reacts* to a flush failure. It is NOT a durability test
 * and NOT a substitute for dm-flakey / power-pull:
 *   - It returns a *fake* EIO; it drops no data and models no power loss.
 *   - A real Linux fsync failure (the fsyncgate scenario §12 is built around) can
 *     mark dirty pages clean and lose them *before* returning the error. This shim
 *     leaves the data in cache. So it proves "we poison on EIO" — NOT "we correctly
 *     treat the data as already-gone". That second property is validated only by
 *     dm-flakey / real hardware (§14.8 H3 gold path, OPEN-pending-owner-hardware).
 *   - It only intercepts the libc `fdatasync` symbol. The WAL's *directory* fsync
 *     uses rustix raw `linux_raw` syscalls and is NOT interceptable here — so this
 *     shim covers the data-sync poison path, not the dir-fsync poison path.
 *
 * Control via env (see tests/fsync_fault_gate.rs and scripts/m8/fsync-fault.sh):
 *
 *   WAL_FAULT_ARM    Path to an "arm" file. Its integer contents K mean: let the
 *                    next K `fdatasync` calls pass, then fail the (K+1)th with EIO.
 *                    One-shot: the file is removed when the injection fires.
 *                      K=0  -> fail the next fdatasync   (single-segment case)
 *                      K=1  -> pass seg1's sync, fail seg2's (split-batch case)
 *                    The file is created by the test immediately before the commit
 *                    it wants to fail, so open()-time segment-create syncs (which
 *                    happen before arming) always pass.
 *
 *   WAL_FAULT_COUNT  Path to a counter file. The shim writes the cumulative number
 *                    of EIOs it has injected. The test asserts this is >= the
 *                    expected number — the ANTI-VACUOUS guard: a zero count means
 *                    interception silently failed (e.g. the sync did not route
 *                    through this libc symbol) and the test verified NOTHING, which
 *                    must be a loud failure, never a pass.
 *
 * Never build or ship this into a real binary; it exists only to be LD_PRELOADed
 * by the M8 H3 gate.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int (*real_fdatasync)(int) = NULL;

static void ensure_real(void) {
    if (!real_fdatasync) {
        real_fdatasync = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
    }
}

/* Read K from the arm file. Returns -1 if not armed. */
static long read_arm(const char *arm) {
    FILE *f = fopen(arm, "r");
    if (!f) return -1;
    long k = 0;
    if (fscanf(f, "%ld", &k) != 1) k = 0;
    fclose(f);
    return k;
}

static void bump_count(void) {
    const char *cnt = getenv("WAL_FAULT_COUNT");
    if (!cnt) return;
    long c = 0;
    FILE *r = fopen(cnt, "r");
    if (r) {
        if (fscanf(r, "%ld", &c) != 1) c = 0;
        fclose(r);
    }
    FILE *w = fopen(cnt, "w");
    if (w) {
        fprintf(w, "%ld\n", c + 1);
        fclose(w);
    }
}

/* Returns 1 if this fdatasync should be failed with EIO, else 0. */
static int should_fail(void) {
    const char *arm = getenv("WAL_FAULT_ARM");
    if (!arm) return 0;
    long k = read_arm(arm);
    if (k < 0) return 0; /* not armed */
    if (k > 0) {
        /* Let this one pass; decrement the skip count. */
        FILE *w = fopen(arm, "w");
        if (w) {
            fprintf(w, "%ld\n", k - 1);
            fclose(w);
        }
        return 0;
    }
    /* k == 0: fire once, then disarm. */
    remove(arm);
    bump_count();
    return 1;
}

int fdatasync(int fd) {
    ensure_real();
    if (should_fail()) {
        errno = EIO;
        return -1;
    }
    return real_fdatasync(fd);
}

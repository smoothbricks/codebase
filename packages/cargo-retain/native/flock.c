/*
 * The one flock(2) the compiled CLI makes (src/locks-ffi.ts calls it through ffi.json). scriptc's FFI hands back only a
 * function's return value, never errno, and errno means something only until the next library call, so the failed call
 * is classified here and its cause leaves as text.
 */
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>

/*
 * 0: the exclusive lock is taken. 1: another process holds it (EWOULDBLOCK, the only answer that means busy). -1:
 * flock(2) failed for another reason, such as a file or file system that has no flock(2); its strerror text,
 * NUL-terminated, is in `reason`.
 */
int32_t cargo_retain_try_lock(int32_t fd, uint8_t *reason, size_t capacity) {
  if (flock(fd, LOCK_EX | LOCK_NB) == 0) return 0;
  const int cause = errno;
  if (cause == EWOULDBLOCK) return 1;
  if (reason != NULL && capacity > 0) snprintf((char *)reason, capacity, "%s", strerror(cause));
  return -1;
}

/*
 * The compiled CLI's open(2) and flock(2) (src/locks-ffi.ts calls them through ffi.json). The runtime's open error
 * erases errno, and errno means something only until the next library call, so failures are captured here.
 */
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>

static void failure_reason(int cause, uint8_t *reason, size_t capacity) {
  if (reason != NULL && capacity > 0)
    snprintf((char *)reason, capacity, "errno %d: %s", cause, strerror(cause));
}

/* The caller supplies UTF-8 bytes including a trailing NUL. The descriptor belongs to the caller on success. */
int32_t cargo_retain_open_lock(const uint8_t *path, size_t path_length, int32_t create,
                               uint8_t *reason, size_t capacity) {
  (void)path_length;
  const int fd = open((const char *)path, O_RDONLY | O_NONBLOCK | (create ? O_CREAT : 0), 0644);
  if (fd < 0) {
    const int cause = errno;
    failure_reason(cause, reason, capacity);
  }
  return fd;
}

/*
 * 0: the exclusive lock is taken. 1: another process holds it (EWOULDBLOCK, the only answer that means busy). -1:
 * flock(2) failed for another reason, such as a file or file system that has no flock(2); its errno and OS cause,
 * NUL-terminated, are in `reason`.
 */
int32_t cargo_retain_try_lock(int32_t fd, uint8_t *reason, size_t capacity) {
  if (flock(fd, LOCK_EX | LOCK_NB) == 0) return 0;
  const int cause = errno;
  if (cause == EWOULDBLOCK) return 1;
  failure_reason(cause, reason, capacity);
  return -1;
}

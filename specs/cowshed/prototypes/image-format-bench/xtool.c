// Measurement tool for the image-format prototype: extent counting and the plain rewrite mirror cowshed's extents.rs.
// Build: cc -O2 -o "$PROTO_ROOT/xtool" xtool.c
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;
}

static void die(const char *what) {
  fprintf(stderr, "%s: %s\n", what, strerror(errno));
  exit(1);
}

static int next_region(int fd, off_t off, off_t len, off_t *s, off_t *e) {
  if (off >= len) return 0;
  off_t start = lseek(fd, off, SEEK_DATA);
  if (start < 0) {
    if (errno == ENXIO) return 0;
    die("SEEK_DATA");
  }
  off_t end = lseek(fd, start, SEEK_HOLE);
  if (end < 0) {
    if (errno == ENXIO) end = len;
    else die("SEEK_HOLE");
  }
  *s = start;
  *e = end < len ? end : len;
  return 1;
}

static off_t run_len(int fd, off_t off, off_t len) {
  struct log2phys q = {0, len, off};
  if (fcntl(fd, F_LOG2PHYS_EXT, &q) == -1) die("F_LOG2PHYS_EXT");
  if (q.l2p_contigbytes <= 0) {
    fprintf(stderr, "zero run at %lld\n", (long long)off);
    exit(1);
  }
  return q.l2p_contigbytes < len ? q.l2p_contigbytes : len;
}

static int cmd_extents(const char *path) {
  int fd = open(path, O_RDONLY);
  if (fd < 0) die("open");
  struct stat st;
  fstat(fd, &st);
  double t0 = now_ms();
  uint64_t extents = 0, regions = 0, data = 0;
  off_t off = 0, s, e;
  while (next_region(fd, off, st.st_size, &s, &e)) {
    regions++;
    data += e - s;
    for (off_t p = s; p < e;) {
      p += run_len(fd, p, e - p);
      extents++;
    }
    off = e;
  }
  printf("extents=%llu regions=%llu data_bytes=%llu alloc_bytes=%llu length=%lld count_ms=%.1f\n",
         (unsigned long long)extents, (unsigned long long)regions, (unsigned long long)data,
         (unsigned long long)st.st_blocks * 512, (long long)st.st_size, now_ms() - t0);
  return 0;
}

// Rewrite one existing byte in place: the first write into a clone copies its extent map.
static int cmd_firstwrite(const char *path) {
  int fd = open(path, O_RDWR);
  if (fd < 0) die("open");
  unsigned char b;
  if (pread(fd, &b, 1, 0) != 1) die("pread");
  double t0 = now_ms();
  if (pwrite(fd, &b, 1, 0) != 1) die("pwrite");
  double t1 = now_ms();
  if (fsync(fd) != 0) die("fsync");
  double t2 = now_ms();
  printf("firstwrite_ms=%.1f fsync_ms=%.1f\n", t1 - t0, t2 - t1);
  return 0;
}

// cowshed defrag's copy: pread/pwrite 8 MiB, F_NOCACHE, holes punched back, fsync.
static int cmd_copy(const char *src, const char *dst) {
  int in = open(src, O_RDONLY);
  if (in < 0) die("open src");
  int out = open(dst, O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (out < 0) die("open dst");
  fcntl(in, F_NOCACHE, 1);
  fcntl(out, F_NOCACHE, 1);
  struct stat st;
  fstat(in, &st);
  const size_t chunk = 8 << 20;
  unsigned char *buf = malloc(chunk);
  double t0 = now_ms();
  off_t off = 0, s, e;
  uint64_t copied = 0;
  while (next_region(in, off, st.st_size, &s, &e)) {
    for (off_t p = s; p < e;) {
      size_t n = (size_t)((e - p) < (off_t)chunk ? (e - p) : (off_t)chunk);
      if (pread(in, buf, n, p) != (ssize_t)n) die("pread");
      if (pwrite(out, buf, n, p) != (ssize_t)n) die("pwrite");
      p += n;
    }
    if (s > off) {
      fpunchhole_t h = {0, 0, off, s - off};
      if (fcntl(out, F_PUNCHHOLE, &h) == -1) die("F_PUNCHHOLE");
    }
    copied += e - s;
    off = e;
  }
  if (ftruncate(out, st.st_size) != 0) die("ftruncate");
  if (fsync(out) != 0) die("fsync");
  double ms = now_ms() - t0;
  printf("copied_bytes=%llu copy_ms=%.0f MBps=%.0f\n", (unsigned long long)copied, ms,
         copied / 1e6 / (ms / 1e3));
  return 0;
}

static unsigned char *random_buf(size_t n) {
  unsigned char *b = malloc(n);
  uint64_t x = 0x9E3779B97F4A7C15ull ^ (uint64_t)getpid();
  for (size_t i = 0; i + 8 <= n; i += 8) {
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    memcpy(b + i, &x, 8);
  }
  return b;
}

// Sequential write of `mb` MiB then F_FULLFSYNC; sequential F_NOCACHE read back.
static int cmd_seq(const char *path, long mb) {
  const size_t chunk = 8 << 20;
  unsigned char *buf = random_buf(chunk);
  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die("open");
  fcntl(fd, F_NOCACHE, 1);
  double t0 = now_ms();
  for (long i = 0; i < mb / 8; i++)
    if (write(fd, buf, chunk) != (ssize_t)chunk) die("write");
  if (fcntl(fd, F_FULLFSYNC) != 0) die("F_FULLFSYNC");
  double wms = now_ms() - t0;
  close(fd);
  fd = open(path, O_RDONLY);
  fcntl(fd, F_NOCACHE, 1);
  t0 = now_ms();
  ssize_t n;
  uint64_t total = 0;
  while ((n = read(fd, buf, chunk)) > 0) total += n;
  double rms = now_ms() - t0;
  close(fd);
  printf("seq_write_MBps=%.0f seq_read_MBps=%.0f bytes=%llu\n", total / 1e6 / (wms / 1e3),
         total / 1e6 / (rms / 1e3), (unsigned long long)total);
  return 0;
}

// Small-file metadata ops: create+write 4 KiB, symlink, unlink both.
static int cmd_small(const char *dir, long n) {
  char p[4096], q[4096];
  unsigned char *buf = random_buf(4096);
  double t0 = now_ms();
  for (long i = 0; i < n; i++) {
    snprintf(p, sizeof p, "%s/d%03ld/f%ld", dir, i % 256, i);
    if (i < 256) {
      char d[4096];
      snprintf(d, sizeof d, "%s/d%03ld", dir, i);
      mkdir(d, 0755);
    }
    int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die("create");
    if (write(fd, buf, 4096) != 4096) die("write small");
    close(fd);
  }
  double tc = now_ms();
  for (long i = 0; i < n; i++) {
    snprintf(p, sizeof p, "../d%03ld/f%ld", i % 256, i);
    snprintf(q, sizeof q, "%s/d%03ld/l%ld", dir, (i + 1) % 256, i);
    if (symlink(p, q) != 0) die("symlink");
  }
  double ts = now_ms();
  for (long i = 0; i < n; i++) {
    snprintf(p, sizeof p, "%s/d%03ld/f%ld", dir, i % 256, i);
    snprintf(q, sizeof q, "%s/d%03ld/l%ld", dir, (i + 1) % 256, i);
    if (unlink(p) != 0 || unlink(q) != 0) die("unlink");
  }
  double tu = now_ms();
  printf("create_ops=%.0f symlink_ops=%.0f unlink_ops=%.0f\n", n / ((tc - t0) / 1e3),
         n / ((ts - tc) / 1e3), 2 * n / ((tu - ts) / 1e3));
  return 0;
}

// Fill a file with `mb` MiB of pseudo-random bytes (data-set generation).
static int cmd_gen(const char *path, long mb) {
  const size_t chunk = 1 << 20;
  unsigned char *buf = random_buf(chunk);
  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die("open");
  for (long i = 0; i < mb; i++) {
    buf[i % chunk] ^= (unsigned char)i;
    if (write(fd, buf, chunk) != (ssize_t)chunk) die("write");
  }
  close(fd);
  return 0;
}

int main(int argc, char **argv) {
  if (argc >= 3 && !strcmp(argv[1], "extents")) return cmd_extents(argv[2]);
  if (argc >= 3 && !strcmp(argv[1], "firstwrite")) return cmd_firstwrite(argv[2]);
  if (argc >= 4 && !strcmp(argv[1], "copy")) return cmd_copy(argv[2], argv[3]);
  if (argc >= 4 && !strcmp(argv[1], "seq")) return cmd_seq(argv[2], atol(argv[3]));
  if (argc >= 4 && !strcmp(argv[1], "small")) return cmd_small(argv[2], atol(argv[3]));
  if (argc >= 4 && !strcmp(argv[1], "gen")) return cmd_gen(argv[2], atol(argv[3]));
  fprintf(stderr, "usage: xtool extents|firstwrite FILE | copy SRC DST | seq FILE MB | small DIR N | gen FILE MB\n");
  return 64;
}

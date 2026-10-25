/**
 * Take an exclusive flock(2) atomically with the `open(2)`: with `O_NONBLOCK` it is `flock(LOCK_EX | LOCK_NB)`, a lock
 * another descriptor holds fails the open with `EAGAIN` instead of waiting, and the descriptor keeps the lock until it
 * closes, however its process ends. macOS and the BSDs only, and absent from `fs.constants` (Node 26 and Bun 1.4 both
 * omit it). Linux has no such flag and ignores unknown bits, so a caller that sets this there holds nothing: gate every
 * use on `process.platform === 'darwin'`.
 */
export const O_EXLOCK = 0x20;

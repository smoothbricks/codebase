//! The host's admission stack as a discrete-event simulation (specs/cowshed/05_gateway.md, "Host
//! scheduling", "Admission", "Parallelism inside a job"). Pure: a task is numbers -- its units of
//! one-thread work, its memory, its dependencies -- and time jumps from one event to the next on a
//! virtual clock, so nothing is spawned, compiled or slept on and a whole gate replays in
//! milliseconds.
//!
//! The stack it composes: cowshed's job admission (the real
//! `cowshed_core::runtime::job_admission` state machine), a pool with mbx's admission contract or
//! a candidate's rule, Nx's `--parallel` slots over the task graph, each runner's thread count,
//! and the kernel sharing contended cores by class weight. Each candidate is a
//! [`policy::Policy`] value over the one [`engine`].
#![cfg(test)]

mod admit;
mod engine;
mod fixture;
mod model;
mod policy;
mod pool;
mod random;
mod replay;
mod score;

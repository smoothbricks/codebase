//! Native half of the generic replay image oracle. Emits the complete VM
//! image after a deterministic insert/remove stream for comparison with wasm.
use columine_types::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, Opcode, PROGRAM_MAGIC};
use columine_vm::state_init::{calculate_state_size, init_state};
use columine_vm::vm::{Vm, u32s_as_bytes};
use std::io::Write;

fn program(remove: bool) -> Vec<u8> {
    let init = [0x10, 0, 0x40, 128, 0, 0];
    let reduce: &[u8] = if remove {
        &[Opcode::BatchMapRemove as u8, 0, 0, 0]
    } else {
        &[Opcode::BatchMapUpsertLast as u8, 0, 0, 1, 0]
    };
    let mut out = vec![0; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([1, 0, 1, 2, 0, 0]);
    out.extend((init.len() as u16).to_le_bytes());
    out.extend((reduce.len() as u16).to_le_bytes());
    out.extend(init);
    out.extend(reduce);
    out
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("seed or programs");
    let insert = program(false);
    let remove = program(true);
    if first == "programs" {
        let mut out = std::io::stdout().lock();
        for program in [&insert, &remove] {
            out.write_all(&(program.len() as u32).to_le_bytes())
                .expect("program size");
            out.write_all(program).expect("program");
        }
        return;
    }
    let mut seed: u32 = first.parse().expect("u32 seed");
    let rounds: u32 = args.next().expect("rounds").parse().expect("u32 rounds");
    let mut state =
        vec![0; calculate_state_size(&insert, DEFAULT_ACCEPTED_PROGRAM_MAGICS) as usize];
    init_state(&mut state, &insert, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("valid fixture");
    let mut vm = Vm::default();
    for _ in 0..rounds {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let key = [(seed >> 8) % 64];
        let value = [seed];
        let program = if seed & 4 != 0 { &remove } else { &insert };
        let cols = [u32s_as_bytes(&key), u32s_as_bytes(&value)];
        assert_eq!(vm.execute_batch(&mut state, program, &cols, 1), 0);
    }
    std::io::stdout().write_all(&state).expect("write image");
}

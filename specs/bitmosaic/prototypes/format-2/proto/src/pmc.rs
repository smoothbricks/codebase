//! A kperf/kpep counter window around a cell (macOS, Apple silicon): the dlopen of kperf and kperfdata, the
//! kpep to kpc register image and the thread-counter reads. Counts only. Programming kpc is root-only, so the
//! binary is launched through a root trampoline, for example:
//!   sudo -n /usr/sbin/dtrace -q -n 'BEGIN{trace(0);}' -c '<bin> <args>'
//! Without it the harness still checks every image and prints sizes, with no counters.
use std::ffi::{CString, c_char, c_int, c_void};

unsafe extern "C" {
    fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
}

const RTLD_LAZY: c_int = 0x1;
const KPERF: &str = "/System/Library/PrivateFrameworks/kperf.framework/kperf";
const KPERFDATA: &str = "/System/Library/PrivateFrameworks/kperfdata.framework/kperfdata";
pub const MAX_COUNTERS: usize = 32;

fn sym(handle: *mut c_void, name: &str) -> Result<*mut c_void, String> {
    let cname = CString::new(name).expect("static name");
    let address = unsafe { dlsym(handle, cname.as_ptr()) };
    if address.is_null() {
        return Err(format!("{name} not found"));
    }
    Ok(address)
}

macro_rules! resolve {
    ($handle:expr, $name:literal, $ty:ty) => {
        match sym($handle, $name) {
            Ok(address) => unsafe { std::mem::transmute::<*mut c_void, $ty>(address) },
            Err(missing) => return Err(missing),
        }
    };
}

struct Kpc {
    force_all_ctrs_set: unsafe extern "C" fn(c_int) -> c_int,
    set_config: unsafe extern "C" fn(u32, *mut u64) -> c_int,
    set_counting: unsafe extern "C" fn(u32) -> c_int,
    set_thread_counting: unsafe extern "C" fn(u32) -> c_int,
    get_thread_counters: unsafe extern "C" fn(u32, u32, *mut u64) -> c_int,
}

pub struct Pmu {
    kpc: Kpc,
    classes: u32,
    regs: [u64; MAX_COUNTERS],
    indices: Vec<usize>,
    pub events: Vec<&'static str>,
}

impl Pmu {
    /// Resolve every named event on this chip's kpep database and program the PMU.
    pub fn open(events: &[&'static str]) -> Result<Self, String> {
        let kperf_path = CString::new(KPERF).expect("static path");
        let kpep_path = CString::new(KPERFDATA).expect("static path");
        let kperf = unsafe { dlopen(kperf_path.as_ptr(), RTLD_LAZY) };
        if kperf.is_null() {
            return Err("kperf framework did not open".to_owned());
        }
        let kpep = unsafe { dlopen(kpep_path.as_ptr(), RTLD_LAZY) };
        if kpep.is_null() {
            return Err("kperfdata framework did not open".to_owned());
        }
        let db_create = resolve!(
            kpep,
            "kpep_db_create",
            unsafe extern "C" fn(*const c_char, *mut *mut c_void) -> c_int
        );
        let db_event = resolve!(
            kpep,
            "kpep_db_event",
            unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut c_void) -> c_int
        );
        let config_create = resolve!(
            kpep,
            "kpep_config_create",
            unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> c_int
        );
        let config_force_counters = resolve!(
            kpep,
            "kpep_config_force_counters",
            unsafe extern "C" fn(*mut c_void) -> c_int
        );
        let config_add_event = resolve!(
            kpep,
            "kpep_config_add_event",
            unsafe extern "C" fn(*mut c_void, *mut *mut c_void, u32, *mut u32) -> c_int
        );
        let config_kpc = resolve!(
            kpep,
            "kpep_config_kpc",
            unsafe extern "C" fn(*mut c_void, *mut u64, usize) -> c_int
        );
        let config_kpc_classes = resolve!(
            kpep,
            "kpep_config_kpc_classes",
            unsafe extern "C" fn(*mut c_void, *mut u32) -> c_int
        );
        let config_kpc_count = resolve!(
            kpep,
            "kpep_config_kpc_count",
            unsafe extern "C" fn(*mut c_void, *mut usize) -> c_int
        );
        let config_kpc_map = resolve!(
            kpep,
            "kpep_config_kpc_map",
            unsafe extern "C" fn(*mut c_void, *mut usize, usize) -> c_int
        );
        let kpc = Kpc {
            force_all_ctrs_set: resolve!(
                kperf,
                "kpc_force_all_ctrs_set",
                unsafe extern "C" fn(c_int) -> c_int
            ),
            set_config: resolve!(
                kperf,
                "kpc_set_config",
                unsafe extern "C" fn(u32, *mut u64) -> c_int
            ),
            set_counting: resolve!(
                kperf,
                "kpc_set_counting",
                unsafe extern "C" fn(u32) -> c_int
            ),
            set_thread_counting: resolve!(
                kperf,
                "kpc_set_thread_counting",
                unsafe extern "C" fn(u32) -> c_int
            ),
            get_thread_counters: resolve!(
                kperf,
                "kpc_get_thread_counters",
                unsafe extern "C" fn(u32, u32, *mut u64) -> c_int
            ),
        };

        let mut db: *mut c_void = std::ptr::null_mut();
        if unsafe { db_create(std::ptr::null(), &raw mut db) } != 0 || db.is_null() {
            return Err("kpep_db_create failed".to_owned());
        }
        let mut config: *mut c_void = std::ptr::null_mut();
        if unsafe { config_create(db, &raw mut config) } != 0 || config.is_null() {
            return Err("kpep_config_create failed".to_owned());
        }
        if unsafe { config_force_counters(config) } != 0 {
            return Err("kpep_config_force_counters failed".to_owned());
        }
        for name in events {
            let event_name = CString::new(*name).expect("static name");
            let mut event: *mut c_void = std::ptr::null_mut();
            if unsafe { db_event(db, event_name.as_ptr(), &raw mut event) } != 0 || event.is_null()
            {
                return Err(format!("chip kpep database lacks {name}"));
            }
            let mut event_slot = event;
            if unsafe { config_add_event(config, &raw mut event_slot, 0, std::ptr::null_mut()) }
                != 0
            {
                return Err(format!(
                    "kpep_config_add_event failed at {name} (does not fit the counter file)"
                ));
            }
        }
        let mut classes = 0u32;
        if unsafe { config_kpc_classes(config, &raw mut classes) } != 0 {
            return Err("kpep_config_kpc_classes failed".to_owned());
        }
        let mut reg_count = 0usize;
        if unsafe { config_kpc_count(config, &raw mut reg_count) } != 0 || reg_count > MAX_COUNTERS
        {
            return Err(format!(
                "kpep_config_kpc_count failed or oversized ({reg_count})"
            ));
        }
        let mut regs = [0u64; MAX_COUNTERS];
        if unsafe { config_kpc(config, regs.as_mut_ptr(), reg_count * size_of::<u64>()) } != 0 {
            return Err("kpep_config_kpc failed".to_owned());
        }
        let mut map = [0usize; MAX_COUNTERS];
        if unsafe { config_kpc_map(config, map.as_mut_ptr(), size_of::<[usize; MAX_COUNTERS]>()) }
            != 0
        {
            return Err("kpep_config_kpc_map failed".to_owned());
        }
        let indices: Vec<usize> = map[..events.len()].to_vec();
        if let Some(overflow) = indices.iter().find(|&&i| i >= MAX_COUNTERS) {
            return Err(format!(
                "an event mapped past the counter file ({overflow})"
            ));
        }
        let mut pmu = Pmu {
            kpc,
            classes,
            regs,
            indices,
            events: events.to_vec(),
        };
        let k = &pmu.kpc;
        let programmed = unsafe {
            if (k.force_all_ctrs_set)(1) != 0 {
                Err(
                    "kpc_force_all_ctrs_set EPERM (root required: run via `sudo -n dtrace -q -n 'BEGIN{trace(0);}' -c '<bin> <args>'`)",
                )
            } else if (k.set_config)(pmu.classes, pmu.regs.as_mut_ptr()) != 0 {
                Err("kpc_set_config failed")
            } else if (k.set_counting)(pmu.classes) != 0 {
                Err("kpc_set_counting failed")
            } else if (k.set_thread_counting)(pmu.classes) != 0 {
                Err("kpc_set_thread_counting failed")
            } else {
                Ok(())
            }
        };
        programmed.map_err(str::to_owned)?;
        Ok(pmu)
    }

    /// One read of the calling thread's counter file, in `events` order.
    pub fn read(&self) -> Result<Vec<u64>, String> {
        let mut buf = [0u64; MAX_COUNTERS];
        if unsafe { (self.kpc.get_thread_counters)(0, MAX_COUNTERS as u32, buf.as_mut_ptr()) } != 0
        {
            return Err("kpc_get_thread_counters failed".to_owned());
        }
        Ok(self.indices.iter().map(|&i| buf[i]).collect())
    }

    /// Leave the PMU as found.
    pub fn disarm(&self) {
        unsafe {
            if (self.kpc.set_thread_counting)(0) != 0 || (self.kpc.set_counting)(0) != 0 {
                eprintln!("bm-pmc: PMU disarm failed; counters may stay armed");
            }
            let _ = (self.kpc.force_all_ctrs_set)(0);
        }
    }
}

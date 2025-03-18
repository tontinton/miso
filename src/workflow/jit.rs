use std::slice::from_raw_parts;

use color_eyre::{eyre::eyre, Result};
use cranelift::{
    jit::{JITBuilder, JITModule},
    module::default_libcall_names,
    native,
    prelude::{settings, Configurable},
};
use memchr::memchr_iter;

pub fn new_jit_module() -> Result<JITModule> {
    let mut flag_builder = settings::builder();
    flag_builder.set("use_colocated_libcalls", "false")?;
    flag_builder.set("is_pic", "false")?;
    let isa_builder =
        native::builder().map_err(|msg| eyre!("host machine is not supported: {}", msg))?;
    let isa = isa_builder.finish(settings::Flags::new(flag_builder))?;
    let builder = JITBuilder::with_isa(isa, default_libcall_names());
    Ok(JITModule::new(builder))
}

pub fn starts_with(
    hay_ptr: *const u8,
    hay_len: usize,
    needle_ptr: *const u8,
    needle_len: usize,
) -> bool {
    if needle_len > hay_len {
        return false;
    }

    // Safety: We assume that the pointers are valid and the memory is correctly aligned.
    let (hay, needle) = unsafe {
        (
            from_raw_parts(hay_ptr, hay_len),
            from_raw_parts(needle_ptr, needle_len),
        )
    };

    hay.get(0..needle_len) == Some(needle)
}

pub fn contains(
    hay_ptr: *const u8,
    hay_len: usize,
    needle_ptr: *const u8,
    needle_len: usize,
) -> bool {
    if needle_len > hay_len {
        return false;
    }

    // Safety: We assume that the pointers are valid and the memory is correctly aligned.
    let (hay, needle) = unsafe {
        (
            from_raw_parts(hay_ptr, hay_len),
            from_raw_parts(needle_ptr, needle_len),
        )
    };

    for pos in memchr_iter(needle[0], hay) {
        if hay[pos..].get(0..needle_len) == Some(needle) {
            return true;
        }
    }
    false
}

//! Types that can live in a shared memory region.
//!
//! A shared region's bytes are written by remote machines, so the
//! library can only hand them back as values of a type `T` when every
//! bit pattern is a valid `T`. [`RemoteSafe`] is the (unsafe,
//! user-asserted) marker for such types; [`impl_remote_safe!`] and
//! [`zeroed_vec`] are the small pieces of machinery around it.

/// A type whose values can live in a shared memory region: **every
/// bit pattern of `size_of::<T>()` bytes is a valid `T`**.
///
/// Remote machines write arbitrary bytes into shared regions, and the
/// library hands those bytes back as `T`s — that is only sound if no
/// pattern is invalid. The compiler cannot check this, so implementing
/// the trait is an assertion by the author that `T` qualifies.
///
/// # Safety
///
/// Implementing this trait asserts, on pain of undefined behavior:
///
/// - `T` is `Copy` (a supertrait): no destructor ever runs on values
///   the remote side fabricated.
/// - Every bit pattern is a valid `T`: `#[repr(C)]` structs of
///   integers, floats, and other `RemoteSafe` types qualify. `bool`,
///   `char`, enums (arbitrary discriminants), references, `Box`,
///   `NonZero*`, and anything containing them do **not** — a remote
///   writer could produce values whose mere existence is undefined
///   behavior in Rust.
/// - Prefer layouts without padding: padding bytes arrive as garbage.
///   Reading fields is fine either way; just never make padding
///   meaningful to the application.
/// - `#[repr(Rust)]` layout is an implementation detail, so agree on
///   `#[repr(C)]` with the remote applications: they are compiled
///   separately and must lay the data out identically. The library
///   cannot verify that the sharing and the reading side chose the
///   same `T` — a mismatch reads garbage without failing, by design.
pub unsafe trait RemoteSafe: Copy + 'static {}

/// Implement [`RemoteSafe`](crate::RemoteSafe) for the listed types.
///
/// Only use it on types meeting the contract documented on the trait:
/// `#[repr(C)]`, `Copy`, made of integers/floats/other `RemoteSafe`
/// members, no `bool`/`char`/enum/reference members.
///
/// ```
/// #[repr(C)]
/// #[derive(Clone, Copy)]
/// struct Reading {
///     timestamp: u64,
///     value: f32,
/// }
/// rdmalib::impl_remote_safe!(Reading);
/// ```
#[macro_export]
macro_rules! impl_remote_safe {
    ($($t:ty),* $(,)?) => {
        $(unsafe impl $crate::RemoteSafe for $t {})*
    };
}

crate::impl_remote_safe!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64,
);

// Arrays are contiguous runs of their element: any bit pattern valid
// for the element is valid for the array.
unsafe impl<T: RemoteSafe, const N: usize> RemoteSafe for [T; N] {}

/// A `Vec` of `len` all-zero `T`s, for reads that fill a typed buffer.
pub(crate) fn zeroed_vec<T: RemoteSafe>(len: usize) -> Vec<T> {
    let mut vec: Vec<T> = Vec::with_capacity(len);
    // SAFETY: `write_bytes` initializes the `len` elements the
    // allocation has capacity for, and all-zero bits are a valid `T`
    // by the `RemoteSafe` contract, so `set_len` only exposes valid
    // values.
    unsafe {
        std::ptr::write_bytes(vec.as_mut_ptr(), 0, len);
        vec.set_len(len);
    }
    vec
}

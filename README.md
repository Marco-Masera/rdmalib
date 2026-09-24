# RDMA-Lib

Name is temporary. Lib is in development.
Provides abstraction to RDMA primitives for Rust applications.


# Usage 

Both sides exchange metadata over TCP; the data itself travels over
RDMA, with no copies on either side. Both nodes define the same
shared type (they are separately compiled — the library checks the
element layout, the type identity is the developer's responsibility):

```rust
#[repr(C)]
#[derive(Clone, Copy)]
struct Test {
    v: usize,
}
rdmalib::impl_remote_safe!(Test);
```

Node 1 creates the array and shares it:

```rust
let owner = rdmalib::SharedMemoryRegionProvider::new(
    rdmalib::SharedMemoryRegionProviderAddr::new(18515, 9917)); // RDMA, TCP ports
let handle = owner.register_typed("tests", vec![Test { v: 1 }, Test { v: 2 }, Test { v: 3 }]);
owner.serve()?; // binds both ports, serves readers in the background
```

Node 2 runs a session and reads the array and its values:

```rust
let reader = rdmalib::RemoteMemoryProvider::new(
    rdmalib::RemoteMemoryProviderAddr::new("node1", 18515, 9917));
reader.update(0)?; // session: greet, join group 0, RDMA rendezvous, tuples
let catalog = reader.get_remote_mr_metadata();
let region = reader.get_remote_mr(&catalog[0], Some(0)).unwrap();
let tests = region.read_typed::<Test>(0, 3)?; // [Test { v: 1 }, Test { v: 2 }, Test { v: 3 }]
assert_eq!(tests.iter().map(|t| t.v).collect::<Vec<_>>(), vec![1, 2, 3]);
```

## Word-level alignment

RDMA one-sided accesses are **atomic at the machine-word level** when
both the address and the size of the access are naturally aligned — a
multiple of 8 bytes on typical 64-bit hardware. An aligned 8-byte
access is never observed half-done by the other side; an unaligned or
odd-sized one can be, while a remote reader/writer is concurrently
active on the same area. (Atomicity holds per aligned word: a larger
transfer is performed as a series of word-sized units, so the transfer
as a whole is *not* atomic.)

Word alignment is **not required** — every API in this library works
with any byte offset and size within the region's bounds, and nothing
enforces multiples of 8. Sharing a `Vec` of 12-byte structs and
reading at offset 3 is fine; it merely forgoes word-level atomicity.
Think of it as a convention for data that remote readers and writers
may access concurrently, not a rule of the library.

### How to ensure it

Everything follows from one property — **the element type's alignment
is at least 8** — plus word-aligned offsets:

1. **Give the type an alignment of ≥ 8.** A `Vec`'s allocation is
   guaranteed aligned to `align_of::<T>()`, and nothing stronger, so
   this is what makes the region's *base address* 8-aligned. In
   practice glibc's allocator returns 16-aligned pointers on x86-64,
   but don't rely on that: guarantee it with a naturally aligned field
   (`u64`, `f64`, `usize`) or explicitly with `#[repr(C, align(8))]`.
   Never use `#[repr(packed)]` for types meant to be word-atomic.
2. **The element size follows automatically.** `size_of::<T>()` is
   always a multiple of `align_of::<T>()`, so an 8-aligned type is
   also a multiple of 8 bytes in size (tail padding is inserted for
   you) — every element boundary stays word-aligned. A 12-byte struct
   can only exist because its alignment is ≤ 4.
3. **Use offsets that are multiples of 8.** With an 8-aligned base,
   those are the absolute word boundaries. For typed reads the offset
   is in bytes: element `i` starts at `i * size_of::<T>()`, already a
   multiple of 8 given the two rules above.

For typed reads, the library double-checks rule 3:
[`read_typed`](src/readers.rs) and `read_into_typed` reject any
`(region base + offset)` not aligned to `align_of::<T>()`, so with an
8-aligned `T` the typed API cannot express a misaligned access. That
is a safety net against accidents, not an atomicity mechanism — the
byte-level `read`/`read_into` accept any offset and size.

### The buffer is exactly the elements

A `Vec`'s metadata (pointer, length, capacity) lives in the `Vec`
value itself, outside the data allocation — the registered region is
exactly the elements, contiguous, starting at the address captured at
registration. No inline header shifts element positions; allocator
bookkeeping, if any, sits *before* the registered address, not inside
it.

### Example

```rust
/// Word-atomic by construction: `align(8)` gives the region an
/// 8-aligned base, and the size rounds up to 16 bytes (a multiple
/// of 8), so every element sits on word boundaries.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
struct Reading {
    timestamp: u64,
    value: f32, // 4 bytes of tail padding follow, implied
}
rdmalib::impl_remote_safe!(Reading);

// Provider side: the base is 8-aligned, elements are 16 bytes apart.
let handle = provider.register_typed("readings", vec![Reading { timestamp: 0, value: 0.0 }; 100]);

// Reader side: offset in bytes, a multiple of 8 — here, element 3.
let r = region.read_typed::<Reading>(3 * 16, 1)?;
```

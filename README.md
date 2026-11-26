# RDMA-Lib

Name is temporary. The library is in development.
It provides abstraction to RDMA primitives for Rust applications, specifically for disaggregated memory settings.

# Structure

Targeting DM settings, the library is asimmetrical, separating who offers its local memory remotely (the **Provider**), and who accesses it (the **Client**):
* `SharedMemoryRegionProvider` -> A construct that allows processes to share parts of their local memory.
* `RemoteMemoryRegion` -> A construct that allows to access remote regions.

That said, nothing forbids a single Node to play both roles, by creating instances of both constructs.

# Usage

## Define custom data types (optional)

Applications can define and register their own data types. 
Only types with compile time known size are accepted, and the macro `impl_remote_safe!` validates the constraint.
```rust
#[repr(C)]
#[derive(Clone, Copy)]
struct MyDataType {
    v: u32,
    fixed_size_string: [u8; 64]
}
rdmalib::impl_remote_safe!(Test);
```

## Share a local buffer

```rust
// Initialize the SharedMemoryRegionProvider struct
let address = SharedMemoryRegionProviderAddr::new(
1010, //RDMA port
2020, // TCP tcp_port
);
let shared_memory_provider = SharedMemoryRegionProvider::new(address);
shared_memory_provider.serve()?;

// Define a memory region to share 
let mut buffer = vec![MyDataType; 256];
// Share it 
let handle: SharedMemoryRegionHandle<MyDataType> shared_memory_provider.register_typed(
    "MyBuffer", //Name for the memory region
    buffer, // Now buffer is owned by the SharedMemoryRegionProvider
    false // access is not revocable
);
```

## Access a remote address, read and write

In order to access a remote region, the process must first connect to the Provider, then obtain a `RemoteMemoryRegion` instance.

```rust
// Attach to the remote memory provider
let memory_node_addr = RemoteMemoryProviderAddr::new(
    "192.168.1.1", // IP address of the owner of the memory
    1010, // RDMA port of the owner 
    2020, // TCP port of the owner
)
let remote_memory = RemoteMemoryProvider::new();
remote_memory.join_and_fetch(0)?; // Join a group, fetch metadata

// Connect to a region using the region name
let region: RemoteMemoryRegion = remote_memory.find_remote_by_name("MyBuffer", 0)?;
```

With `RemoteMemoryRegion`, the process can read remote memory:

```rust
// Read from remote memory, allocating local memory to store the data
let mut content: Vec<MyDataType> = region.read_typed::<MyDataType>(
    0, 10 // offset - num_elements. This call reads the first 10 elements in the buffer
)?;

// The reader can also provide a reference to a local buffer, skipping memory allocation
region.read_into_typed(
    30, &mut content // content is filled from element 30 onward, until full
);
```

And write on remote memory:
```rust
// Define what you want to write 
let to_write: &[MyDataType] = & [
    MyDataType { v: 1, fixed_size_string: [0; 64] },
    MyDataType { v: 2, fixed_size_string: [b'a'; 64] },
];
// Write it at position 10
region.write_typed(10, to_write);
```

## Registered buffers: zero-copy reads and writes

Every read and write above passes through a small internal scratch buffer, causing one extra local copy per operation.
An application that accesses a region at high frequency can skip it: register a buffer **once** with `register_buffer_typed`, and the *registered* APIs read and write the remote memory straight into and out of it, with no copy per operation.

The registration pins the buffer's pages, and the buffer follows the group's connection across its re-establishments — it re-registers itself on first use, so recoveries need no bookkeeping.

```rust
// Register a local buffer
let mut buf = remote_memory.register_buffer_typed(
    0,
    vec![MyDataType { v: 0, fixed_size_string: [0; 64] }; 16],
)?;
region.read_into_registered(0, 16, &mut buf)?;   // remote data lands directly in buf
region.write_from_registered(0, 16, &buf)?;      // remote memory is written directly from it
```

## Revocable access

Permission management and revocation is managed through **groups**.
As shown in the examples above, the client needs to join at least one group before accessing regions: `remote_memory.join_and_fetch(GROUP)`, and then specifies a group again when accessing the region: `remote_memory.find_remote_by_name("MyBuffer", GROUP)`

When a region is registered as revocable, the provider can then set permissions at group-level.

```rust
// A region registered as revocable
let handle = shared_memory_provider.register_typed("MyBuffer", buffer, true);
...
// Revoke group 5's access: returns only after the window's
// zero-length bind *completed* at the device — from that point no
// access under the previous binding is served, a request already
// in flight included; everything written afterwards is invisible
// through group 5's rkey.
owner.revoke_region(handle.id(), 5)?;
...
// Re-grant: the window binds over the full range again, under a
// fresh rkey (the old one never revives) the group picks up by
// re-requesting its tuples (reader.join_and_fetch).
owner.grant_region(handle.id(), 5)?;
```

A revoked group keeps being served its — now dead — rkey, so its
operations fail with a remote access violation a consumer can
classify (`rdmalib::is_remote_access`; the flush that follows a
failed queue pair is `rdmalib::is_flushed`) instead of the region
vanishing from its catalog. 
A revocation kills the queue pair. A client that gets its permissions back must re-establish it with `reader.reconnect(group)`.

```rust
// A client registers in group 5
dm.join_and_fetch(5)?;
let mut region = dm.find_remote_by_name("RemoteRegionName", 5)?;
// then it tries to read 
match region.read_into_typed(0, &mut buffer) {
    Ok(()) => {} // Read succeeded, and data is in buffer
    Err(e) if rdmalib::is_remote_access(&e) || rdmalib::is_remote_op(&e) => {
        // Permission was revoked!
        // ...must ask the provider to grant permission back, outside the library scope...
        // after permissions are granted:
        dm.reconnect(5)?; // Run a fresh session: new QP, fresh rkey fetched
        region = dm.find_remote_by_name("RemoteRegionName", 5)?; // fresh rkey, same name
        
    }
    Err(e) => return Err(e), // Other error
}
```

## Raw bytes APIs

The APIs described so far accept a type T, and shared memory regions are logical arrays of type T.
The raw bytes APIs instead allow to share buffers of bytes with no type enforcement.

Provider side: register raw bytes.

```rust
let handle = shared_memory_provider.register(
    SharedMemoryRegionMetadata::new("MyBuffer", vec![0u8; 4096]),
    false,
);
handle.borrow_mut()[0] = 1; // &mut [u8]
```

Reader side: the same groups and lookups, but offsets and lengths count bytes, and no element layout travels or is checked.

```rust
remote_memory.join_and_fetch(0)?;
let region = remote_memory.find_remote_by_name("MyBuffer", 0)?;

let head: Vec<u8> = region.read(0, 4)?;      // first 4 bytes, allocated for you
region.read_into(0, 4, &mut scratch)?;       // into a buffer you already own
region.write(4, &[0u8; 4])?;                 // one-sided write, any size that fits
```

The zero-copy paths have byte flavors too — a registered buffer, then
registered reads and writes:

```rust
let mut buf = remote_memory.register_buffer(0, vec![0u8; 4096])?;
region.read_into_registered(0, 4096, &mut buf)?;
region.write_from_registered(0, 4096, &buf)?;
```

# Word-level alignment

RDMA one-sided accesses are **atomic at the machine-word level** when both the address and the size of the access are naturally aligned.

The library itself does not enforce word-level alignment when sharing a raw bytes buffer: the client is allowed to read and write at any offset and length. On the contrary, alignment is **enforced** when using the typed API.

If the user wants to enforce word-level alignment:
**Give the type T an alignment of ≥ 8.**. Already satisfied for naturally aligned fields (`u64`, `f64`, `usize`), and can be enforced on custom data types with `#[repr(C, align(8))]`

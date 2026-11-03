use crate::RemoteSafe;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
struct Sample {
    voltage: f32,
    index: u32,
}

crate::impl_remote_safe!(Sample);

#[test]
fn primitives_structs_and_arrays_are_remote_safe() {
    fn assert_remote_safe<T: RemoteSafe>() {}
    assert_remote_safe::<u8>();
    assert_remote_safe::<f64>();
    assert_remote_safe::<Sample>();
    assert_remote_safe::<[Sample; 4]>();
    assert_remote_safe::<[[u8; 2]; 8]>();
}

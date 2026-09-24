use crate::pod::zeroed_vec;
use crate::RemoteSafe;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
struct Sample {
    voltage: f32,
    index: u32,
}

crate::impl_remote_safe!(Sample);

#[test]
fn zeroed_vec_is_all_zeros() {
    let ints: Vec<u32> = zeroed_vec(4);
    assert_eq!(ints, vec![0, 0, 0, 0]);

    let samples: Vec<Sample> = zeroed_vec(2);
    assert_eq!(samples, vec![Sample { voltage: 0.0, index: 0 }; 2]);

    let arrays: Vec<[u64; 2]> = zeroed_vec(3);
    assert_eq!(arrays, vec![[0, 0]; 3]);
}

#[test]
fn primitives_structs_and_arrays_are_remote_safe() {
    fn assert_remote_safe<T: RemoteSafe>() {}
    assert_remote_safe::<u8>();
    assert_remote_safe::<f64>();
    assert_remote_safe::<Sample>();
    assert_remote_safe::<[Sample; 4]>();
    assert_remote_safe::<[[u8; 2]; 8]>();
}

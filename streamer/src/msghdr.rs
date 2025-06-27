#![cfg(target_os = "linux")]

use {
    libc::{iovec, msghdr, sockaddr_storage, socklen_t},
    std::{
        mem::{zeroed, MaybeUninit},
        ptr,
    },
};

pub(crate) fn create_msghdr(
    msg_name: &mut sockaddr_storage,
    msg_namelen: socklen_t,
    iov: &mut iovec,
) -> msghdr {
    // Cannot construct msghdr directly on musl
    // See https://github.com/rust-lang/libc/issues/2344 for more info
    // SAFETY: `msghdr` is POD, it's safe to initialize it with zeros.
    let mut msg_hdr: msghdr = unsafe { zeroed() };
    msg_hdr.msg_name = msg_name as *mut sockaddr_storage  as *mut _;
    msg_hdr.msg_namelen = msg_namelen;
    msg_hdr.msg_iov = iov as *mut iovec as *mut _;
    msg_hdr.msg_iovlen = 1;
    msg_hdr.msg_control = ptr::null::<libc::c_void>() as *mut _;
    msg_hdr.msg_controllen = 0;
    msg_hdr.msg_flags = 0;
    msg_hdr
}

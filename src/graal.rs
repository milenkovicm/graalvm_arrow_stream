use crate::bindings::{graal_isolate_t, graal_isolatethread_t};

///
pub(crate) type FnTearDownIsolate =
    unsafe extern "C" fn(*mut graal_isolatethread_t) -> ::std::os::raw::c_int;
///
pub(crate) type FnDetachThread =
    unsafe extern "C" fn(thread: *mut graal_isolatethread_t) -> ::std::os::raw::c_int;
///
pub(crate) type FnAttachThread = unsafe extern "C" fn(
    isolate: *mut graal_isolate_t,
    thread: *mut *mut graal_isolatethread_t,
) -> ::std::os::raw::c_int;

///
pub(crate) type FnCreateReader = unsafe extern "C" fn(
    arg1: *mut graal_isolatethread_t,
    arg2: *mut ::std::os::raw::c_char,
    arg3: ::std::os::raw::c_long,
) -> ::std::os::raw::c_int;

///
pub(crate) type FnLastError = unsafe extern "C" fn(
    arg1: *mut graal_isolatethread_t,
    arg2: *mut ::std::os::raw::c_char,
    arg3: ::std::os::raw::c_int,
) -> ::std::os::raw::c_int;

//! GraalArrowStreamer provides integration between GraalVM and Rust Apache Arrow.
//!
//! This module allows streaming Arrow record batches from a GraalVM native library that implements
//! the Arrow C Data Interface. The main components are:
//!
//! - [GraalArrowStreamer]: Main interface for creating and managing GraalVM isolates and Arrow streams
//! - [LocalArrowArrayStreamReader]: A record batch reader that is tied to the lifetime of its parent isolate
//!
//! # Safety
//!
//! This struct handles unsafe FFI calls to GraalVM native methods and manages memory/resource cleanup.
//! It is marked as `Send` and `Sync` since the underlying GraalVM isolate can be safely shared between threads.
//!
//! [GraalArrowStreamer] has to outlive any batches created by any of created [LocalArrowArrayStreamReader]
//! otherwise `SIGSEGV` may occur, as dropping of the batches will try to call isolate which produced it to
//! release memory.
//!
//! # Example
//!
//! ```rust,no_run
//! use graalvm_arrow_stream::GraalArrowStreamer;
//!
//! // Create streamer from a native library
//! let streamer = GraalArrowStreamer::try_new_from_name("my_lib").unwrap();
//!
//! // Create a reader for a specific data source
//! let mut reader = streamer.create_reader("path/to/data").unwrap();
//!
//! // Iterate through record batches
//! while let Some(batch) = reader.next() {
//!     // Process batch
//! }
//! ```
//!
//! # Memory Management
//!
//! The struct manages the lifecycle of:
//!
//! - GraalVM isolate
//! - Native library handle
//! - Thread attachments
//!
//! The isolate is automatically torn down when the [GraalArrowStreamer] is dropped.
//! Readers created by this struct must not outlive the [GraalArrowStreamer] instance.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused)]

use std::{
    ffi::{CStr, c_char},
    marker::PhantomData,
    mem::MaybeUninit,
    path::PathBuf,
    sync::Arc,
};

use arrow::{
    array::{RecordBatch, RecordBatchReader},
    datatypes::SchemaRef,
    error::{ArrowError, Result},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};

use libloading::{Library, Symbol};

use crate::{
    bindings::{
        gas_last_error_fn_t, gas_reader_stream_fn_t, graal_attach_thread_fn_t,
        graal_create_isolate_fn_t, graal_detach_thread_fn_t, graal_isolate_t,
        graal_isolatethread_t, graal_tear_down_isolate_fn_t,
    },
    graal::{FnAttachThread, FnCreateReader, FnDetachThread, FnLastError, FnTearDownIsolate},
};
mod bindings;
mod graal;

#[derive(Debug)]
pub struct GraalArrowStreamer {
    // Isolate needs to outlive any interaction with
    // GraalVM, that includes releasing batches
    // produced by the graal stream reader
    ptr_isolate: *mut graal_isolate_t,
    f_tear_down_isolate: FnTearDownIsolate,
    f_detach_thread: FnDetachThread,
    f_attach_thread: FnAttachThread,
    f_create_reader: FnCreateReader,
    f_last_error: FnLastError,
    // library needs to outlive others
    #[allow(dead_code)]
    lib: Arc<Library>,
}

unsafe impl Send for GraalArrowStreamer {}
unsafe impl Sync for GraalArrowStreamer {}

impl GraalArrowStreamer {
    /// Creates a new instance using a library name.
    ///
    /// The library name will be converted to the appropriate platform-specific library filename.
    /// `LD_LIBRARY_PATH` should be considered when loading library
    ///
    /// # Arguments
    /// * `lib_name` - The name of the library without platform-specific prefix/suffix.
    ///                If the name is `libgas.so` only `gas` should be used as name.
    ///
    /// # Returns
    /// * `Result<Self>` - A new GraalArrowStreamer instance or an error
    ///
    /// # Safety
    /// This function loads a dynamic library, which is inherently unsafe
    pub fn try_new_from_name(lib_name: &str) -> Result<Self> {
        let filename = libloading::library_filename(lib_name);
        let lib =
            unsafe { Library::new(filename).map_err(|e| ArrowError::ExternalError(Box::new(e)))? };

        Self::try_new(lib.into())
    }

    /// Creates a new instance from a specific file path.
    ///
    /// # Arguments
    /// * `file` - The complete file path to the library
    ///
    /// # Returns
    /// * `Result<Self>` - A new GraalArrowStreamer instance or an error
    ///
    /// # Safety
    /// This function loads a dynamic library, which is inherently unsafe
    pub fn try_new_from_file(file: &str) -> Result<Self> {
        let lib =
            unsafe { Library::new(file).map_err(|e| ArrowError::ExternalError(Box::new(e)))? };

        Self::try_new(lib.into())
    }

    /// Creates a new instance using a library name and path.
    ///
    /// # Arguments
    /// * `lib_name` - The name of the library without platform-specific prefix/suffix.
    ///                If the name is `libgas.so` only `gas` should be used as name.
    /// * `lib_path` - The path where the library is located
    ///
    /// # Returns
    /// * `Result<Self>` - A new GraalArrowStreamer instance or an error
    ///
    /// # Safety
    /// This function loads a dynamic library, which is inherently unsafe
    pub fn try_new_from_name_and_path(lib_name: &str, lib_path: &str) -> Result<Self> {
        let mut filename = PathBuf::from(lib_path);
        filename.push(libloading::library_filename(lib_name));

        let lib =
            unsafe { Library::new(filename).map_err(|e| ArrowError::ExternalError(Box::new(e)))? };

        Self::try_new(lib.into())
    }

    /// Creates a new instance from an already loaded library.
    ///
    /// Initializes the GraalVM isolate and loads all required function symbols.
    ///
    /// # Arguments
    /// * `lib` - Arc-wrapped Library instance
    ///
    /// # Returns
    /// * `Result<Self>` - A new GraalArrowStreamer instance or an error
    ///
    /// # Safety
    /// This function interacts with raw pointers and FFI functions
    pub fn try_new(lib: Arc<Library>) -> Result<Self> {
        unsafe {
            let f_graal_create_isolate: Symbol<graal_create_isolate_fn_t> = lib
                .get(b"graal_create_isolate")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_graal_create_isolate = f_graal_create_isolate.ok_or_else(|| {
                ArrowError::CDataInterface("can't find f_graal_create_isolate method".into())
            })?;

            let f_graal_tear_down_isolate: Symbol<graal_tear_down_isolate_fn_t> = lib
                .get(b"graal_tear_down_isolate")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_graal_tear_down_isolate = f_graal_tear_down_isolate.ok_or_else(|| {
                ArrowError::CDataInterface("can't find f_graal_tear_down_isolate method".into())
            })?;

            let f_graal_detach_thread: Symbol<graal_detach_thread_fn_t> = lib
                .get(b"graal_detach_thread")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_graal_detach_thread = f_graal_detach_thread.ok_or_else(|| {
                ArrowError::CDataInterface("can't find f_graal_detach_tread method".into())
            })?;

            let f_graal_attach_thread: Symbol<graal_attach_thread_fn_t> = lib
                .get(b"graal_attach_thread")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_graal_attach_thread = f_graal_attach_thread.ok_or_else(|| {
                ArrowError::CDataInterface("can't find graal_attach_thread method".into())
            })?;

            let f_gas_reader_stream: Symbol<gas_reader_stream_fn_t> = lib
                .get(b"gas_reader_stream")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_gas_reader_stream = f_gas_reader_stream.ok_or_else(|| {
                ArrowError::CDataInterface("can't find gas_reader_stream method".into())
            })?;

            let f_gas_last_error: Symbol<gas_last_error_fn_t> = lib
                .get(b"gas_last_error")
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let f_gas_last_error = f_gas_last_error.ok_or_else(|| {
                ArrowError::CDataInterface("can't find gas_last_error method".into())
            })?;

            //
            //
            //
            //

            let mut ptr_isolate: *mut graal_isolate_t = MaybeUninit::zeroed().assume_init();
            let mut ptr_thread: *mut graal_isolatethread_t = MaybeUninit::zeroed().assume_init();

            // attach to actual graalvm, this will start a
            // new JVM instance (isolate).
            if f_graal_create_isolate(std::ptr::null_mut(), &mut ptr_isolate, &mut ptr_thread) != 0
            {
                return Err(ArrowError::CDataInterface(
                    "run f_graal_create_isolate failed".into(),
                ));
            }

            // we're detaching thread as its only needed
            // when we interact with isolate
            f_graal_detach_thread(ptr_thread);

            Ok(Self {
                lib,
                ptr_isolate,
                f_tear_down_isolate: f_graal_tear_down_isolate,
                f_detach_thread: f_graal_detach_thread,
                f_attach_thread: f_graal_attach_thread,
                f_create_reader: f_gas_reader_stream,
                f_last_error: f_gas_last_error,
            })
        }
    }

    /// Creates an Arrow array stream reader for the specified path.
    ///
    /// # Arguments
    /// * `path` - The path to the data source
    ///
    /// # Returns
    /// * `Result<LocalArrowArrayStreamReader>` - A reader for the Arrow array stream or an error

    pub fn create_reader<'local>(
        &'local self,
        path: &str,
    ) -> Result<LocalArrowArrayStreamReader<'local>> {
        unsafe {
            let ptr_thread: *mut graal_isolatethread_t = self.attach_tread()?;
            let c_str_path = std::ffi::CString::new(path).unwrap();
            let c_str_path = c_str_path.into_raw();

            let mut ffi_stream = FFI_ArrowArrayStream::empty();
            let stream_address = std::ptr::addr_of!(ffi_stream) as i64;

            let err_code = (self.f_create_reader)(ptr_thread, c_str_path, stream_address);

            //

            if err_code != 0 {
                let error = self.last_error(ptr_thread);
                self.detach_thread(ptr_thread)?;
                return Err(ArrowError::CDataInterface(error));
            } else {
                self.detach_thread(ptr_thread)?;
                let stream_reader = ArrowArrayStreamReader::from_raw(&mut ffi_stream)?;
                Ok(LocalArrowArrayStreamReader {
                    inner: stream_reader,
                    pd: PhantomData::default(),
                })
            }
        }
    }

    /// Retrieves the last error message from the GraalVM context.
    ///
    /// # Arguments
    /// * `ptr_thread` - Pointer to the GraalVM thread
    ///
    /// # Returns
    /// * `String` - The error message

    fn last_error(&self, ptr_thread: *mut graal_isolatethread_t) -> String {
        let buffer_size: usize = 1024;
        let mut buffer: Vec<u8> = vec![0; buffer_size];
        let ptr_string: *mut c_char = buffer.as_mut_ptr() as *mut c_char;
        let len_string: i32 = buffer_size as i32;
        unsafe {
            (self.f_last_error)(ptr_thread, ptr_string, len_string);
            let c_str = CStr::from_ptr(ptr_string);

            c_str.to_string_lossy().to_string()
        }
    }

    /// Attaches the current thread to the GraalVM isolate.
    ///
    /// # Returns
    /// * `Result<*mut graal_isolatethread_t>` - Pointer to the attached thread or an error

    // We could create handles to detach threads when they are out of scope
    // but for this simple example it would be overkill.
    fn attach_tread(&self) -> Result<*mut graal_isolatethread_t> {
        unsafe {
            let mut ptr_thread: *mut graal_isolatethread_t =
                std::mem::MaybeUninit::zeroed().assume_init();

            if (self.f_attach_thread)(self.ptr_isolate, &mut ptr_thread) == 0 {
                Ok(ptr_thread)
            } else {
                Err(ArrowError::CDataInterface("can't attach thread".into()))
            }
        }
    }
    /// Detaches a thread from the GraalVM isolate.
    ///
    /// # Arguments
    /// * `ptr_thread` - Pointer to the thread to detach
    ///
    /// # Returns
    /// * `Result<()>` - Success or error status

    fn detach_thread(&self, ptr_thread: *mut graal_isolatethread_t) -> Result<()> {
        unsafe {
            if (self.f_detach_thread)(ptr_thread) == 0 {
                Ok(())
            } else {
                Err(ArrowError::CDataInterface("can't detach thread".into()))
            }
        }
    }
}

// we want to prevent stream reader to outlive
// isolate from which was created
pub struct LocalArrowArrayStreamReader<'local> {
    inner: ArrowArrayStreamReader,
    pd: std::marker::PhantomData<&'local ArrowArrayStreamReader>,
}

impl<'local> Iterator for LocalArrowArrayStreamReader<'local> {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'local> RecordBatchReader for LocalArrowArrayStreamReader<'local> {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
/// # Safety
///
/// Note about safety, this drop can block if there are
/// threads attached to isolate
impl Drop for GraalArrowStreamer {
    fn drop(&mut self) {
        unsafe {
            // attaching thread to interact with graal
            // it is needed to call `f_tear_down_isolate`
            let mut ptr_thread: *mut graal_isolatethread_t =
                std::mem::MaybeUninit::zeroed().assume_init();

            if (self.f_attach_thread)(self.ptr_isolate, &mut ptr_thread) == 0 {
                (self.f_tear_down_isolate)(ptr_thread);
            }
        }
    }
}

#[cfg(test)]
mod test {

    use crate::GraalArrowStreamer;
    use arrow::array::RecordBatchReader;

    #[test]
    fn should_call_stream() -> arrow::error::Result<()> {
        let streamer = GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java")?;
        let mut stream = streamer.create_reader("path")?;

        assert!(stream.next().is_some());
        assert!(stream.next().is_some());
        assert!(stream.next().is_some());
        assert!(stream.next().is_some());

        let schema = stream.schema();

        assert!(schema.field_with_name("age").is_ok());

        Ok(())
    }

    #[test]
    fn should_call_stream_leak_memory_0() -> arrow::error::Result<()> {
        let streamer = GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java")?;
        let mut stream = streamer.create_reader("path")?;

        let batch = stream.next();

        //
        // test that checks if dropping stream will not make memory leak.
        //
        drop(stream);

        drop(batch);

        Ok(())
    }

    #[test]
    #[ignore = "if streamer is dropped before last batch it will `SIGSEGV` randomly (when) batch is dropped"]
    fn should_call_stream_leak_memory_1() -> arrow::error::Result<()> {
        let streamer = GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java")?;
        let mut stream = streamer.create_reader("path")?;

        let batch = stream.next();

        // SIGSEGV if streamer is dropped before last
        // batch is dropped
        drop(streamer);

        // batch drop will call release which should trigger
        // JNI call. JNI call will SIGSEGV as there is no
        // Active Isolate (JVM instance)
        drop(batch);

        Ok(())
    }

    #[test]
    #[should_panic = "java.lang.RuntimeException: you've made mock reader panic!"]
    fn should_handle_last_error_message() {
        let streamer =
            GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java").unwrap();
        // "panic" - as a path should produce create_reader to return error.
        //           we check here if the message is going to be returned
        let _ = streamer.create_reader("panic").unwrap();
    }

    #[test]
    fn should_not_close_allocator() -> arrow::error::Result<()> {
        let streamer = GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java")?;

        let mut stream = streamer.create_reader("path0")?;
        assert!(stream.next().is_some());
        drop(stream);

        let mut stream = streamer.create_reader("path1")?;
        assert!(stream.next().is_some());
        drop(stream);

        drop(streamer);

        Ok(())
    }
}

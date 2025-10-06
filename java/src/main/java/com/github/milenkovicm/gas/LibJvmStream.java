package com.github.milenkovicm.gas;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LibJvmStream provides a native interface for streaming Apache Arrow data from Java/GraalVM to Rust.
 * This class implements the necessary entry points for the Arrow C Data Interface, allowing seamless
 * integration between GraalVM native code and Rust applications.
 *
 * @see org.apache.arrow.c.ArrowArrayStream
 * @see org.apache.arrow.memory.BufferAllocator
 */
class LibJvmStream {

    /** Root allocator for Arrow memory management */
    static final BufferAllocator allocator = new RootAllocator();
    static final Logger logger = LoggerFactory.getLogger(LibJvmStream.class);

    /** Thread-local storage for error messages  */
    // fingers crossed :) this is not going to leak memory
    // with native code interaction
    static ThreadLocal<String> lastError = ThreadLocal.withInitial(() -> "Success");

    /**
     * Native entry point for creating an Arrow stream reader.
     * This method is called from Rust to create a new Arrow stream for reading data.
     *
     * @param _thread GraalVM isolate thread context
     * @param cPath path where to load data from (just for showcase)
     * @param addressOutputStream Memory address where the Arrow stream should be created
     * @return 0 on success, -1 on failure (with error message set in lastError)
     */
    @CEntryPoint(name = "gas_reader_stream")
    private static int cReaderStream(IsolateThread _thread, CCharPointer cPath, long addressOutputStream) {
        try {
            String path = CTypeConversion.toJavaString(cPath);
            readStream(path, addressOutputStream);
            return 0;
        } catch (Exception e) {
            var message = e.toString();
            lastError.set(message);
            logger.error("", e);
            return -1;
        }
    }

    /**
     * Native entry point for retrieving the last error message.
     * Writes the last error message into a buffer allocated by the Rust side.
     *
     * @param _thread GraalVM isolate thread context
     * @param resultBuffer Native pointer to the buffer where the error message should be written
     * @param bufferSize Size of the provided buffer
     * @return 0 on success, -1 if buffer size is insufficient
     */
    @CEntryPoint(name = "gas_last_error")
    private static int cLastError(IsolateThread _thread,
                               CCharPointer resultBuffer,
                               int bufferSize) {
        if (bufferSize < 1) {
            return -1;
        }

        CTypeConversion.toCString(lastError.get(), resultBuffer, WordFactory.unsigned(bufferSize));
        // should we return number of bytes written
        return  0;
    }

    @Deprecated
    @CEntryPoint(name = "gas_dummy_function")
    private static int myFunction(IsolateThread _thread, int a, int b) {
        return a + b;
    }

    /**
     * Creates an Arrow stream at the specified memory address using the provided path.
     * This method sets up the Arrow stream and exports it to the provided memory location
     * for consumption by the Rust side.
     *
     * <p>Note on memory management: While it's possible to use a child allocator that outlives
     * the reader (see commented code), the current implementation uses the root allocator
     * directly. When implementing with real data sources, consider using child allocators
     * for better memory management.</p>
     *
     * @param path Source path or identifier for the data stream
     * @param addressOutputStream Memory address where the Arrow stream should be created
     * @throws RuntimeException if path is "panic" (used for testing error propagation)
     */
    public static void readStream(String path, long addressOutputStream) {
        logger.debug("readStream () - START ... {}", path);
        // we make a case to test if error can be propagated
        // across C boundary
        if ("panic".equalsIgnoreCase(path)) {
            throw new RuntimeException("you've made mock reader panic!");
        }

        try (ArrowArrayStream outputStream = ArrowArrayStream.wrap(addressOutputStream)) {
            //
            // reader can be dropped before last batch is consumed,
            // so we use child allocator as it will outlive the child allocator
            //
            // ```
            // var childAllocator = allocator.newChildAllocator("readStream", 0, 16 * 1024 * 1024 );
            // var reader = new MockReader(childAllocator);
            // ```

            // MockReader should be replaced with actual ArrowReader
            var reader = new MockReader(allocator);
            Data.exportArrayStream(allocator, reader, outputStream);
            logger.debug("readStream () - DONE");
        }
    }
}

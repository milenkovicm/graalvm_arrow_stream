# ⛽️ GraalVM Arrow Stream (GAS)

This is a showcase project integrating [GraalVM] generated native shared library with a rustlang based application using [Arrow C Interface], enabling efficient streaming of [Apache Arrow] record batches created by [GraalVM] native library.

## ℹ️ Overview

> [!WARNING]
> A Arrow Java version 19+ is required, or version patched with [Apache Arrow Java Issue #866](https://github.com/apache/arrow-java/issues/866) is required
>

`GraalArrowStream` bridges the gap between [GraalVM] native libraries and Rust applications by implementing the [Arrow C Interface]. This allows for efficient streaming of [Apache Arrow] record batches while maintaining proper memory management and thread safety.

## ✨ Features & Limitations

- ✅ Streams [Apache Arrow] record batches from [GraalVM] native shared library with zero copy.
- ✅ Simpler deployment than JVM based solution
- ❌ Community edition GraalVM [supports only "Serial GC"](https://www.graalvm.org/latest/reference-manual/native-image/optimizations-and-performance/MemoryManagement/) which may not be the best option in all cases.

## 📝 Requirements

- Tested with [GraalVM] 25 (CE)
- Arrow Java 19 or patched version, see [Apache Arrow Java Issue #866](https://github.com/apache/arrow-java/issues/866) for details.

## 🛠️ Usage

### 💡 Basic Example

```rust
use graalvm_arrow_stream::GraalArrowStreamer;

fn main() -> arrow::error::Result<()> {
    // Create a streamer instance from a library
    let streamer = GraalArrowStreamer::try_new_from_name_and_path("gas", "./target/java")?;

    // Create a reader for your data source
    let mut reader = streamer.create_reader("path/to/data")?;

    // Iterate through record batches
    while let Some(batch) = reader.next() {
        match batch {
            Ok(data) => {
                // Process your data batch
                println!("Received batch with {} rows", data.num_rows());
            }
            Err(e) => eprintln!("Error reading batch: {}", e),
        }
    }

    Ok(())
}
```

### ➡️ Loading Libraries

The library provides multiple ways to load your GraalVM native library:

```rust
// Load by name (platform-specific prefix/suffix will be added automatically)
// Should consider LDLD_LIBRARY_PATH
let streamer = GraalArrowStreamer::try_new_from_name("my_lib")?;

// Load from specific file path
let streamer = GraalArrowStreamer::try_new_from_file("/path/to/libmy_lib.so")?;

// Load from name and path
let streamer = GraalArrowStreamer::try_new_from_name_and_path("my_lib", "./lib")?;
```

## ⚠️ Important Safety Notes

1. **Lifetime Management**: The `GraalArrowStreamer` instance must outlive any record batches created by its readers. Failing to maintain this lifetime ordering may result in segmentation faults (`SIGSEGV`).
This is current implementation limitation, as it was not trivial specifying this using lifetimes with current APIs.

```rust
// ❌ INCORRECT: Will cause SIGSEGV
let batch = {
    let streamer = GraalArrowStreamer::try_new_from_name("my_lib")?;
    let mut reader = streamer.create_reader("path")?;
    reader.next()
}; // streamer is dropped here while batch still lives

// ✅ CORRECT: Maintain proper lifetime
let streamer = GraalArrowStreamer::try_new_from_name("my_lib")?;
let mut reader = streamer.create_reader("path")?;
let batch = reader.next();
```

## ⚙️ Extending Library

Library extension can be achieved by replacing [MockReader.java](java/src/main/java/com/github/milenkovicm/gas/MockReader.java) with an actual implementation. This may involve [reconfiguring the build process](https://www.graalvm.org/latest/reference-manual/native-image/overview/Build-Overview/).

## ⚖️ License

MIT

## 🤝 Contributing

This is a showcase project and will not be actively maintained. Contributions are welcome!
Please feel free to submit a Pull Request.

## 🏗️ TODO

- [ ] Integrate java and rust loggers

[GraalVM]: https://www.graalvm.org/latest/docs/
[Arrow C Interface]: https://arrow.apache.org/docs/java/cdata.html
[Apache Arrow]: https://arrow.apache.org/docs/index.html

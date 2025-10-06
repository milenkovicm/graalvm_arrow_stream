use std::path::PathBuf;

fn main() {
    generate_ffi();
}

fn generate_ffi() {
    let header0 = "./target/java/graal_isolate_dynamic.h";
    let header1 = "./target/java/libgas_dynamic.h";

    println!("cargo:rerun-if-changed={}", header0);
    println!("cargo:rerun-if-changed={}", header1);

    let bindings = bindgen::Builder::default()
        .clang_arg("-I./target/java")
        .header(header0)
        .header(header1)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .derive_copy(false)
        .derive_debug(false)
        .generate()
        .expect("Unable to generate bindings!");

    // let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let out_path = PathBuf::from("src/");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

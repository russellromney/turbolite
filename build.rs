fn main() {
    if std::env::var("CARGO_FEATURE_LOADABLE_EXTENSION").is_ok() {
        // Loadable extension mode: compile the C shim that provides the
        // sqlite3_turbolite_init entry point and routes sqlite3_vfs_register
        // etc. through the extension API table.
        //
        // Do NOT link libsqlite3 — the host process provides SQLite.
        // The C shim's symbol implementations satisfy sqlite-vfs's extern "C".
        let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.1.0".into());

        // Use vendored SQLite headers — the macOS SDK headers define
        // SQLITE_OMIT_LOAD_EXTENSION which makes SQLITE_EXTENSION_INIT1
        // a no-op, breaking the extension entry point.
        cc::Build::new()
            .file("src/ext_entry.c")
            .include("vendor/sqlite3")
            .define(
                "TURBOLITE_VERSION",
                Some(format!("\"{}\"", version).as_str()),
            )
            .warnings(true)
            .compile("ext_entry");

        // Force the entry point symbol to be exported — the linker would
        // otherwise strip it because no Rust code references it.
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        if target_os == "macos" {
            // -exported_symbol prevents dead-stripping of the entry point.
            // -undefined dynamic_lookup lets sqlite3_create_function_v2 etc.
            // resolve at dlopen time from the host process.
            println!("cargo:rustc-cdylib-link-arg=-Wl,-exported_symbol,_sqlite3_turbolite_init");
            println!("cargo:rustc-cdylib-link-arg=-undefined");
            println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
        } else {
            // Linux: --export-dynamic exports all symbols.
            println!("cargo:rustc-cdylib-link-arg=-Wl,--export-dynamic");
        }

        println!("cargo:rerun-if-changed=src/ext_entry.c");
    } else {
        // Normal mode: link SQLite for the cdylib (FFI shared library) and bins.
        //
        // `sqlite-vfs` references sqlite3_vfs_register, sqlite3_snprintf, etc.
        // directly via extern "C" blocks.
        //
        // Without bundled-sqlite: link system libsqlite3.
        // With bundled-sqlite: libsqlite3-sys compiles SQLite from source.
        //   Its `cargo:rustc-link-lib=static=sqlite3` should propagate, but
        //   for cdylib targets we emit the instructions explicitly as a safety net.
        if std::env::var("CARGO_FEATURE_BUNDLED_SQLITE").is_ok() {
            if let Ok(lib_dir) = std::env::var("DEP_SQLITE3_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", lib_dir);
                println!("cargo:rustc-link-lib=static=sqlite3");
            }
        } else {
            println!("cargo:rustc-link-lib=sqlite3");
        }
    }
}

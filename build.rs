fn main() {
    // The cdylib target needs all symbols resolved at link time.
    // `sqlite-vfs` references sqlite3_vfs_register, sqlite3_snprintf, etc.
    // directly via extern "C" blocks.
    //
    // Without bundled-sqlite: link system libsqlite3.
    // With bundled-sqlite: libsqlite3-sys compiles SQLite from source.
    //   Its `cargo:rustc-link-lib=static=sqlite3` should propagate, but
    //   for cdylib targets we emit the instructions explicitly as a safety net.
    if std::env::var("CARGO_FEATURE_BUNDLED_SQLITE").is_ok() {
        // DEP_SQLITE3_LINK_TARGET is set by libsqlite3-sys via its `links = "sqlite3"` key.
        // It tells us the -L search path where libsqlite3.a lives.
        if let Ok(lib_dir) = std::env::var("DEP_SQLITE3_LIB_DIR") {
            println!("cargo:rustc-link-search=native={}", lib_dir);
            println!("cargo:rustc-link-lib=static=sqlite3");
        }
    } else {
        println!("cargo:rustc-link-lib=sqlite3");
    }
}

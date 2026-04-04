//! turbolite CLI
//!
//! The legacy CompressedVfs management commands (info, compact, convert, encrypt,
//! decrypt, embed-dict, extract-dict) have been removed along with the CompressedVfs
//! format. Use TurboliteVfs local mode instead.
//!
//! This binary is a placeholder for future CLI commands operating on the
//! TurboliteVfs manifest + page group format.

fn main() {
    eprintln!("turbolite CLI: no commands implemented yet.");
    eprintln!("The legacy CompressedVfs commands were removed in Phase Unification-h.");
    std::process::exit(1);
}

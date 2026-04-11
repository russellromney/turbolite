//! turbolite CLI
//!
//! The legacy CompressedVfs management commands (info, compact, convert, encrypt,
//! decrypt, embed-dict, extract-dict) have been removed along with the CompressedVfs
//! format. Use TurboliteVfs local mode instead.
//!
//! This binary is a placeholder for future CLI commands operating on the
//! TurboliteVfs manifest + page group format.

use clap::{CommandFactory, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "turbolite", version, about = "turbolite CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Version,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Version) => {
            println!("turbolite {}", env!("CARGO_PKG_VERSION"));
        }
        None => {
            let mut command = Cli::command();
            command.print_help().expect("failed to print help");
            println!();
        }
    }
}

use std::path::PathBuf;

use clap::Parser;
use solari_transfers::{fast_paths::FastGraphVec, valinor::TransferGraph};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    valhalla_tiles: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = Args::parse();
    let transfer_graph = TransferGraph::<FastGraphVec>::new(&args.valhalla_tiles)?;
    transfer_graph.save_to_dir(args.output)?;
    Ok(())
}

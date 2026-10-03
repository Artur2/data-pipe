use std::env;
use std::fs;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/config.toml");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("config.toml");

    println!("starting copy");
    fs::copy("src/config.toml", dest_path).unwrap();

    println!(
        "cargo::warning=The output directory is for config.toml is: {}",
        out_dir
    );
}
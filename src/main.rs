use std::process;

mod repl;
pub mod rules;
mod seed;
mod udf;

#[tokio::main]
async fn main() {
    let Some(arg2) = std::env::args().nth(1) else {
        eprintln!("Specify either `seed` or `repl`");
        process::exit(1)
    };

    match arg2.as_str() {
        "seed" => seed::seed().await,
        "repl" => repl::repl().await,
        _ => {
            eprintln!("Specify either `seed` or `repl`");
            process::exit(1)
        }
    }
}

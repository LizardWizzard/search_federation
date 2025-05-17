use std::process;

use search_federation::{repl, seed};

#[tokio::main]
async fn main() {
    let Some(arg2) = std::env::args().nth(1) else {
        eprintln!("Specify either `seed` or `repl`");
        process::exit(1)
    };

    match arg2.as_str() {
        "seed" => seed::seed().await,
        "repl" => {
            let ctx = search_federation::make_context().await;
            repl::repl(&ctx).await
        }
        _ => {
            eprintln!("Specify either `seed` or `repl`");
            process::exit(1)
        }
    }
}

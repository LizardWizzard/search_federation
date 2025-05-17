use datafusion::prelude::SessionContext;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

pub async fn repl(ctx: &SessionContext) {
    let mut rl = DefaultEditor::new().unwrap();
    if rl.load_history(".repl_history").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                let df = match ctx.sql(&line).await {
                    Ok(df) => df,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        continue;
                    }
                };

                match df.show().await {
                    Ok(df) => df,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        continue;
                    }
                };
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(".repl_history").unwrap();
}

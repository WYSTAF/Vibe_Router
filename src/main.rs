// Vibe Router — single-binary LLM router.
//
// Deliberately thin: argument parsing, tracing setup and dispatch live in
// `vibe-router-cli` so the binary is a library with a CLI skin and the whole stack can
// be driven in-process from integration tests.

fn main() {
    if let Err(err) = vibe_router_cli::run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

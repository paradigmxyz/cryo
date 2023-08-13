use std::process::Command;

fn main() {
    // Run `git describe --tags --always`
    let output =
        Command::new("git").args(["describe", "--tags", "--always"]).output().unwrap_or_else(|e| {
            panic!("Failed to execute git command: {}", e);
        });

    if output.status.success() {
        let git_description = String::from_utf8(output.stdout)
            .unwrap_or_else(|e| {
                panic!("Failed to read git command output: {}", e);
            })
            .trim()
            .to_string();

        println!("cargo:rustc-env=GIT_DESCRIPTION={}", git_description);
    } else {
        println!("cargo:warning=Could not determine git description");
    }
}

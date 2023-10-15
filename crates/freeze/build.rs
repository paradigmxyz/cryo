use std::process::Command;

fn main() {
    let git_description =
        get_git_description().unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());

    println!("cargo:rustc-env=GIT_DESCRIPTION={}", git_description);
}

fn get_git_description() -> Result<String, std::io::Error> {
    let output = Command::new("git").args(["describe", "--tags", "--always"]).output()?;

    if output.status.success() {
        let description = String::from_utf8(output.stdout)
            .expect("Failed to read git command output")
            .trim()
            .to_string();

        Ok(description)
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Git command failed"))
    }
}

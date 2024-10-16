use std::fs::{self, DirEntry, File};
use std::io::{self, Read, ErrorKind};
use std::path::{Path, PathBuf};
use std::fs::copy;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use tracing::{info, error, warn, Level};
use tracing_subscriber::FmtSubscriber;
use clap::Parser;
use rayon::prelude::*;

/// Command-line arguments structure
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The source directory (box folder)
    #[arg(short, long)]
    source: PathBuf,

    /// The target directory (one-drive folder)
    #[arg(short, long)]
    target: PathBuf,
}

// Function to get file size
fn get_file_size(entry: &DirEntry) -> Option<u64> {
    entry.metadata().ok().map(|metadata| metadata.len())
}

// Function to create directory structure in the target (one-drive) location
fn create_target_directory_structure(source: &Path, target: &Path, source_root: &Path) -> io::Result<PathBuf> {
    let relative_path = source.strip_prefix(source_root).unwrap_or(source).parent().unwrap_or(Path::new(""));
    let target_path = target.join(relative_path);
    
    if !target_path.exists() {
        fs::create_dir_all(&target_path)?;  // Create directories as needed
    }

    Ok(target_path)
}

// Function to fetch the file with retries to handle file locks during download
fn fetch_file_with_progress(entry: &DirEntry, multi_progress: Arc<MultiProgress>) -> io::Result<()> {
    let path = entry.path();
    let file_size = get_file_size(entry).unwrap_or(0);

    // Check if the file is a stub that needs to be downloaded
    if file_size == 0 {
        info!("Fetching stub file: {:?}", path);

        // Create a progress bar
        let pb = multi_progress.add(ProgressBar::new(file_size));
        pb.set_style(ProgressStyle::default_bar()
            .template("{wide_bar} {bytes}/{total_bytes} ({eta})")
            .progress_chars("##-"));

        // Retry loop to handle file locks
        let mut retries = 0;
        loop {
            match File::open(&path) {
                Ok(mut file) => {
                    let mut buffer = vec![0; 8192]; // Read in 8KB chunks
                    let mut total_read = 0;

                    // Read the file in chunks to show download progress
                    while let Ok(bytes_read) = file.read(&mut buffer) {
                        if bytes_read == 0 { break; } // End of file
                        total_read += bytes_read as u64;
                        pb.set_position(total_read);
                    }

                    pb.finish_with_message("Download complete");
                    return Ok(());
                }
                Err(e) => {
                    if e.kind() == ErrorKind::PermissionDenied || e.kind() == ErrorKind::WouldBlock {
                        // The file might still be locked due to ongoing download, so retry
                        if retries >= 5 {
                            error!("Failed to fetch file after multiple retries: {:?}", path);
                            return Err(io::Error::new(io::ErrorKind::Other, "File lock timeout"));
                        }
                        retries += 1;
                        warn!("File locked, retrying... (attempt {})", retries);
                        thread::sleep(Duration::from_secs(2)); // Wait before retrying
                    } else {
                        error!("Error opening file: {:?}", e);
                        return Err(e); // Propagate other errors
                    }
                }
            }
        }
    } else {
        // File is not a stub, no need to fetch
        Ok(())
    }
}

// Function to move file to the one-drive directory, preserving folder structure
fn move_file(entry: &DirEntry, target_root: &Path, source_root: &Path) -> io::Result<()> {
    let source_path = entry.path();

    // Create the target directory structure
    let target_dir = create_target_directory_structure(&source_path, &target_root, source_root)?;
    // Print the target directory path
    println!("Target directory: {:?}", target_dir);

    let target_path = target_dir.join(entry.file_name());

    info!("Moving file from {:?} to {:?}", source_path, target_path);

    // Check if the target file already exists
    if target_path.exists() {
        warn!("{:?} exists", entry.file_name());
        Ok(())
    } else {
        // Move the file
        match copy(source_path, target_path) {
            Ok(_) => {
                info!("Successfully moved file: {:?}", entry.file_name());
                Ok(())
            }
            Err(e) => {
                error!("Failed to move file {:?}: {:?}", entry.file_name(), e);
                Err(e)
            }
        }
    }
}

// Function to visit directories recursively and collect files and directories
fn visit_dirs(dir: &Path) -> io::Result<(Vec<DirEntry>, Vec<PathBuf>)> {
    let mut files = vec![];
    let mut dirs = vec![];

    if dir.is_dir() {
        let entries: Vec<_> = fs::read_dir(dir)?.collect::<Result<_, _>>()?;
        
        let (sub_files, sub_dirs): (Vec<_>, Vec<_>) = entries.into_par_iter()
            .filter_map(|entry| {
                let path = entry.path();
                if path.is_file() {
                    Some((Some(entry), None))
                } else if path.is_dir() {
                    let (sub_files, sub_dirs) = visit_dirs(&path).ok()?;
                    Some((None, Some((path, sub_files, sub_dirs))))
                } else {
                    None
                }
            })
            .unzip();

        files.extend(sub_files.into_iter().flatten());
        for (dir, sub_files, sub_dirs) in sub_dirs.into_iter().flatten() {
            dirs.push(dir);
            files.extend(sub_files);
            dirs.extend(sub_dirs);
        }
    } else {
        error!("{} is not a dir", dir.display());
    }

    // Remove duplicates
    files.par_sort_by_key(|a| a.path());
    files.dedup_by(|a, b| a.path() == b.path());

    dirs.par_sort_unstable();
    dirs.dedup();

    // Sort files by size (largest first)
    files.par_sort_by(|a, b| get_file_size(b).cmp(&get_file_size(a)));

    Ok((files, dirs))
}

fn main() -> io::Result<()> {
    // Initialize the tracing subscriber for logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default subscriber failed");

    // Parse command-line arguments
    let args = Args::parse();

    let box_dir = args.source;
    let one_drive_dir = args.target;
    info!("Copying from {} to {}", box_dir.display(), one_drive_dir.display());

    // Get the files and directories from the box directory recursively
    let (files, dirs) = match visit_dirs(&box_dir) {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to read box directory: {:?}", e);
            return Err(e);
        }
    };

    // Create all directories in the target location
    dirs.par_iter().try_for_each(|dir| {
        let target_dir = create_target_directory_structure(&dir, &one_drive_dir, &box_dir)?;
        info!("Created directory: {:?}", target_dir);
        Ok::<(), io::Error>(())
    })?;

    // Create a MultiProgress instance
    let multi_progress = Arc::new(MultiProgress::new());

    // Iterate through the files, sorted by size
    files.par_iter()
        .map(|file| {
            let multi_progress = Arc::clone(&multi_progress);
            // Fetch the file with progress (this will trigger download if it's a stub)
            if let Err(e) = fetch_file_with_progress(&file, multi_progress) {
                error!("Failed to fetch file: {:?}", e);
                return Err(e);
            }

            // Move the file to the one-drive directory, preserving folder structure
            if let Err(e) = move_file(&file, &one_drive_dir, &box_dir) {
                error!("Failed to move file: {:?}", e);
                return Err(e);
            }

            Ok(())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}
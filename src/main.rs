use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{value_parser, Arg, ArgAction, Command};
use memmap2::Mmap;
use regex::{Regex, RegexBuilder};
use rayon::prelude::*;
use walkdir::WalkDir;
use ansi_term::Colour::{Red, Green};
use globset::{Glob, GlobSetBuilder};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

// Constants for configuration
const BUFFER_SIZE: usize = 8192;
const UNROLL_FACTOR: usize = 4;
const MAX_MMAP_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB

// Main function: Entry point of the program
fn main() -> io::Result<()> {
    // Parse command line arguments using clap
    let matches = Command::new("grepverse")
        .version("0.4.0")
        .author("Your Name")
        .about("A grep-like utility written in Rust")
        .arg(Arg::new("pattern")
            .help("The pattern to search for")
            .required(true)
            .index(1))
        .arg(Arg::new("path")
            .help("The file or directory to search in (use '-' for stdin)")
            .required(true)
            .index(2))
        .arg(Arg::new("regex")
            .short('r')
            .long("regex")
            .help("Use regex for pattern matching"))
        .arg(Arg::new("ignore-case")
            .short('i')
            .long("ignore-case")
            .help("Ignore case distinctions"))
        .arg(Arg::new("line-number")
            .short('n')
            .long("line-number")
            .help("Prefix each line of output with the line number within its input file"))
        .arg(Arg::new("count")
            .short('c')
            .long("count")
            .help("Print only a count of selected lines"))
        .arg(Arg::new("context")
            .short('C')
            .long("context")
            .value_parser(value_parser!(usize))
            .action(ArgAction::Set)
            .help("Print NUM lines of output context"))
        .arg(Arg::new("invert-match")
            .short('v')
            .long("invert-match")
            .help("Invert the sense of matching, to select non-matching lines"))
        .arg(Arg::new("recursive")
            .short('R')
            .long("recursive")
            .help("Read all files under each directory, recursively"))
        .arg(Arg::new("color")
            .long("color")
            .action(ArgAction::Set)
            .value_parser(["always", "auto", "never"])
            .default_value("auto")
            .help("Use markers to highlight the matching strings"))
        .arg(Arg::new("include")
            .long("include")
            .action(ArgAction::Append)            
            .num_args(1..)            
            .help("Search only files that match the given glob pattern"))
        .arg(Arg::new("exclude")
            .long("exclude")
            .action(ArgAction::Set)
            .num_args(1..)  
            .help("Skip files that match the given glob pattern"))
        .arg(Arg::new("word-regexp")
            .short('w')
            .long("word-regexp")
            .help("Match only whole words"))
        .arg(Arg::new("fixed-strings")
            .short('F')
            .long("fixed-strings")
            .help("Interpret pattern as a fixed string, not a regular expression"))
        .get_matches();

    // Extract command line arguments
    let pattern = matches.get_one::<String>("pattern").unwrap();
    let path = matches.get_one::<String>("path").unwrap();
    let use_regex = matches.contains_id("regex");
    let ignore_case = matches.contains_id("ignore-case");
    let show_line_numbers = matches.contains_id("line-number");
    let count_only = matches.contains_id("count");
    let context_lines: usize = *matches.get_one("context").unwrap_or(&0);
    let invert_match = matches.contains_id("invert-match");
    let recursive = matches.contains_id("recursive");
    let color = match matches.get_one::<String>("color").unwrap().as_str() {
        "always" => true,
        "never" => false,
        "auto" => atty::is(atty::Stream::Stdout),
        _ => unreachable!(),
    };
    let word_regexp = matches.contains_id("word-regexp");
    let fixed_strings = matches.contains_id("fixed-strings");
 
    // Create include and exclude glob sets
    
    let include_globs = matches.get_many::<String>("include").unwrap_or_default().map(|v| v.as_str()).collect::<Vec<_>>();
    let exclude_globs = matches.get_many::<String>("exclude").unwrap_or_default().map(|v| v.as_str()).collect::<Vec<_>>();
    let include_globset = create_globset(&include_globs)?;
    let exclude_globset = create_globset(&exclude_globs)?;

    // Create the matcher function based on the command line options
    let matcher = create_matcher(pattern.as_str(), use_regex, ignore_case, word_regexp, fixed_strings, invert_match)?;

    // Handle stdin or file/directory search
    if path == "-" {
        // Search stdin
        let stdin = io::stdin();
        let reader = stdin.lock();
        search_reader(reader, matcher.as_ref(), show_line_numbers, context_lines, color)?;
    } else {
        // Search files or directories
        let paths = if recursive {
            get_file_paths(path, &include_globset, &exclude_globset)?
        } else {
            vec![PathBuf::from(path)]
        };

        let mut total_matches = 0;

        for file_path in paths {
            if file_path.is_file() && should_process_file(&file_path, &include_globset, &exclude_globset) {
                let file_matches = search_file(&file_path, matcher.as_ref(), show_line_numbers, context_lines, color)?;
                total_matches += file_matches.len();

                if !count_only {
                    for (line_number, line, is_match) in file_matches {
                        print_match(&file_path, line_number, &line, is_match, show_line_numbers, color);
                    }
                }
            }
        }

        if count_only {
            println!("{}", total_matches);
        }
    }

    Ok(())
}

// Function to create a GlobSet from a list of glob patterns
fn create_globset(patterns: &[&str]) -> io::Result<globset::GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?);
    }
    builder.build().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

// Function to check if a file should be processed based on include and exclude globs
fn should_process_file(file_path: &Path, include_globset: &globset::GlobSet, exclude_globset: &globset::GlobSet) -> bool {
    let file_name = file_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    (include_globset.is_empty() || include_globset.is_match(file_name)) && !exclude_globset.is_match(file_name)
}

// Function to create the matcher function based on command line options
fn create_matcher(
    pattern: &str,
    use_regex: bool,
    ignore_case: bool,
    word_regexp: bool,
    fixed_strings: bool,
    invert_match: bool,
) -> io::Result<Arc<dyn Fn(&str) -> bool + Send + Sync>> {
    if use_regex || word_regexp {
        let mut regex_builder = RegexBuilder::new(pattern);
        regex_builder.case_insensitive(ignore_case);
        // if word_regexp {
        //     regex_builder.word_boundary(true);
        // }
        let regex = regex_builder.build().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(Arc::new(move |line| regex.is_match(line) != invert_match))
    } else if fixed_strings {
        let pattern = if ignore_case { pattern.to_lowercase() } else { pattern.to_string() };
        Ok(Arc::new(move |line| {
            let line = if ignore_case { line.to_lowercase() } else { line.to_string() };
            line.contains(&pattern) != invert_match
        }))
    } else {
        let pattern = if ignore_case { pattern.to_lowercase() } else { pattern.to_string() };
        Ok(Arc::new(move |line| {
            let line = if ignore_case { line.to_lowercase() } else { line.to_string() };
            line.contains(&pattern) != invert_match
        }))
    }
}

// Function to recursively get file paths
fn get_file_paths(
    path: &str,
    include_globset: &globset::GlobSet,
    exclude_globset: &globset::GlobSet,
) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() && should_process_file(entry.path(), include_globset, exclude_globset) {
            paths.push(entry.path().to_path_buf());
        }
    }
    Ok(paths)
}

// Function to search a file
fn search_file<F>(
    file_path: &Path,
    matcher: &F,
    show_line_numbers: bool,
    context_lines: usize,
    color: bool,
) -> io::Result<Vec<(usize, String, bool)>>
where
    F: Fn(&str) -> bool + Send + Sync + ?Sized,
{
    let file = File::open(file_path)?;
    let file_size = get_file_size(&file)?;

    if file_size > MAX_MMAP_SIZE {
        search_file_buffered(&file, matcher, show_line_numbers, context_lines)
    } else {
        let mmap = unsafe { Mmap::map(&file)? };
        search_file_mmap(&mmap, matcher, show_line_numbers, context_lines)
    }
}

// Function to get file size (cross-platform)
#[cfg(unix)]
fn get_file_size(file: &File) -> io::Result<u64> {
    let metadata = file.metadata()?;
    Ok(metadata.size())
}

#[cfg(windows)]
fn get_file_size(file: &File) -> io::Result<u64> {
    let metadata = file.metadata()?;
    Ok(metadata.file_size())
}

// Function to search a file using buffered reading
fn search_file_buffered<F>(
    file: &File,
    matcher: &F,
    show_line_numbers: bool,
    context_lines: usize,
) -> io::Result<Vec<(usize, String, bool)>>
where
    F: Fn(&str) -> bool + Send + Sync + ?Sized
{
    let reader = BufReader::new(file);
    let mut results = Vec::new();
    let mut line_number = 0;
    let mut context_buffer = Vec::with_capacity(context_lines * 2 + 1);

    for line in reader.lines() {
        line_number += 1;
        let line = line?;
        let is_match = matcher(&line);

        if is_match || !context_buffer.is_empty() {
            context_buffer.push((line_number, line, is_match));

            if context_buffer.len() > context_lines * 2 + 1 {
                let (old_line_number, old_line, old_is_match) = context_buffer.remove(0);
                if old_is_match {
                    results.extend(context_buffer.iter().cloned());
                    context_buffer.clear();
                }
            }

            if is_match && context_buffer.len() == context_lines + 1 {
                results.extend(context_buffer.iter().cloned());
                context_buffer.clear();
            }
        }
    }

    if !context_buffer.is_empty() {
        results.extend(context_buffer.iter().cloned());
    }

    Ok(results)
}

// Function to search a file using memory mapping
fn search_file_mmap<F>(
    mmap: &Mmap,
    matcher: &F,
    show_line_numbers: bool,
    context_lines: usize,
) -> io::Result<Vec<(usize, String, bool)>>
where
    F: Fn(&str) -> bool + Send + Sync + ?Sized,
{
    let chunk_size = mmap.len() / rayon::current_num_threads().max(1);
    let results: Vec<_> = mmap.par_chunks(chunk_size)
        .map(|chunk| search_chunk(chunk, matcher, show_line_numbers, context_lines))
        .collect();

    Ok(results.into_iter().flatten().collect())
}

// Function to search a chunk of data
fn search_chunk<F>(
    chunk: &[u8],
    matcher: &F,
    show_line_numbers: bool,
    context_lines: usize,
) -> Vec<(usize, String, bool)>
where
    F: Fn(&str) -> bool + Send + Sync + ?Sized,
{
    let mut results = Vec::new();
    let mut line_number = 1;
    let mut line_start = 0;
    let mut context_buffer = Vec::with_capacity(context_lines * 2 + 1);

    for (i, &b) in chunk.iter().enumerate() {
        if b == b'\n' {
            if let Ok(line) = std::str::from_utf8(&chunk[line_start..i]) {
                let is_match = matcher(line);

                if is_match || !context_buffer.is_empty() {
                    context_buffer.push((line_number, line.to_string(), is_match));

                    if context_buffer.len() > context_lines * 2 + 1 {
                        let (old_line_number, old_line, old_is_match) = context_buffer.remove(0);
                        if old_is_match {
                            results.extend(context_buffer.iter().cloned());
                            context_buffer.clear();
                        }
                    }

                    if is_match && context_buffer.len() == context_lines + 1 {
                        results.extend(context_buffer.iter().cloned());
                        context_buffer.clear();
                    }
                }
            }
            line_number += 1;
            line_start = i + 1;
        }
    }

    if !context_buffer.is_empty() {
        results.extend(context_buffer.iter().cloned());
    }

    results
}

// Function to print a matching line with optional coloring and line numbers
fn print_match(file_path: &Path, line_number: usize, line: &str, is_match: bool, show_line_numbers: bool, use_color: bool) {
    let file_name = file_path.to_string_lossy();
    let line_num = if show_line_numbers {
        format!("{}:", line_number)
    } else {
        String::new()
    };

    let output = if use_color {
        if is_match {
            format!("{}:{}:{}", Green.paint(file_name), Green.paint(line_num), Red.paint(line))
        } else {
            format!("{}:{}{}", Green.paint(file_name), Green.paint(line_num), line)
        }
    } else {
        format!("{}:{}{}", file_name, line_num, line)
    };

    println!("{}", output);
}

// Function to search a reader (e.g., stdin)
fn search_reader<R, F>(
    mut reader: R,
    matcher: &F,
    show_line_numbers: bool,
    context_lines: usize,
    color: bool,
) -> io::Result<()>
where
    R: BufRead,
    F: Fn(&str) -> bool + Send + Sync + ?Sized,
{
    let mut line_number = 0;
    let mut context_buffer = Vec::with_capacity(context_lines * 2 + 1);
    let mut line = String::new();

    while reader.read_line(&mut line)? > 0 {
        line_number += 1;
        let is_match = matcher(&line);

        if is_match || !context_buffer.is_empty() {
            context_buffer.push((line_number, line.clone(), is_match));

            if context_buffer.len() > context_lines * 2 + 1 {
                let (old_line_number, old_line, old_is_match) = context_buffer.remove(0);
                if old_is_match {
                    for (num, content, matched) in &context_buffer {
                        print_match(Path::new("stdin"), *num, content, *matched, show_line_numbers, color);
                    }
                    context_buffer.clear();
                }
            }

            if is_match && context_buffer.len() == context_lines + 1 {
                for (num, content, matched) in &context_buffer {
                    print_match(Path::new("stdin"), *num, content, *matched, show_line_numbers, color);
                }
                context_buffer.clear();
            }
        }

        line.clear();
    }

    if !context_buffer.is_empty() {
        for (num, content, matched) in &context_buffer {
            print_match(Path::new("stdin"), *num, content, *matched, show_line_numbers, color);
        }
    }

    Ok(())
}

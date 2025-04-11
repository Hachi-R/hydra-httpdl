use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, ClientBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(name = "hydra-httpdl")]
#[command(author = "los-broxas")]
#[command(version = "0.1.2")]
#[command(about = "high speed and low resource usage http downloader", long_about = None)]
struct CliArgs {
    /// file url
    #[arg(required = true)]
    url: String,

    /// output file path
    #[arg(default_value = "output.bin")]
    output: String,

    /// number of concurrent connections
    #[arg(short = 'c', long, default_value = "16")]
    connections: usize,

    /// chunk size in MB
    #[arg(short = 'k', long, default_value = "5")]
    chunk_size: usize,

    /// buffer size in MB
    #[arg(short, long, default_value = "16")]
    buffer_size: usize,

    #[arg(short = 'v', long, default_value = "false")]
    verbose: bool,

    #[arg(short = 's', long, default_value = "false")]
    silent: bool,

    /// force download from beginning
    #[arg(short = 'f', long, default_value = "false")]
    force: bool,

    #[arg(short = 't', long, default_value = "30")]
    timeout: u64,

    #[arg(long, default_value = "5")]
    max_retries: usize,

    /// output progress as JSON (compatible with silent mode)
    #[arg(short = 'j', long, default_value = "false")]
    json_output: bool,
}

struct DownloadConfig {
    url: String,
    output_path: String,
    num_connections: usize,
    chunk_size: usize,
    buffer_size: usize,
    verbose: bool,
    silent: bool,
    force: bool,
    timeout: Duration,
    max_retries: usize,
    json_output: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct DownloadMetadata {
    url: String,
    file_size: u64,
    completed_chunks: HashSet<u64>,
    last_modified: chrono::DateTime<chrono::Utc>,
    version: u8,
}

#[derive(Serialize, Debug)]
struct ProgressInfo {
    folder_name: String,
    file_name: String,
    file_size: u64,
    progress: f64,
    download_speed: f64,
    bytes_downloaded: u64,
}

impl DownloadMetadata {
    fn new(url: String, file_size: u64) -> Self {
        Self {
            url,
            file_size,
            completed_chunks: HashSet::new(),
            last_modified: chrono::Utc::now(),
            version: 1,
        }
    }

    fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string(self)?;
        let temp_path = format!("{}.tmp", path.as_ref().to_string_lossy());
        fs::write(&temp_path, &json)
            .with_context(|| format!("Failed to write temporary metadata file {}", temp_path))?;
        fs::rename(&temp_path, &path).with_context(|| {
            format!(
                "Failed to rename metadata file from {} to {}",
                temp_path,
                path.as_ref().to_string_lossy()
            )
        })?;
        Ok(())
    }

    fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path).with_context(|| {
            format!(
                "Failed to read metadata file {}",
                path.as_ref().to_string_lossy()
            )
        })?;
        let metadata: DownloadMetadata =
            serde_json::from_str(&content).with_context(|| "Failed to parse metadata JSON")?;
        Ok(metadata)
    }

    fn mark_chunk_complete(&mut self, chunk_id: u64) -> Result<()> {
        self.completed_chunks.insert(chunk_id);
        self.last_modified = chrono::Utc::now();
        Ok(())
    }

    fn is_chunk_complete(&self, chunk_id: u64) -> bool {
        self.completed_chunks.contains(&chunk_id)
    }
}

struct Downloader {
    client: Client,
    config: DownloadConfig,
}

impl Downloader {
    fn new(config: DownloadConfig) -> Result<Self> {
        let client = ClientBuilder::new()
            .timeout(config.timeout)
            .connect_timeout(Duration::from_secs(10))
            .user_agent("hydra-httpdl/0.1.2")
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self { client, config })
    }

    async fn download(&self) -> Result<()> {
        let (file_size, filename) = self.get_file_info().await?;

        let output_path = if Path::new(&self.config.output_path)
            .file_name()
            .unwrap_or_default()
            == "output.bin"
            && filename.is_some()
        {
            filename.unwrap()
        } else {
            self.config.output_path.clone()
        };

        if let Some(parent) = Path::new(&output_path).parent() {
            if parent.to_string_lossy() != "" && !parent.exists() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create output directory {}", parent.display())
                })?;
            }
        }

        if self.config.verbose && !self.config.silent {
            println!("Detected filename: {}", output_path);
        }

        // Extract folder and file name for JSON output
        let path = Path::new(&output_path);
        let folder_name = path
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or(".")
            .to_string();
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        let metadata_path = format!("{}.meta", output_path);
        let metadata = if Path::new(&metadata_path).exists()
            && Path::new(&output_path).exists()
            && !self.config.force
        {
            if self.config.verbose && !self.config.silent {
                println!("Found existing download metadata, attempting to resume");
            }

            match DownloadMetadata::load_from_file(&metadata_path) {
                Ok(md) => {
                    if md.url == self.config.url && md.file_size == file_size {
                        md
                    } else {
                        if self.config.verbose && !self.config.silent {
                            println!("Metadata mismatch, starting new download");
                        }
                        DownloadMetadata::new(self.config.url.clone(), file_size)
                    }
                }
                Err(e) => {
                    if self.config.verbose && !self.config.silent {
                        println!("Failed to load metadata: {}", e);
                    }
                    DownloadMetadata::new(self.config.url.clone(), file_size)
                }
            }
        } else {
            if self.config.force && Path::new(&output_path).exists() {
                if self.config.verbose && !self.config.silent {
                    println!("Force flag set, removing existing file");
                }
                if let Err(e) = fs::remove_file(&output_path) {
                    if self.config.verbose && !self.config.silent {
                        println!("Warning: Failed to remove existing file: {}", e);
                    }
                }
                if let Err(e) = fs::remove_file(&metadata_path) {
                    if self.config.verbose
                        && !self.config.silent
                        && Path::new(&metadata_path).exists()
                    {
                        println!("Warning: Failed to remove existing metadata: {}", e);
                    }
                }
            }
            DownloadMetadata::new(self.config.url.clone(), file_size)
        };

        let file = if Path::new(&output_path).exists() && !self.config.force {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&output_path)
                .with_context(|| format!("Failed to open output file {}", output_path))?;

            let actual_size = file
                .metadata()
                .with_context(|| "Failed to get file metadata")?
                .len();

            if actual_size != file_size {
                if self.config.verbose && !self.config.silent {
                    println!(
                        "File size mismatch: expected {}, got {}. Recreating file.",
                        file_size, actual_size
                    );
                }
                let file = File::create(&output_path)
                    .with_context(|| format!("Failed to create output file {}", output_path))?;
                file.set_len(file_size)
                    .with_context(|| format!("Failed to set file size to {}", file_size))?;
                file
            } else {
                file
            }
        } else {
            let file = File::create(&output_path)
                .with_context(|| format!("Failed to create output file {}", output_path))?;
            file.set_len(file_size)
                .with_context(|| format!("Failed to set file size to {}", file_size))?;
            file
        };

        let file = Arc::new(Mutex::new(BufWriter::with_capacity(
            self.config.buffer_size,
            file,
        )));

        let progress_bar = if !self.config.silent {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                    .context("Failed to set progress bar style")?
            );

            let completed_bytes = metadata
                .completed_chunks
                .iter()
                .map(|&chunk_id| {
                    let start = chunk_id * (self.config.chunk_size as u64);
                    let end = std::cmp::min(
                        (chunk_id + 1) * (self.config.chunk_size as u64) - 1,
                        file_size - 1,
                    );
                    end - start + 1
                })
                .sum();

            pb.set_position(completed_bytes);

            if completed_bytes > 0 {
                std::thread::sleep(std::time::Duration::from_millis(100));
                pb.set_message("Download resumed");
                pb.reset_eta();
            }

            Some(pb)
        } else {
            None
        };

        // Track download progress for JSON output
        let bytes_downloaded = Arc::new(Mutex::new(
            metadata
                .completed_chunks
                .iter()
                .map(|&chunk_id| {
                    let start = chunk_id * (self.config.chunk_size as u64);
                    let end = std::cmp::min(
                        (chunk_id + 1) * (self.config.chunk_size as u64) - 1,
                        file_size - 1,
                    );
                    end - start + 1
                })
                .sum(),
        ));

        // Setup JSON progress reporting if enabled
        let json_reporter_handle = if self.config.json_output {
            let folder_name_clone = folder_name.clone();
            let file_name_clone = file_name.clone();
            let bytes_downloaded_clone = Arc::clone(&bytes_downloaded);
            let file_size_clone = file_size;

            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                let mut last_bytes = 0u64;

                loop {
                    interval.tick().await;
                    let current_bytes = *bytes_downloaded_clone.lock().await;
                    let progress = current_bytes as f64 / file_size_clone as f64 * 100.0;

                    // Calcula a velocidade em bytes por segundo
                    let download_speed = (current_bytes - last_bytes) as f64; // Bytes/s
                    last_bytes = current_bytes;

                    let progress_info = ProgressInfo {
                        folder_name: folder_name_clone.clone(),
                        file_name: file_name_clone.clone(),
                        file_size: file_size_clone,
                        progress,
                        download_speed, // Agora em bytes/s
                        bytes_downloaded: current_bytes,
                    };

                    println!("{}", serde_json::to_string(&progress_info).unwrap());

                    if current_bytes >= file_size_clone {
                        break;
                    }
                }
            }))
        } else {
            None
        };

        let supports_range = self.check_range_support().await?;
        if !supports_range && self.config.num_connections > 1 {
            if !self.config.silent {
                println!(
                    "Server doesn't support range requests, falling back to single connection"
                );
            }
        }

        let chunk_size = self.config.chunk_size as u64;
        let mut chunks = Vec::new();

        for i in 0..=(file_size / chunk_size) {
            let chunk_id = i;
            if !metadata.is_chunk_complete(chunk_id) {
                chunks.push((
                    chunk_id,
                    i * chunk_size,
                    std::cmp::min((i + 1) * chunk_size - 1, file_size - 1),
                ));
            }
        }

        if chunks.is_empty() {
            if !self.config.silent {
                println!("All chunks already downloaded. Download complete!");
                if let Some(pb) = progress_bar {
                    pb.finish_with_message("Download complete");
                }
            }
            return Ok(());
        }

        let metadata = Arc::new(Mutex::new(metadata));
        let metadata_path = Arc::new(metadata_path);

        let num_connections = if supports_range {
            self.config.num_connections
        } else {
            1
        };

        // Create a new Arc for tracking downloaded bytes that will be updated during download
        let download_progress = Arc::clone(&bytes_downloaded);

        let mut tasks = FuturesUnordered::new();

        for (chunk_id, start, end) in chunks {
            let client = self.client.clone();
            let url = self.config.url.clone();
            let file_clone = Arc::clone(&file);
            let pb_clone = progress_bar.clone();
            let metadata_clone = Arc::clone(&metadata);
            let metadata_path_clone = Arc::clone(&metadata_path);
            let max_retries = self.config.max_retries;
            let progress_tracker = Arc::clone(&download_progress);

            tasks.push(tokio::spawn(async move {
                Self::download_chunk_with_retry(
                    client,
                    url,
                    chunk_id,
                    start,
                    end,
                    file_clone,
                    pb_clone,
                    metadata_clone,
                    metadata_path_clone,
                    max_retries,
                    supports_range,
                    progress_tracker,
                )
                .await
            }));

            if tasks.len() >= num_connections {
                if let Some(result) = tasks.next().await {
                    result??;
                }
            }
        }

        while let Some(result) = tasks.next().await {
            result??;
        }

        // Stop the JSON reporter if it was started
        if let Some(handle) = json_reporter_handle {
            handle.abort();
        }

        // Final progress update for JSON output
        if self.config.json_output {
            let progress_info = ProgressInfo {
                folder_name,
                file_name,
                file_size,
                progress: 100.0,
                download_speed: 0.0, // Download is complete
                bytes_downloaded: file_size,
            };
            println!("{}", serde_json::to_string(&progress_info).unwrap());
        }

        {
            let mut writer = file.lock().await;
            writer.flush().context("Failed to flush output file")?;
        }

        if let Some(pb) = progress_bar {
            pb.finish_with_message("Download complete");
        }

        Ok(())
    }

    async fn check_range_support(&self) -> Result<bool> {
        let resp = self
            .client
            .head(&self.config.url)
            .header("Range", "bytes=0-0")
            .send()
            .await
            .context("Failed to send HEAD request for range support check")?;

        let accepts_ranges = resp
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("bytes"))
            .unwrap_or(false);

        Ok(accepts_ranges || resp.status() == StatusCode::PARTIAL_CONTENT)
    }

    async fn download_chunk_with_retry(
        client: Client,
        url: String,
        chunk_id: u64,
        start: u64,
        end: u64,
        file: Arc<Mutex<BufWriter<File>>>,
        progress_bar: Option<ProgressBar>,
        metadata: Arc<Mutex<DownloadMetadata>>,
        metadata_path: Arc<String>,
        max_retries: usize,
        supports_range: bool,
        download_progress: Arc<Mutex<u64>>,
    ) -> Result<()> {
        let mut retries = 0;
        loop {
            match Self::download_chunk(
                client.clone(),
                url.clone(),
                chunk_id,
                start,
                end,
                file.clone(),
                progress_bar.clone(),
                metadata.clone(),
                metadata_path.clone(),
                supports_range,
                download_progress.clone(),
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(e.context(format!("Failed after {} retries", max_retries)));
                    }

                    let backoff = Duration::from_millis(500 * 2u64.pow(retries as u32 - 1));
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    async fn download_chunk(
        client: Client,
        url: String,
        chunk_id: u64,
        start: u64,
        end: u64,
        file: Arc<Mutex<BufWriter<File>>>,
        progress_bar: Option<ProgressBar>,
        metadata: Arc<Mutex<DownloadMetadata>>,
        metadata_path: Arc<String>,
        supports_range: bool,
        download_progress: Arc<Mutex<u64>>,
    ) -> Result<()> {
        let req = client.get(&url);

        let req = if supports_range {
            req.header("Range", format!("bytes={}-{}", start, end))
        } else {
            req
        };

        let resp = req.send().await?;

        if !supports_range && (resp.status() != StatusCode::OK) {
            anyhow::bail!("Server returned unexpected status code: {}", resp.status());
        } else if supports_range
            && (resp.status() != StatusCode::PARTIAL_CONTENT && resp.status() != StatusCode::OK)
        {
            anyhow::bail!(
                "Server returned unexpected status code for range request: {}",
                resp.status()
            );
        }

        let mut stream = resp.bytes_stream();
        let mut position = start;
        let mut total_bytes = 0;
        let expected_bytes = end - start + 1;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            let chunk_size = chunk.len() as u64;

            total_bytes += chunk_size;

            if !supports_range {
                if total_bytes <= start {
                    continue;
                }

                let chunk_start = if total_bytes - chunk_size < start {
                    (start - (total_bytes - chunk_size)) as usize
                } else {
                    0
                };

                let chunk_end = if total_bytes > end {
                    chunk_size as usize - (total_bytes - end - 1) as usize
                } else {
                    chunk_size as usize
                };

                if chunk_start >= chunk_end {
                    continue;
                }

                let mut writer = file.lock().await;
                writer.seek(SeekFrom::Start(position))?;
                writer.write_all(&chunk[chunk_start..chunk_end])?;
                drop(writer);

                let written = (chunk_end - chunk_start) as u64;
                position += written;

                // Update the JSON progress tracker
                {
                    let mut progress = download_progress.lock().await;
                    *progress += written;
                }

                if let Some(pb) = &progress_bar {
                    pb.inc(written);
                }

                if total_bytes > end {
                    break;
                }
            } else {
                if total_bytes > expected_bytes {
                    let remaining = expected_bytes - (total_bytes - chunk_size);
                    let mut writer = file.lock().await;
                    writer.seek(SeekFrom::Start(position))?;
                    writer.write_all(&chunk[..remaining as usize])?;

                    // Update the JSON progress tracker
                    {
                        let mut progress = download_progress.lock().await;
                        *progress += remaining;
                    }

                    if let Some(pb) = &progress_bar {
                        pb.inc(remaining);
                    }
                    break;
                }

                let mut writer = file.lock().await;
                writer.seek(SeekFrom::Start(position))?;
                writer.write_all(&chunk)?;
                drop(writer);

                position += chunk_size;

                // Update the JSON progress tracker
                {
                    let mut progress = download_progress.lock().await;
                    *progress += chunk_size;
                }

                if let Some(pb) = &progress_bar {
                    pb.inc(chunk_size);
                }
            }
        }

        {
            let mut md = metadata.lock().await;
            md.mark_chunk_complete(chunk_id)?;
            md.save_to_file(metadata_path.as_str())?;
        }

        Ok(())
    }

    async fn get_file_info(&self) -> Result<(u64, Option<String>)> {
        let resp = self
            .client
            .head(&self.config.url)
            .send()
            .await
            .with_context(|| format!("Failed to send HEAD request to {}", self.config.url))?;

        if !resp.status().is_success() {
            anyhow::bail!(
                "Server returned unsuccessful status code: {}",
                resp.status()
            );
        }

        let file_size = if let Some(content_length) = resp.headers().get("content-length") {
            content_length
                .to_str()
                .with_context(|| "Failed to parse content-length header")?
                .parse()
                .with_context(|| "Failed to parse content-length as integer")?
        } else {
            anyhow::bail!(
                "Could not determine file size - server did not send content-length header"
            )
        };

        let mut filename = None;

        if let Some(disposition) = resp.headers().get("content-disposition") {
            if let Ok(disposition_str) = disposition.to_str() {
                filename = Self::parse_content_disposition(disposition_str);
            }
        }

        if filename.is_none() {
            if let Ok(parsed_url) = Url::parse(&self.config.url) {
                let path = parsed_url.path();
                if let Some(path_filename) = Path::new(path).file_name() {
                    if let Some(filename_str) = path_filename.to_str() {
                        if !filename_str.is_empty() {
                            if let Ok(decoded) = urlencoding::decode(filename_str) {
                                let decoded_str = decoded.to_string();
                                if !decoded_str.is_empty() {
                                    filename = Some(decoded_str);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((file_size, filename))
    }

    fn parse_content_disposition(disposition: &str) -> Option<String> {
        if let Some(idx) = disposition.find("filename*=") {
            let start = idx + 10;
            let end = disposition[start..]
                .find(';')
                .map_or(disposition.len(), |i| start + i);
            let value = &disposition[start..end].trim();

            if value.starts_with('\'') && value.contains("''") {
                let parts: Vec<&str> = value.splitn(3, '\'').collect();
                if parts.len() >= 3 {
                    if let Ok(decoded) = urlencoding::decode(parts[2]) {
                        return Some(decoded.to_string());
                    }
                }
            }
        }

        if let Some(idx) = disposition.find("filename=") {
            let start = idx + 9;
            let value = &disposition[start..];

            if value.starts_with('"') {
                if let Some(end_quote) = value[1..].find('"') {
                    return Some(value[1..=end_quote].to_string());
                }
            } else {
                let end = value.find(';').map_or(value.len(), |i| i);
                let filename = value[..end].trim();
                if !filename.is_empty() {
                    return Some(filename.to_string());
                }
            }
        }

        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();

    let config = DownloadConfig {
        url: args.url.clone(),
        output_path: args.output,
        num_connections: args.connections,
        chunk_size: args.chunk_size * 1024 * 1024,
        buffer_size: args.buffer_size * 1024 * 1024,
        verbose: args.verbose,
        silent: args.silent,
        force: args.force,
        timeout: Duration::from_secs(args.timeout),
        max_retries: args.max_retries,
        json_output: args.json_output,
    };

    let downloader = Downloader::new(config)?;

    if downloader.config.verbose && !downloader.config.silent {
        println!(
            "Starting download with {} connections, chunk size: {}MB, buffer: {}MB",
            downloader.config.num_connections, args.chunk_size, args.buffer_size
        );
        println!("URL: {}", args.url);
    }

    match downloader.download().await {
        Ok(_) => Ok(()),
        Err(e) => {
            if !downloader.config.silent {
                eprintln!("Download failed: {}", e);
                if downloader.config.verbose {
                    eprintln!("Error details: {:?}", e);
                }
            }
            Err(e)
        }
    }
}

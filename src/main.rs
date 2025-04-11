use anyhow::Result;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, StatusCode, Url};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const DEFAULT_MAX_RETRIES: usize = 3;
const RETRY_BACKOFF_MS: u64 = 500;
const DEFAULT_OUTPUT_FILENAME: &str = "output.bin";
const DEFAULT_CONNECTIONS: usize = 16;
const DEFAULT_CHUNK_SIZE_MB: usize = 5;
const DEFAULT_BUFFER_SIZE_MB: usize = 8;
const DEFAULT_VERBOSE: bool = false;
const DEFAULT_SILENT: bool = false;

#[derive(Parser)]
#[command(name = "hydra-httpdl")]
#[command(author = "los-broxas")]
#[command(version = "0.1.3")]
#[command(about = "high speed and low resource usage http downloader", long_about = None)]
struct CliArgs {
    /// file url
    #[arg(required = true)]
    url: String,

    /// output file path
    #[arg(default_value = DEFAULT_OUTPUT_FILENAME)]
    output: String,

    /// number of concurrent connections
    #[arg(short = 'c', long, default_value_t = DEFAULT_CONNECTIONS)]
    connections: usize,

    /// chunk size in MB
    #[arg(short = 'k', long, default_value_t = DEFAULT_CHUNK_SIZE_MB)]
    chunk_size: usize,

    /// buffer size in MB
    #[arg(short, long, default_value_t = DEFAULT_BUFFER_SIZE_MB)]
    buffer_size: usize,

    #[arg(short = 'v', long, default_value_t = DEFAULT_VERBOSE)]
    verbose: bool,

    #[arg(short = 's', long, default_value_t = DEFAULT_SILENT)]
    silent: bool,
}

struct DownloadConfig {
    url: String,
    output_path: String,
    num_connections: usize,
    chunk_size: usize,
    buffer_size: usize,
    verbose: bool,
    silent: bool,
}

impl DownloadConfig {
    fn should_log(&self) -> bool {
        self.verbose && !self.silent
    }
}

struct ProgressTracker {
    bar: Option<ProgressBar>,
}

impl ProgressTracker {
    fn new(file_size: u64, silent: bool) -> Result<Self> {
        let bar = if !silent {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")?
            );
            Some(pb)
        } else {
            None
        };

        Ok(Self { bar })
    }

    fn increment(&self, amount: u64) {
        if let Some(pb) = &self.bar {
            pb.inc(amount);
        }
    }

    fn finish(&self) {
        if let Some(pb) = &self.bar {
            pb.finish_with_message("Download complete");
        }
    }
}

struct Downloader {
    client: Client,
    config: DownloadConfig,
}

impl Downloader {
    async fn download(&self) -> Result<()> {
        let (file_size, filename) = self.get_file_info().await?;
        let output_path = self.determine_output_path(filename);

        if self.config.should_log() {
            println!("Detected filename: {}", output_path);
        }

        let file = self.prepare_output_file(&output_path, file_size)?;
        let progress = ProgressTracker::new(file_size, self.config.silent)?;

        let chunks = self.calculate_chunks(file_size);
        self.process_chunks(chunks, file, file_size, progress)
            .await?;

        Ok(())
    }

    fn determine_output_path(&self, filename: Option<String>) -> String {
        if Path::new(&self.config.output_path)
            .file_name()
            .unwrap_or_default()
            == DEFAULT_OUTPUT_FILENAME
            && filename.is_some()
        {
            filename.unwrap()
        } else {
            self.config.output_path.clone()
        }
    }

    fn prepare_output_file(&self, path: &str, size: u64) -> Result<Arc<Mutex<BufWriter<File>>>> {
        let file = File::create(path)?;
        file.set_len(size)?;
        Ok(Arc::new(Mutex::new(BufWriter::with_capacity(
            self.config.buffer_size,
            file,
        ))))
    }

    fn calculate_chunks(&self, file_size: u64) -> Vec<(u64, u64)> {
        let chunk_size = self.config.chunk_size as u64;
        let mut chunks = Vec::new();

        for i in 0..=(file_size / chunk_size) {
            chunks.push((
                i * chunk_size,
                std::cmp::min((i + 1) * chunk_size - 1, file_size - 1),
            ));
        }

        chunks
    }

    async fn process_chunks(
        &self,
        chunks: Vec<(u64, u64)>,
        file: Arc<Mutex<BufWriter<File>>>,
        _file_size: u64,
        progress: ProgressTracker,
    ) -> Result<()> {
        let mut tasks = FuturesUnordered::new();

        for (start, end) in chunks {
            let client = self.client.clone();
            let url = self.config.url.clone();
            let file_clone = Arc::clone(&file);
            let pb_clone = progress.bar.clone();

            tasks.push(tokio::spawn(async move {
                Self::download_chunk_with_retry(
                    client,
                    url,
                    start,
                    end,
                    file_clone,
                    pb_clone,
                    DEFAULT_MAX_RETRIES,
                )
                .await
            }));

            if tasks.len() >= self.config.num_connections {
                if let Some(result) = tasks.next().await {
                    result??;
                }
            }
        }

        while let Some(result) = tasks.next().await {
            result??;
        }

        {
            let mut writer = file.lock().await;
            writer.flush()?;
        }

        progress.finish();

        Ok(())
    }

    async fn download_chunk_with_retry(
        client: Client,
        url: String,
        start: u64,
        end: u64,
        file: Arc<Mutex<BufWriter<File>>>,
        progress_bar: Option<ProgressBar>,
        max_retries: usize,
    ) -> Result<()> {
        let mut retries = 0;
        loop {
            match Self::download_chunk(
                client.clone(),
                url.clone(),
                start,
                end,
                file.clone(),
                progress_bar.clone(),
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        RETRY_BACKOFF_MS * retries as u64,
                    ))
                    .await;
                }
            }
        }
    }

    async fn download_chunk(
        client: Client,
        url: String,
        start: u64,
        end: u64,
        file: Arc<Mutex<BufWriter<File>>>,
        progress_bar: Option<ProgressBar>,
    ) -> Result<()> {
        let resp = client
            .get(&url)
            .header("Range", format!("bytes={}-{}", start, end))
            .send()
            .await?;

        if resp.status() != StatusCode::PARTIAL_CONTENT && resp.status() != StatusCode::OK {
            anyhow::bail!("Server does not support Range requests");
        }

        let mut stream = resp.bytes_stream();
        let mut position = start;
        let mut total_bytes = 0;
        let expected_bytes = end - start + 1;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            let chunk_size = chunk.len() as u64;

            total_bytes += chunk_size;
            if total_bytes > expected_bytes {
                let remaining = expected_bytes - (total_bytes - chunk_size);
                let mut writer = file.lock().await;
                writer.seek(SeekFrom::Start(position))?;
                writer.write_all(&chunk[..remaining as usize])?;

                let tracker = ProgressTracker {
                    bar: progress_bar.clone(),
                };
                tracker.increment(remaining);
                break;
            }

            let mut writer = file.lock().await;
            writer.seek(SeekFrom::Start(position))?;
            writer.write_all(&chunk)?;
            drop(writer);

            position += chunk_size;
            let tracker = ProgressTracker {
                bar: progress_bar.clone(),
            };
            tracker.increment(chunk_size);
        }

        Ok(())
    }

    async fn get_file_info(&self) -> Result<(u64, Option<String>)> {
        let resp = self.client.head(&self.config.url).send().await?;

        let file_size = if let Some(content_length) = resp.headers().get("content-length") {
            content_length.to_str()?.parse()?
        } else {
            anyhow::bail!("Could not determine file size")
        };

        let filename = self.extract_filename_from_response(&resp).await;

        Ok((file_size, filename))
    }

    async fn extract_filename_from_response(&self, resp: &reqwest::Response) -> Option<String> {
        if let Some(disposition) = resp.headers().get("content-disposition") {
            if let Ok(disposition_str) = disposition.to_str() {
                if let Some(filename) = Self::parse_content_disposition(disposition_str) {
                    return Some(filename);
                }
            }
        }

        Self::extract_filename_from_url(&self.config.url)
    }

    fn parse_content_disposition(disposition: &str) -> Option<String> {
        if let Some(idx) = disposition.find("filename=") {
            let start = idx + 9;
            let mut end = disposition.len();

            if disposition.as_bytes().get(start) == Some(&b'"') {
                let quoted_name = &disposition[start + 1..];
                if let Some(quote_end) = quoted_name.find('"') {
                    return Some(quoted_name[..quote_end].to_string());
                }
            } else {
                if let Some(semicolon) = disposition[start..].find(';') {
                    end = start + semicolon;
                }
                return Some(disposition[start..end].to_string());
            }
        }
        None
    }

    fn extract_filename_from_url(url: &str) -> Option<String> {
        if let Ok(parsed_url) = Url::parse(url) {
            let path = parsed_url.path();
            if let Some(path_filename) = Path::new(path).file_name() {
                if let Some(filename_str) = path_filename.to_str() {
                    if !filename_str.is_empty() {
                        if let Ok(decoded) = urlencoding::decode(filename_str) {
                            return Some(decoded.to_string());
                        }
                    }
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
    };

    let downloader = Downloader {
        client: Client::new(),
        config,
    };

    if downloader.config.should_log() {
        println!(
            "Starting download with {} connections, chunk size: {}MB, buffer: {}MB",
            downloader.config.num_connections, args.chunk_size, args.buffer_size
        );
        println!("URL: {}", args.url);
    }

    downloader.download().await?;

    Ok(())
}

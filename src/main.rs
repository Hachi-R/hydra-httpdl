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
    #[arg(short, long, default_value = "8")]
    buffer_size: usize,

    #[arg(short = 'v', long, default_value = "false")]
    verbose: bool,

    #[arg(short = 's', long, default_value = "false")]
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

struct Downloader {
    client: Client,
    config: DownloadConfig,
}

impl Downloader {
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

        if self.config.verbose && !self.config.silent {
            println!("Detected filename: {}", output_path);
        }

        let file = File::create(&output_path)?;
        file.set_len(file_size)?;
        let file = Arc::new(Mutex::new(BufWriter::with_capacity(
            self.config.buffer_size,
            file,
        )));

        let progress_bar = if !self.config.silent {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")?
            );
            Some(pb)
        } else {
            None
        };

        let chunk_size = self.config.chunk_size as u64;
        let mut tasks = FuturesUnordered::new();

        for i in 0..=(file_size / chunk_size) {
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size - 1, file_size - 1);

            let client = self.client.clone();
            let url = self.config.url.clone();
            let file_clone = Arc::clone(&file);
            let pb_clone = progress_bar.clone();

            tasks.push(tokio::spawn(async move {
                Self::download_chunk(client, url, start, end, file_clone, pb_clone).await
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

        if let Some(pb) = progress_bar {
            pb.finish_with_message("Download complete");
        }

        Ok(())
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

                if let Some(pb) = &progress_bar {
                    pb.inc(remaining);
                }
                break;
            }

            let mut writer = file.lock().await;
            writer.seek(SeekFrom::Start(position))?;
            writer.write_all(&chunk)?;
            writer.flush()?;
            drop(writer);

            position += chunk_size;
            if let Some(pb) = &progress_bar {
                pb.inc(chunk_size);
            }
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

        let mut filename = None;

        if let Some(disposition) = resp.headers().get("content-disposition") {
            if let Ok(disposition_str) = disposition.to_str() {
                if let Some(idx) = disposition_str.find("filename=") {
                    let start = idx + 9;
                    let mut end = disposition_str.len();

                    if disposition_str.as_bytes().get(start) == Some(&b'"') {
                        let quoted_name = &disposition_str[start + 1..];
                        if let Some(quote_end) = quoted_name.find('"') {
                            filename = Some(quoted_name[..quote_end].to_string());
                        }
                    } else {
                        if let Some(semicolon) = disposition_str[start..].find(';') {
                            end = start + semicolon;
                        }

                        filename = Some(disposition_str[start..end].to_string());
                    }
                }
            }
        }

        if filename.is_none() {
            if let Ok(parsed_url) = Url::parse(&self.config.url) {
                let path = parsed_url.path();
                if let Some(path_filename) = Path::new(path).file_name() {
                    if let Some(filename_str) = path_filename.to_str() {
                        if !filename_str.is_empty() {
                            if let Ok(decoded) = urlencoding::decode(filename_str) {
                                filename = Some(decoded.to_string());
                            }
                        }
                    }
                }
            }
        }

        Ok((file_size, filename))
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

    if args.verbose && !args.silent {
        println!(
            "Starting download with {} connections, chunk size: {}MB, buffer: {}MB",
            downloader.config.num_connections, args.chunk_size, args.buffer_size
        );
        println!("URL: {}", args.url);
    }

    downloader.download().await?;

    Ok(())
}

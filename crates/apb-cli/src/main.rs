mod input;
mod output;

use std::fs;
use std::io;

use clap::{Parser, Subcommand};
use tracing::{debug, info, warn};

use apb_core::descriptor::ProtoSchema;
use apb_core::mapping::{infer_mapping, InferOptions};
use apb_core::transcode::Transcoder;
use apb_core::validation::{self, ReportStatus};

use input::OpenInput;
use output::{OutputFormat, OutputWriter};

#[derive(Parser)]
#[command(name = "apb", about = "Arrow to Protobuf transcoder")]
struct Cli {
    /// Enable verbose logging (repeat for more: -v debug, -vv trace).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate a mapping between an Arrow schema and a proto message.
    Validate {
        /// Path to proto descriptor file (FileDescriptorSet binary).
        #[arg(long)]
        descriptor: String,

        /// Fully qualified proto message name.
        #[arg(long)]
        message: String,

        /// DuckDB SQL query to get the Arrow schema.
        #[arg(long, group = "input_source")]
        query: Option<String>,

        /// Arrow IPC file path (or - for stdin).
        #[arg(long, group = "input_source")]
        ipc: Option<String>,

        /// Promote warnings to errors.
        #[arg(long)]
        strict: bool,

        /// Output format: human or json.
        #[arg(long, default_value = "human")]
        format: String,
    },

    /// Read Arrow data, transcode to protobuf, write output.
    Transcode {
        /// Path to proto descriptor file (FileDescriptorSet binary).
        #[arg(long)]
        descriptor: String,

        /// Fully qualified proto message name.
        #[arg(long)]
        message: String,

        /// DuckDB SQL query to read Arrow data.
        #[arg(long, group = "input_source")]
        query: Option<String>,

        /// Arrow IPC file path (or - for stdin).
        #[arg(long, group = "input_source")]
        ipc: Option<String>,

        /// Output format.
        #[arg(long, value_enum, default_value = "proto-delimited")]
        out_format: OutputFormat,

        /// Output file path (default: stdout).
        #[arg(long)]
        out: Option<String>,

        /// Allow type coercions globally (e.g. string → enum).
        #[arg(long)]
        coerce: bool,

        /// Behavior for unknown enum string values: error, default, skip.
        #[arg(long, value_enum, default_value = "error")]
        unknown_enum: CliUnknownEnum,
    },
}

/// CLI wrapper for UnknownEnumBehavior (with clap derive).
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliUnknownEnum {
    Error,
    Default,
    Skip,
}

impl From<CliUnknownEnum> for apb_core::transcode::UnknownEnumBehavior {
    fn from(v: CliUnknownEnum) -> Self {
        match v {
            CliUnknownEnum::Error => Self::Error,
            CliUnknownEnum::Default => Self::Default,
            CliUnknownEnum::Skip => Self::Skip,
        }
    }
}

fn init_logging(verbose: u8) {
    use tracing_subscriber::EnvFilter;

    let filter = match verbose {
        0 => EnvFilter::new("warn"),
        1 => EnvFilter::new("info"),
        2 => EnvFilter::new("debug"),
        _ => EnvFilter::new("trace"),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_target(false)
        .init();
}

fn main() {
    let cli = Cli::parse();
    init_logging(cli.verbose);

    let result = match cli.command {
        Command::Validate {
            descriptor,
            message,
            query,
            ipc,
            strict,
            format,
        } => run_validate(descriptor, message, query, ipc, strict, format),
        Command::Transcode {
            descriptor,
            message,
            query,
            ipc,
            out_format,
            out,
            coerce,
            unknown_enum,
        } => run_transcode(
            descriptor,
            message,
            query,
            ipc,
            out_format,
            out,
            coerce,
            unknown_enum.into(),
        ),
    };

    if let Err(e) = result {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

fn load_schema(descriptor: &str) -> Result<ProtoSchema, Box<dyn std::error::Error>> {
    debug!(descriptor, "loading proto descriptor");
    let bytes = fs::read(descriptor)?;
    let schema = ProtoSchema::from_bytes(&bytes)?;
    Ok(schema)
}

fn open_input(
    query: Option<String>,
    ipc: Option<String>,
) -> Result<OpenInput, Box<dyn std::error::Error>> {
    match (query, ipc) {
        #[cfg(feature = "duckdb")]
        (Some(q), _) => {
            debug!("opening DuckDB input");
            input::open_duckdb(&q)
        }
        #[cfg(not(feature = "duckdb"))]
        (Some(_), _) => {
            Err("--query requires the 'duckdb' feature (build with --features duckdb)".into())
        }
        (_, Some(path)) => {
            debug!(path, "opening IPC input");
            input::open_ipc(&path)
        }
        _ => Err("either --query or --ipc is required".into()),
    }
}

fn run_validate(
    descriptor: String,
    message: String,
    query: Option<String>,
    ipc: Option<String>,
    strict: bool,
    format: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let proto_schema = load_schema(&descriptor)?;
    let msg_desc = proto_schema.message(&message)?;

    let source_name = ipc.as_deref().unwrap_or("(query)").to_string();
    let input = open_input(query, ipc)?;

    info!(message = %message, "validating schema mapping");

    let options = InferOptions {
        allow_unmapped_proto: !strict,
        allow_unmapped_arrow: !strict,
        ..InferOptions::default()
    };

    let mut report = validation::validate(&input.schema, &msg_desc, &options);
    report.source_name = Some(source_name);

    match format.as_str() {
        "json" => println!("{}", report.to_json()),
        _ => print!("{}", report.render_human()),
    }

    io::Write::flush(&mut io::stdout())?;

    if report.status == ReportStatus::Error {
        Err("validation failed".into())
    } else if strict && report.status == ReportStatus::Warnings {
        Err("validation has warnings (--strict mode)".into())
    } else {
        Ok(())
    }
}

fn run_transcode(
    descriptor: String,
    message: String,
    query: Option<String>,
    ipc: Option<String>,
    out_format: OutputFormat,
    out: Option<String>,
    coerce: bool,
    unknown_enum: apb_core::transcode::UnknownEnumBehavior,
) -> Result<(), Box<dyn std::error::Error>> {
    let proto_schema = load_schema(&descriptor)?;
    let msg_desc = proto_schema.message(&message)?;

    let input = open_input(query, ipc)?;

    let arrow_fields = input.schema.fields().len();
    info!(arrow_fields, "schema loaded");

    let infer_opts = InferOptions {
        coerce_all: coerce,
        ..InferOptions::default()
    };
    let mapping = infer_mapping(&input.schema, &msg_desc, &infer_opts)?;

    let mapped = mapping.bindings.len();
    let unmapped_arrow = mapping.unmapped_arrow.len();
    let unmapped_proto = mapping.unmapped_proto.len();
    info!(mapped, unmapped_arrow, unmapped_proto, "mapping resolved");

    if unmapped_arrow > 0 || unmapped_proto > 0 {
        warn!(unmapped_arrow, unmapped_proto, "some fields are unmapped");
    }

    let transcoder = Transcoder::new(&mapping)?.with_unknown_enum(unknown_enum);

    let writer: Box<dyn io::Write> = match &out {
        Some(path) => {
            debug!(path, "writing output to file");
            Box::new(fs::File::create(path)?)
        }
        None => Box::new(io::stdout().lock()),
    };
    let mut output = OutputWriter::new(&out_format, writer, &msg_desc)?;

    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;

    for batch_result in input.into_batches() {
        let batch = batch_result?;
        let rows = batch.num_rows();
        total_rows += rows;
        total_batches += 1;
        debug!(batch = total_batches, rows, "transcoding batch");
        output.write_batch(&batch, &transcoder)?;
    }

    output.finish()?;

    info!(total_rows, total_batches, "transcoding complete");

    Ok(())
}

use std::fmt::Write;

use super::report::*;

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

/// A row to render — plain text columns computed first, then formatted with
/// padding and color in a second pass.
struct Row {
    arrow: String,
    proto: String,
    status: Status,
    /// Indentation depth for nested fields.
    depth: usize,
}

enum Status {
    Ok,
    OkCoerce(String),
    Missing { oneof: Option<String> },
    Warning(String),
}

impl MappingReport {
    pub fn render_human(&self) -> String {
        let mut rows = Vec::new();
        collect_rows(&mut rows, self, 0);

        // Compute column widths from plain text (no ANSI codes).
        let indent_size = 2;
        let arrow_w = rows
            .iter()
            .map(|r| r.depth * indent_size + r.arrow.len())
            .max()
            .unwrap_or(12)
            .max(12);
        let proto_w = rows
            .iter()
            .map(|r| r.depth * indent_size + r.proto.len())
            .max()
            .unwrap_or(12)
            .max(12);

        let mut out = String::new();
        writeln!(out).unwrap();
        writeln!(
            out,
            "  {BOLD}{:<aw$}{RESET} {BOLD}{}{RESET}",
            "Arrow Schema",
            self.message_name,
            aw = arrow_w + 2,
        )
        .unwrap();
        writeln!(
            out,
            "  {DIM}{:<aw$} {}{RESET}",
            "─".repeat(arrow_w),
            "─".repeat(proto_w),
            aw = arrow_w + 2,
        )
        .unwrap();

        for row in &rows {
            let indent = " ".repeat(row.depth * indent_size);
            let arrow_padded = format!("{indent}{}", row.arrow);
            let proto_padded = format!("{indent}{}", row.proto);

            match &row.status {
                Status::Ok => {
                    writeln!(
                        out,
                        "  {:<aw$} {:<pw$} {GREEN}✓{RESET}",
                        arrow_padded,
                        proto_padded,
                        aw = arrow_w + 2,
                        pw = proto_w,
                    )
                    .unwrap();
                }
                Status::OkCoerce(mode) => {
                    writeln!(
                        out,
                        "  {:<aw$} {:<pw$} {GREEN}✓{RESET} {DIM}{mode}{RESET}",
                        arrow_padded,
                        proto_padded,
                        aw = arrow_w + 2,
                        pw = proto_w,
                    )
                    .unwrap();
                }
                Status::Missing { oneof } => {
                    let suffix = match oneof {
                        Some(name) => format!(" {DIM}// oneof {name}{RESET}"),
                        None => String::new(),
                    };
                    writeln!(
                        out,
                        "  {:<aw$} {:<pw$} {RED}✗ missing{RESET}{suffix}",
                        "",
                        proto_padded,
                        aw = arrow_w + 2,
                        pw = proto_w,
                    )
                    .unwrap();
                }
                Status::Warning(reason) => {
                    writeln!(
                        out,
                        "  {:<aw$} {:<pw$} {YELLOW}⚠ {reason}{RESET}",
                        arrow_padded,
                        proto_padded,
                        aw = arrow_w + 2,
                        pw = proto_w,
                    )
                    .unwrap();
                }
            }
        }

        // Unmapped Arrow columns.
        if !self.unmapped_arrow.is_empty() {
            writeln!(out).unwrap();
            for f in &self.unmapped_arrow {
                let arrow_col = format!("{} ({})", f.name, f.arrow_type);
                writeln!(
                    out,
                    "  {:<aw$} {:<pw$} {YELLOW}⚠ no proto field{RESET}",
                    arrow_col,
                    "",
                    aw = arrow_w + 2,
                    pw = proto_w,
                )
                .unwrap();
            }
        }

        // Structural errors.
        if !self.structural_errors.is_empty() {
            writeln!(out).unwrap();
            for e in &self.structural_errors {
                writeln!(out, "  {RED}✗ {}: {}{RESET}", e.path, e.message).unwrap();
            }
        }

        // Summary.
        let total = self.mapped.len() + self.unmapped_proto.len() + self.type_errors.len();
        let mapped = self.mapped.len();
        writeln!(out).unwrap();
        let color = match self.status {
            ReportStatus::Ok => GREEN,
            ReportStatus::Warnings => YELLOW,
            ReportStatus::Error => RED,
        };
        writeln!(out, "  {color}{mapped}/{total} proto fields mapped{RESET}").unwrap();

        out
    }
}

/// Collect rows from a report, recursing into nested reports.
fn collect_rows(rows: &mut Vec<Row>, report: &MappingReport, depth: usize) {
    // Interleave all entries by proto field number.
    enum Entry<'a> {
        Mapped(&'a MappedField),
        TypeErr(&'a FieldTypeError),
        Unmapped(&'a UnmappedProtoField),
    }

    let mut entries: Vec<Entry> = Vec::new();
    for f in &report.mapped {
        entries.push(Entry::Mapped(f));
    }
    for e in &report.type_errors {
        entries.push(Entry::TypeErr(e));
    }
    for f in &report.unmapped_proto {
        entries.push(Entry::Unmapped(f));
    }
    entries.sort_by_key(|e| match e {
        Entry::Mapped(f) => f.proto_number,
        Entry::TypeErr(e) => e.proto_number,
        Entry::Unmapped(f) => f.number,
    });

    for entry in entries {
        match entry {
            Entry::Mapped(f) => {
                let arrow = if depth > 0 {
                    format!(".{} ({})", f.arrow_name, f.arrow_type)
                } else {
                    format!("{} ({})", f.arrow_name, f.arrow_type)
                };
                let proto = if depth > 0 {
                    format!("{} .{} = {};", f.proto_type, f.proto_name, f.proto_number)
                } else {
                    format!("{} {} = {};", f.proto_type, f.proto_name, f.proto_number)
                };
                let status = if f.type_mode == "direct" {
                    Status::Ok
                } else {
                    Status::OkCoerce(f.type_mode.clone())
                };
                rows.push(Row {
                    arrow,
                    proto,
                    status,
                    depth,
                });

                // Recurse into nested message.
                if f.field_shape == FieldShapeSummary::Message {
                    if let Some(nested) =
                        report.nested.iter().find(|n| n.proto_field == f.proto_name)
                    {
                        collect_rows(rows, &nested.report, depth + 1);
                    }
                }
            }
            Entry::TypeErr(e) => {
                let arrow = if depth > 0 {
                    format!(".{} ({})", e.arrow_name, e.arrow_type)
                } else {
                    format!("{} ({})", e.arrow_name, e.arrow_type)
                };
                let proto = if depth > 0 {
                    format!("{} .{} = {};", e.proto_type, e.proto_name, e.proto_number)
                } else {
                    format!("{} {} = {};", e.proto_type, e.proto_name, e.proto_number)
                };
                rows.push(Row {
                    arrow,
                    proto,
                    status: Status::Warning(e.reason.clone()),
                    depth,
                });
            }
            Entry::Unmapped(f) => {
                let proto = if depth > 0 {
                    format!("{} .{} = {};", f.proto_type, f.name, f.number)
                } else {
                    format!("{} {} = {};", f.proto_type, f.name, f.number)
                };
                rows.push(Row {
                    arrow: String::new(),
                    proto,
                    status: Status::Missing {
                        oneof: f.oneof_name.clone(),
                    },
                    depth,
                });
            }
        }
    }
}

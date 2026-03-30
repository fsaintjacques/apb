use std::fmt::Write;

use super::report::*;

impl MappingReport {
    /// Render the report as human-readable text for CLI output.
    pub fn render_human(&self) -> String {
        let mut out = String::new();

        writeln!(out, "Validation: {}", self.message_name).unwrap();
        writeln!(
            out,
            "Status: {}",
            match self.status {
                ReportStatus::Ok => "OK",
                ReportStatus::Warnings => "WARNINGS",
                ReportStatus::Error => "ERROR",
            }
        )
        .unwrap();
        writeln!(out).unwrap();

        // Mapped fields.
        writeln!(out, "Mapped fields ({}):", self.mapped.len()).unwrap();
        if self.mapped.is_empty() {
            writeln!(out, "  (none)").unwrap();
        } else {
            for f in &self.mapped {
                let coerce_note = if f.type_mode.starts_with("coerce") {
                    format!(", {}", f.type_mode)
                } else {
                    String::new()
                };
                writeln!(
                    out,
                    "  + {:<20} -> {:<20} (#{} {:?})  {}{}",
                    f.arrow_name,
                    f.proto_name,
                    f.proto_number,
                    f.field_shape,
                    f.bind_method,
                    coerce_note,
                )
                .unwrap();
            }
        }
        writeln!(out).unwrap();

        // Unmapped Arrow fields.
        writeln!(
            out,
            "Unmapped Arrow fields ({}):",
            self.unmapped_arrow.len()
        )
        .unwrap();
        if self.unmapped_arrow.is_empty() {
            writeln!(out, "  (none)").unwrap();
        } else {
            for f in &self.unmapped_arrow {
                writeln!(out, "  ? {}  ({})", f.name, f.detail).unwrap();
            }
        }
        writeln!(out).unwrap();

        // Unmapped proto fields.
        writeln!(
            out,
            "Unmapped proto fields ({}):",
            self.unmapped_proto.len()
        )
        .unwrap();
        if self.unmapped_proto.is_empty() {
            writeln!(out, "  (none)").unwrap();
        } else {
            for f in &self.unmapped_proto {
                writeln!(out, "  ? {}  ({})", f.name, f.detail).unwrap();
            }
        }
        writeln!(out).unwrap();

        // Type errors.
        writeln!(out, "Type errors ({}):", self.type_errors.len()).unwrap();
        if self.type_errors.is_empty() {
            writeln!(out, "  (none)").unwrap();
        } else {
            for e in &self.type_errors {
                writeln!(
                    out,
                    "  x {} -> {} : arrow:{} <> proto:{} -- {}",
                    e.arrow_name, e.proto_name, e.arrow_type, e.proto_type, e.reason,
                )
                .unwrap();
            }
        }
        writeln!(out).unwrap();

        // Structural errors.
        writeln!(
            out,
            "Structural errors ({}):",
            self.structural_errors.len()
        )
        .unwrap();
        if self.structural_errors.is_empty() {
            writeln!(out, "  (none)").unwrap();
        } else {
            for e in &self.structural_errors {
                writeln!(out, "  x {}: {}", e.path, e.message).unwrap();
            }
        }

        // Nested reports.
        for nested in &self.nested {
            writeln!(out).unwrap();
            writeln!(out, "--- Nested: {} ---", nested.proto_field).unwrap();
            write!(out, "{}", nested.report.render_human()).unwrap();
        }

        out
    }
}

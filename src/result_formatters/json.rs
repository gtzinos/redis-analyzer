use serde_json;
use std::fs::File;
use std::io::Write;

use super::Formatter;
use crate::analyzer::AnalyzerResult;
use crate::config::Config;

pub struct JsonFormatter;

impl Formatter for JsonFormatter {
    fn call(&self, config: &Config, result: &AnalyzerResult) {
        let json = serde_json::to_string_pretty(&result.root_prefix).unwrap();
        if let Some(ref folder) = config.output_folder {
            std::fs::create_dir_all(folder).expect("Unable to create output folder");
            let path = format!("{}/output.json", folder);
            let mut file = File::create(&path).expect("Unable to create output.json");
            file.write_all(json.as_bytes()).expect("Unable to write JSON data");
            println!("Exported analysis result to {}", path);
        } else {
            println!("{}", json);
        }
    }
}

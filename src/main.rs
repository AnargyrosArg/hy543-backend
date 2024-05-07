use std::{fs::File, io::BufReader};

use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;

mod dataframe;
pub mod execgraph;

fn main() {
    worker();
}

fn worker() {
    // Open the file in read-only mode with buffer.
    let file = File::open("nodes.json").expect("Could not open file!");
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let graph: ExecGraph = serde_json::from_reader(reader).expect("Could not read json!");

    graph.print();

    let mut dataframe = Dataframe::new_empty();

    dataframe.play(graph);

    // let dataframe:Dataframe = Dataframe::read_from_csv("deniro.csv",1,10).unwrap();

    dataframe.print();
}

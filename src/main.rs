use std::{
    fs::File,
    io::BufReader,
};

use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;
use mpi::{
    environment::Universe,
    traits::Communicator,
};

mod dataframe;
pub mod execgraph;

fn main() {
    let universe: Universe = mpi::initialize().unwrap();
    let world: mpi::topology::SimpleCommunicator = universe.world();
    let rank: i32 = world.rank();
    //need atleast 2 nodes: 1 communicator + 1 worker
    assert!(world.size() > 1);
    //MPI ranks:
    // rank 0 -> communicator process
    //code that receives work from client, puts it in a queue and broadcasts it to workers
    //code that accumulates the result from the workers and sends it back to the client
    // rank 1 -> special worker node that performs some work and broadcasts it to all other worker nodes in order to
    //avoid duplicate work
    // rank _ -> worker nodes -> receive graph and execute it

    match rank {
        0 => {
            todo!("Communicator node");
        }
        _ => {
            worker(universe);
        }
    }
}

fn worker(universe: Universe) {
    // Open the file in read-only mode with buffer.
    let file = File::open("nodes.json").expect("Could not open file!");
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let graph: ExecGraph = serde_json::from_reader(reader).expect("Could not read json!");

    let mut dataframe = Dataframe::new_empty(universe);

    //Execute the graph
    dataframe.play(graph);
    println!("========================");
    dataframe.print();

}

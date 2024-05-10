use std::{fs::File, io::BufReader};

use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;
use mpi::traits::CommunicatorCollectives;
use mpi::{environment::Universe, traits::Communicator};

mod dataframe;
pub mod execgraph;

use std::io::Read;
use std::net::Ipv4Addr;
use std::net::TcpListener;

const ADDR: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const PORT: u16 = 7878;

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
            communicator(universe);
        }
        _ => {
            worker(universe);
        }
    }
}

fn communicator(universe: Universe) {
    loop {
        let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();

            let mut buffer = [0; 512];
            let read_size = stream.read(&mut buffer).unwrap();
            let s = std::str::from_utf8(&buffer[0..read_size])
                .expect("Not a valid UTF-8 sequence")
                .to_string();
            let deserialized_graph: ExecGraph = serde_json::from_str(&s).unwrap();

            deserialized_graph.print();
            break;
        }

        universe.world().barrier();
    }
}

fn worker(universe: Universe) {
    loop {
        universe.world().barrier();
        // Open the file in read-only mode with buffer.
        let file = File::open("nodes.json").expect("Could not open file!");
        let reader = BufReader::new(file);

        // Read the JSON as an instance of graph struct.
        let graph: ExecGraph = serde_json::from_reader(reader).expect("Could not read json!");

        //init dataframe
        let mut dataframe = Dataframe::new_empty(&universe);

        //Execute the graph
        dataframe.play(graph);

        dataframe.print();
    }
}

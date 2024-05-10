use std::collections::HashMap;
use std::{fs::File, io::BufReader};

use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;
use mpi::collective::SystemOperation;
use mpi::traits::{CommunicatorCollectives, Root};
use mpi::{environment::Universe, traits::Communicator};

mod dataframe;
pub mod execgraph;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

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
            
        },
        _ => {
            worker(universe);
        }
    }
}

fn communicator(universe: Universe) {
    loop {
        //receive graph from client
        let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();

            let mut buffer = [0; 512];
            let read_size = stream.read(&mut buffer).unwrap();
            let s = std::str::from_utf8(&buffer[0..read_size])
                .expect("Not a valid UTF-8 sequence")
                .to_string();
            let deserialized_graph: ExecGraph = serde_json::from_str(&s).unwrap();
            let mut file = File::create("nodes.json").unwrap();
            file.write_all(s.as_bytes()).unwrap();
            deserialized_graph.print();
            break;
        }
        //barrier not required -> there should be an implicit barrier on graph broadcast -> remove when implemented
        universe.world().barrier();

        //TODO BROADCAST GRAPH

        //TODO GATHER RESULT
        let mut numeric_result = 0 as usize;
        let dummy = 0 as usize;
        universe.world().process_at_rank(0).reduce_into_root(
            &dummy,
            &mut numeric_result,
            SystemOperation::sum(),
        );

        println!("Reduced result: {}", numeric_result);

        //Response to client
        let mut stream = TcpStream::connect("127.0.0.1:8001").unwrap();
        let num_string = numeric_result.to_string();
        let msg = num_string.as_bytes();
        stream.write_all(msg).unwrap();
    }
}

fn worker(universe: Universe) {
    let mut persisted_graphs: HashMap<usize,Dataframe> = HashMap::new();
    loop {
        //barrier not required -> there should be an implicit barrier on graph broadcast -> remove when implemented
        universe.world().barrier();


        // Open the file in read-only mode with buffer.
        let file = File::open("nodes.json").expect("Could not open file!");
        let reader = BufReader::new(file);

        // Read the JSON as an instance of graph struct.
        let graph: ExecGraph = serde_json::from_reader(reader).expect("Could not read json!");

        //init dataframe
        let mut dataframe;
        if *graph.get_checkpoint() == 0 {
            //no checkpoint for current graph -> make new from scratch
            dataframe = Dataframe::new_empty(&universe);
        }else{
            //get from checkpoint
            dataframe = persisted_graphs.remove(graph.get_checkpoint()).unwrap();
        }

        //Execute the graph
        dataframe.play(&graph);

        //communicate numeric to communicator -> fetch not supported yet
        let mut numeric_result = dataframe.get_result();
        universe
            .world()
            .process_at_rank(0)
            .reduce_into(&mut numeric_result, SystemOperation::sum());
        
        let id = graph.iter().last().unwrap().get_operation_id();
        persisted_graphs.insert(id, dataframe);
    }
}

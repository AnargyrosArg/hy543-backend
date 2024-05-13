use std::collections::HashMap;
use std::{fs::File, io::BufReader};

use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;
use mpi::collective::SystemOperation;
use mpi::traits::{CommunicatorCollectives, Root};
use mpi::{environment::Universe, traits::Communicator};
use serde_json::{json, Value};

mod dataframe;
pub mod execgraph;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use gethostname::gethostname;

fn main() {
    let universe: Universe = mpi::initialize().unwrap();
    let world: mpi::topology::SimpleCommunicator = universe.world();
    let rank: i32 = world.rank();

    println!("Init..");
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
        let mut len: i32 = 0;
        let mut s:String = String::new();
        //receive graph from client

        println!("ATTEMPTING TO BIND PORT AT 65000");
        let listener = TcpListener::bind("0.0.0.0:65000").unwrap();
        println!("Communicator started at node: {:?}",gethostname());
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();

            let mut buffer = [0; 512];
            let read_size = stream.read(&mut buffer).unwrap();
            s = std::str::from_utf8(&buffer[0..read_size])
                .expect("Not a valid UTF-8 sequence")
                .to_string();
            len = s.len() as i32;
            let deserialized_graph: ExecGraph = serde_json::from_str(&s).unwrap();
            let mut file = File::create("nodes.json").unwrap();
            file.write_all(s.as_bytes()).unwrap();
            deserialized_graph.print();
            break;
        }


        // Broadcast the length of the string
        universe.world().process_at_rank(0).broadcast_into(&mut len);

        // Create a buffer of the appropriate size
        let mut x_bytes = vec![0; len as usize];

        if universe.world().rank() == 0 {
            x_bytes.copy_from_slice(s.as_bytes());
        }
        
        // Broadcast the JSON string
        universe.world().process_at_rank(0).broadcast_into(&mut x_bytes);

        //gather result
        let mut numeric_result = 0 as usize;
        let dummy = 0 as usize;
        universe.world().process_at_rank(0).reduce_into_root(
            &dummy,
            &mut numeric_result,
            SystemOperation::sum(),
        );

        println!("Reduced result: {}", numeric_result);

        let client_addr ="large2:65001";
        //Response to client
        let mut stream = TcpStream::connect(client_addr).unwrap();
        let num_string = numeric_result.to_string();
        let msg = num_string.as_bytes();
        stream.write_all(msg).unwrap();
    }
}

fn worker(universe: Universe) {
    let mut persisted_graphs: HashMap<usize, Dataframe> = HashMap::new();
    loop {
        //barrier not required -> there should be an implicit barrier on graph broadcast -> remove when implemented
        // universe.world().barrier();

        let mut len = 0;
        universe.world().process_at_rank(0).broadcast_into(&mut len);
        let mut x_bytes = vec![0; len as usize];

        // Broadcast the JSON string
        universe
            .world()
            .process_at_rank(0)
            .broadcast_into(&mut x_bytes);

        let x = String::from_utf8(x_bytes).unwrap();
        // println!("Rank {} received value: {}.", universe.world().rank(), x);

        // Read the JSON as an instance of graph struct.    
        let graph: ExecGraph = serde_json::from_str(&x).unwrap();

        //init dataframe
        let mut dataframe;
        if *graph.get_checkpoint() == 0 {
            //no checkpoint for current graph -> make new from scratch
            dataframe = Dataframe::new_empty(&universe);
        } else {
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

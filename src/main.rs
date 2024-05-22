use std::collections::HashMap;
use std::fs::File;
use std::process::exit;

use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
use aws_sdk_dynamodb::operation::put_item::PutItemInput;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput, ReturnValue, ScalarAttributeType
};
use dataframe::dataframe::Dataframe;
use execgraph::execgraph::ExecGraph;
use futures::executor::block_on;
use mpi::collective::SystemOperation;
use mpi::traits::Root;
use mpi::{environment::Universe, traits::Communicator};
mod dataframe;
pub mod execgraph;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use gethostname::gethostname;

#[tokio::main]
async fn main() {
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
        _ => worker(universe),
    }
}

fn communicator(universe: Universe) {
    loop {
        let mut len: i32 = 0;
        let mut s: String = String::new();
        //receive graph from client

        let listener = TcpListener::bind("0.0.0.0:65000").unwrap();
        println!("Communicator started at node: {:?}", gethostname());
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();

            let mut buffer = [0; 512];
            let read_size = stream.read(&mut buffer).unwrap();
            s = std::str::from_utf8(&buffer[0..read_size])
                .expect("Not a valid UTF-8 sequence")
                .to_string();
            len = s.len() as i32;
            let deserialized_graph: ExecGraph = serde_json::from_str(&s).unwrap();

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
        universe
            .world()
            .process_at_rank(0)
            .broadcast_into(&mut x_bytes);

        //gather result
        let mut numeric_result = 0 as usize;
        let dummy = 0 as usize;
        universe.world().process_at_rank(0).reduce_into_root(
            &dummy,
            &mut numeric_result,
            SystemOperation::sum(),
        );

        println!("Reduced result: {}", numeric_result);

        let client_addr = "0.0.0.0:65001";
        //Response to client
        let mut stream = TcpStream::connect(client_addr).unwrap();
        let num_string = numeric_result.to_string();
        let msg = num_string.as_bytes();
        stream.write_all(msg).unwrap();
    }
}

fn worker(universe: Universe) {

    //create a dynamoBD table for every worker
    let table_name = "worker".to_string() + &universe.world().rank().to_string();
    let future = create_table(table_name.as_str(), "id");
    //block until dynamoDB responds
    block_on(future).unwrap();

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
        println!("Graph executed!");
        //communicate numeric to communicator -> fetch not supported yet
        let mut numeric_result = dataframe.get_result();
        universe
            .world()
            .process_at_rank(0)
            .reduce_into(&mut numeric_result, SystemOperation::sum());

        //save current graph state
        let id = graph.iter().last().unwrap().get_operation_id();
        let state_value = serde_json::to_string(&dataframe).expect("couldnt serialize state");
        block_on(add_item(&table_name,id.to_string(),state_value)).unwrap();

    }
}

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{Client, Error};

pub async fn create_table(table: &str, key: &str) -> Result<CreateTableOutput, Error> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .test_credentials()
        .load()
        .await;
    let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config)
        // Override the endpoint in the config to use a local dynamodb server.
        .endpoint_url(
            // DynamoDB run locally uses port 8000 by default.
            "http://localhost:8000",
        )
        .build();

    let client = Client::from_conf(dynamodb_local_config);

    let a_name: String = key.into();
    let table_name: String = table.into();

    let ad = AttributeDefinition::builder()
        .attribute_name(&a_name)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap();

    let ks = KeySchemaElement::builder()
        .attribute_name(&a_name)
        .key_type(KeyType::Hash)
        .build()
        .unwrap();

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build()
        .unwrap();

    let create_table_response = client
        .create_table()
        .table_name(table_name)
        .key_schema(ks)
        .attribute_definitions(ad)
        .provisioned_throughput(pt)
        .send()
        .await;

    match create_table_response {
        Ok(out) => {
            println!("Added table {} with key {}", table, key);
            Ok(out)
        }
        Err(e) => {
            panic!("Error creating table;");
        }
    }
}

pub async fn add_item(table: &String,id:String ,value:String) -> Result<(), Error> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .test_credentials()
        .load()
        .await;
    let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config)
        // Override the endpoint in the config to use a local dynamodb server.
        .endpoint_url(
            // DynamoDB run locally uses port 8000 by default.
            "http://localhost:8000",
        )
        .build();

    let client = Client::from_conf(dynamodb_local_config);

    
    let id_av = AttributeValue::S(id);
    let value_av = AttributeValue::S(value);


    let request = client
        .put_item()
        .return_values(ReturnValue::AllOld)
        .table_name(table)
        .item("id",id_av )
        .item("val",value_av);

    let _resp = request.send().await?;
    
    Ok(())
}

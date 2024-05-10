pub mod execgraph {
    use crate::dataframe::table::table::FilterOpcodes;
    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    pub enum OperationType {
        Read, //filename
        Select,
        Where,
        Sum,
        Count,
        Fetch,
        Empty, // it is for the initialization
    }

    #[derive(Deserialize)]
    pub struct ExecGraph {
        operations: Vec<OpNode>,
        checkpoint:usize,
    }

    #[derive(Deserialize, Debug)]
    pub struct OpNode {
        //id
        id: usize, // if it is lazy t means it just creates a new node to the graph
        function_name: OperationType, //the function name
        args: Vec<String>, //arguments about the function
                   //to add more fields
    }

    impl OpNode {
        pub fn optype(&self) -> &OperationType {
            return &self.function_name;
        }

        pub fn get_read_op_filename(&self) -> &String {
            return &self.args[0];
        }

        pub fn get_binary_operation_left(&self) -> &String {
            return &self.args[0];
        }

        pub fn get_binary_operation_operator(&self) -> FilterOpcodes {
            match self.args[1].as_str() {
                "==" => FilterOpcodes::Equal,
                ">" => FilterOpcodes::Greater,
                ">=" => FilterOpcodes::GreaterEqual,
                "<" => FilterOpcodes::Less,
                "<=" => FilterOpcodes::LessEqual,
                &_ => panic!(),
            }
        }

        pub fn get_binary_operation_right(&self) -> &String {
            return &self.args[2];
        }

        pub fn get_projection_fields(&self) -> &Vec<String> {
            return &self.args;
        }

        pub fn get_operation_id(&self) -> usize {
            return self.id;
        }
    }

    impl std::fmt::Display for OperationType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                OperationType::Select => write!(f, "Select"),
                OperationType::Where => write!(f, "Where"),
                OperationType::Sum => write!(f, "Sum"),
                OperationType::Count => write!(f, "Count"),
                OperationType::Empty => write!(f, "Empty"),
                OperationType::Read => write!(f, "Read"),
                OperationType::Fetch => write!(f, "Fetch"),
            }
        }
    }

    impl ExecGraph {
        // pub fn new(ops: Vec<OpNode>) -> ExecGraph {
        //     return ExecGraph { operations: ops };
        // }

        pub fn get_checkpoint(&self) -> &usize{
            return &self.checkpoint;
        }
        
        pub fn print(&self) {
            for op in &self.operations {
                print!("{} -> ", op.function_name);
            }
            println!("");
        }

        pub fn iter(&self) -> std::slice::Iter<'_, OpNode> {
            return self.operations.iter();
        }
    }
}

pub mod table;

pub mod dataframe {

    use super::table::table::Table;
    use crate::execgraph::execgraph::{ExecGraph, OpNode, OperationType};
    use csv::StringRecord;
    use std::collections::HashMap;
    use std::fs::File;

    pub struct Dataframe {
        table: Table,
        field_indexes: HashMap<String, usize>,
    }
    impl Dataframe {
        pub fn play(&mut self, graph: ExecGraph) {
            for op in graph.iter() {
                match op.optype() {
                    OperationType::Select => self.exec_select(op),
                    OperationType::Where => self.exec_where(op),
                    OperationType::Sum => self.exec_sum(op),
                    OperationType::Count => self.exec_count(op),
                    OperationType::Empty => self.dummy(),
                    OperationType::Read => self.exec_read(op),
                };
            }
        }

        pub fn dummy(&mut self) {}

        pub fn exec_sum(&mut self,op: &OpNode){
            let i = self.table.sum_field(*self.field_indexes.get(op.get_read_op_filename()).expect("Could not resolve field name!"));
            println!("Sum of field {}: {}",op.get_read_op_filename(),i);
        }

        pub fn exec_read(&mut self, op: &OpNode) {
            self.read_from_csv(&op.get_read_op_filename(), 1, 10);
        }

        pub fn exec_where(&mut self, op: &OpNode) {
            let opcode = op.get_binary_operation_operator();
            let right_operand = op.get_binary_operation_right();

            let intermediate: Vec<usize>;
            if right_operand.parse::<i64>().is_err() {
                intermediate = self.table.filter_string(
                    right_operand,
                    opcode,
                    *self
                        .field_indexes
                        .get(op.get_binary_operation_left())
                        .unwrap(),
                );
            } else {
                intermediate = self.table.filter_numerical(
                    right_operand.parse::<i64>().unwrap(),
                    opcode,
                    *self
                        .field_indexes
                        .get(op.get_binary_operation_left())
                        .unwrap(),
                );
            }

            self.table.apply_intermediate_result(intermediate);
        }

        pub fn exec_select(&mut self, op: &OpNode) {
            for field in op.get_projection_fields() {
                self.table.select_projection(
                    *self
                        .field_indexes
                        .get(field)
                        .expect("Could not resolve field name!"),
                );
            }
        }

        pub fn exec_count(&self, op: &OpNode) {
            println!("Number of elements in table {}", self.table.len());
            // todo!();
        }

        pub fn new_empty() -> Dataframe {
            return Dataframe {
                table: Table::new(0),
                field_indexes: HashMap::new(),
            };
        }

        fn add_entry(&mut self, record: StringRecord) {
            let mut entry = Vec::new();
            for i in record.iter() {
                entry.push(i.to_string());
            }
            self.table.push(entry);
        }

        // fn new(fieldnames:Vec<String>) -> Dataframe{
        //     let mut fieldmap: HashMap<String,usize> =  HashMap::new();
        //     for i in 0..fieldnames.len(){
        //         fieldmap.insert(fieldnames[i].trim().to_owned(),i);
        //     }

        //     let dataframe = Dataframe{
        //         table:Table::new(fieldnames.len()),
        //         field_indexes: fieldmap,
        //     };
        //     return dataframe;
        // }

        // pub fn read_from_csv(filename:&str, starting_line:usize,stopping_line:usize) -> Result<Dataframe, Box<dyn Error>>{
        //     let file = File::open(filename);

        //     //create reader object
        //     let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).quoting(true).from_reader(file.unwrap());

        //     //get headers -> the names of the fields and construct a new dataframe object
        //     let headers = rdr.headers()?;
        //     let mut fieldnames = Vec::new();
        //     for i in headers.iter(){
        //         fieldnames.push(i.to_string());
        //     }

        //     //create new dataframe and load data
        //     let mut dataframe = Dataframe::new(fieldnames);
        //     //insert csv data into the dataframe skipping starting_line - 1 records
        //     let mut records_iter = rdr.records().skip(starting_line-1);
        //     for _iter in 0..(stopping_line-starting_line +1){
        //         let record = records_iter.next().expect("Failed to get element while iterating");
        //         dataframe.add_entry(record.unwrap());
        //     }

        //     return Ok(dataframe);
        // }

        pub fn read_from_csv(
            &mut self,
            filename: &str,
            starting_line: usize,
            stopping_line: usize,
        ) {
            assert!(
                starting_line >= 1,
                "Starting line should be >= 1! First line == line 1"
            );
            let file = File::open(filename);

            //create reader object
            let mut rdr = csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .quoting(true)
                .from_reader(file.unwrap());

            //get headers -> the names of the fields and construct a new dataframe object
            let headers = rdr.headers().expect("failed getting headers!");
            let mut fieldnames = Vec::new();
            for i in headers.iter() {
                fieldnames.push(i.to_string());
            }

            //create new dataframe and load data
            // let mut dataframe = Dataframe::new(fieldnames);

            let mut fieldmap: HashMap<String, usize> = HashMap::new();
            for i in 0..fieldnames.len() {
                fieldmap.insert(fieldnames[i].trim().to_owned(), i);
            }
            self.table = Table::new(fieldnames.len());
            self.field_indexes = fieldmap;

            //insert csv data into the dataframe skipping starting_line - 1 records
            let mut records_iter = rdr.records().skip(starting_line - 1);
            for _iter in 0..(stopping_line - starting_line + 1) {
                let record = records_iter
                    .next()
                    .expect("Failed to get element while iterating");
                self.add_entry(record.unwrap());
            }
        }

        pub fn print(&self) {
            for i in self.field_indexes.iter() {
                if self.table.is_projected(
                    *self
                        .field_indexes
                        .get(i.0)
                        .expect("Unable to resolve field name!"),
                ) {
                    print!("{},", i.0);
                }
            }
            println!();
            self.table.print();
        }
    }
}

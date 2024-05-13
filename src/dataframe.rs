pub mod table;

pub mod dataframe {

    use super::table::table::Table;
    use crate::execgraph::execgraph::{ExecGraph, OpNode, OperationType};
    use csv::StringRecord;
    use mpi::environment::Universe;
    use mpi::traits::{Communicator, Group, Root};
    use std::cmp;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    pub struct Dataframe<'a> {
        table: Table,
        field_indexes: HashMap<String, usize>,
        result: usize,
        mpi_universe: &'a Universe,
    }
    impl<'a> Dataframe<'a> {
        pub fn play(&mut self, graph: &ExecGraph) {
            for op in graph.iter() {
                println!("Executing: {}",op.optype());
                match op.optype() {
                    OperationType::Select => self.exec_select(op),
                    OperationType::Where => self.exec_where(op),
                    OperationType::Sum => self.exec_sum(op),
                    OperationType::Count => self.exec_count(),
                    OperationType::Empty => self.dummy(),
                    OperationType::Read => self.exec_read(op),
                    OperationType::Fetch => todo!("FETCH"),
                };
                println!("Done!");
            }
        }

        pub fn dummy(&mut self) {}

        pub fn exec_sum(&mut self, op: &OpNode) {
            let i = self.table.sum_field(
                *self
                    .field_indexes
                    .get(op.get_read_op_filename())
                    .expect("Could not resolve field name!"),
            );
            // println!("Sum of field {}: {}", op.get_read_op_filename(), i);
            self.result = i as usize;
        }

        pub fn exec_read(&mut self, op: &OpNode) {
            self.read_from_csv(&op.get_read_op_filename());
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

            self.table.apply_intermediate_result(&intermediate);
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

        pub fn exec_count(&mut self) {
            self.result = self.table.len();
        }

        pub fn new_empty(universe: &Universe) -> Dataframe {
            return Dataframe {
                table: Table::new(0),
                field_indexes: HashMap::new(),
                result: 0,
                mpi_universe: universe,
            };
        }

        fn add_entry(&mut self, record: StringRecord) {
            let mut entry = Vec::new();
            for i in record.iter() {
                entry.push(i.to_string());
            }
            self.table.push(entry);
        }

        pub fn read_from_csv(&mut self, filename: &str) {
            let file: File = File::open(filename).expect("Could not open file!");
            let reader = BufReader::new(file);
            let mut linecount: usize = 100000001;

            let workers_vec = (1..self.mpi_universe.world().size()).collect::<Vec<_>>();
            let workers_group = self.mpi_universe.world().group().include(&workers_vec[..]);
            let workers = self
                .mpi_universe
                .world()
                .split_by_subgroup(&workers_group)
                .unwrap();

            // if workers.rank() == 0 {
            //     //get linecount by parsing file once
            //     linecount = reader.lines().count();
            // }

            workers.process_at_rank(0).broadcast_into(&mut linecount);

            linecount = linecount - 1; //-1 to account for header line

            let rows_per_line = ((linecount as f32) / workers.size() as f32).ceil() as i32;

            let starting_line = workers.rank() * rows_per_line;
            let stopping_line: i32 = cmp::min(starting_line + rows_per_line, linecount as i32);

            let file: File = File::open(filename).unwrap();

            //create reader object
            let mut rdr = csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .quoting(true)
                .from_reader(file);

            //get headers -> the names of the fields and construct a new dataframe object
            let headers = rdr.headers().expect("failed getting headers!");
            let mut fieldnames = Vec::new();
            for i in headers.iter() {
                fieldnames.push(i.to_string());
            }

            //construct fieldmap
            let mut fieldmap: HashMap<String, usize> = HashMap::new();
            for i in 0..fieldnames.len() {
                fieldmap.insert(fieldnames[i].trim().to_owned(), i);
            }
            self.table = Table::new(fieldnames.len());
            self.field_indexes = fieldmap;

            //insert csv data into the dataframe skipping starting_line - 1 records
            let mut records_iter = rdr.records().skip(starting_line as usize);
            for _iter in 0..(stopping_line - starting_line) {
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

        pub fn get_result(&self) -> usize {
            return self.result;
        }
    }
}

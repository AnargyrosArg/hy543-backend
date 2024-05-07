pub mod table;

pub mod dataframe{
    use std::{collections::HashMap, error::Error, io::Seek};
    use csv::{Position, StringRecord};
    use std::io::{BufRead, BufReader};
    use super::table::table::Table;
    use std::fs::File;
    
    pub struct Dataframe {
        table: Table,
        field_indexes: HashMap<String,usize>
    }
    impl Dataframe{
        
        fn new(fieldnames:Vec<String>) -> Dataframe{
            let mut fieldmap: HashMap<String,usize> =  HashMap::new();
            for i in 0..fieldnames.len(){
                fieldmap.insert(fieldnames[i].trim().to_owned(),i);
            }

            let dataframe = Dataframe{
                table:Table::new(fieldnames.len()),
                field_indexes: fieldmap,
            };
            return dataframe;
        }
        
        fn add_entry(&mut self,record:StringRecord){
            let mut entry = Vec::new(); 
            for i in record.iter(){
                entry.push(i.to_string());
            }
            self.table.push(entry);

        }

        
        pub fn read_from_csv(filename:&str, starting_line:usize,stopping_line:usize) -> Result<Dataframe, Box<dyn Error>>{
            let file = File::open(filename);
            
            //create reader object
            let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).quoting(true).from_reader(file.unwrap());
            
            //get headers -> the names of the fields and construct a new dataframe object
            let headers = rdr.headers()?;
            let mut fieldnames = Vec::new();
            for i in headers.iter(){
                fieldnames.push(i.to_string());
            }                    
            
            //create new dataframe and load data
            let mut dataframe = Dataframe::new(fieldnames);
            //insert csv data into the dataframe skipping starting_line - 1 records
            let mut records_iter = rdr.records().skip(starting_line-1);
            for _iter in 0..(stopping_line-starting_line +1){
                let record = records_iter.next().expect("Failed to get element while iterating");
                dataframe.add_entry(record.unwrap());
            }

            return Ok(dataframe);
        }

        
        pub fn print(&self){
            for i in self.field_indexes.iter(){
                print!("{},",i.0);
            }
            println!();
            self.table.print();
        }

        pub fn print_field(&self ,fieldname:&str){
            println!("{}:",fieldname);
            self.table.print_field(self.field_indexes[fieldname]);
        }


    }
   

}
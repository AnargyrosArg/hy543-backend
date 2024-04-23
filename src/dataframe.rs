pub mod table;

pub mod dataframe{
    use std::{arch::x86_64::_SIDD_CMP_EQUAL_ORDERED, collections::HashMap, error::Error};
    use csv::StringRecord;

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

        
        pub fn read_from_csv(filename:&str) -> Result<Dataframe, Box<dyn Error>>{
            let file = File::open(filename);
            
            //create reader object
            let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).quoting(true).from_reader(file.unwrap());
            
            //get headers -> the names of the fields and construct a new dataframe object
            let headers = rdr.headers()?;
            let mut fieldnames = Vec::new();
            for i in headers.iter(){
                fieldnames.push(i.to_string());
            }                    

            let mut dataframe = Dataframe::new(fieldnames);
            //insert csv data into the dataframe            
            for record in rdr.records(){
                dataframe.add_entry(record.unwrap());
            }

            return Ok(dataframe);
        }


        pub fn print_field(&self ,fieldname:&str){
            println!("{}:",fieldname);
            self.table.print_field(self.field_indexes[fieldname]);
        }
    }
   

}
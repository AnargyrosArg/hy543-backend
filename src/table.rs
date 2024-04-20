pub mod table{
    pub struct Table {
        data: Vec<Vec<(usize,String)>>,
        nfields:usize,
        new_index:usize
    }
    
    impl Table{
        pub fn new(nfields:usize) -> Table{
            
            let mut tab = Table{
                data: Vec::new(),
                nfields:nfields,
                new_index:0,
            };

            for _i in 0..nfields{
                tab.data.push(Vec::new());
            }
            return tab;
        }


        pub fn push(&mut self,mut entry: Vec<String>){
            assert!(self.nfields == entry.len());
            
            for i in (0..self.nfields).rev(){
                self.data[i].push((self.new_index,entry.pop().unwrap()));
            }

            self.new_index = self.data[0].len();
        }


        pub fn print(&self){
            for i in 0..self.data[0].len(){
                for j in 0..self.nfields{
                    print!("{},",self.data[j][i].1);
                }
                println!("");
            }
        }


        
        //TODO create variations of this function that accept functions with various arguements 
        //in order to implement all simple filter statement functionalities

        // filter_numerical (String, number , operation enum)
        // filter_string (String, String, operation enum)

        //given a function that accepts a string and returns a boolean,
        //this method filters out all rows that do not specify that condition for the given field
        pub fn filter(&mut self, fnct:fn(&String) -> bool, field:usize ){
            assert!(field < self.nfields);

            //vector that stores all row indexes that must be kept
            let mut idx_vec:Vec<usize> = Vec::new();
            
            //find all relevant indexes
            for i in 0..self.data[0].len(){
                if fnct(&self.data[field][i].1) {
                    idx_vec.push(self.data[field][i].0);
                }
            }
            //only keep relevant rows for all collumns
            for j in 0..self.nfields{
                self.data[j].retain(|i|idx_vec.contains(&i.0));
            }
        }  
    }

}

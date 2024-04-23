pub mod table{
    use std::collections::BTreeMap;
    pub enum FilterOpcodes {
        Equal,
        Greater,
        GreaterEqual,
        Less,        
        LessEqual
    }


    pub struct Table {
        data: Vec<BTreeMap<usize,String>>,
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
                tab.data.push(BTreeMap::new());
            }
            return tab;
        }


        pub fn push(&mut self,mut entry: Vec<String>){
            assert!(self.nfields == entry.len());
            
            for i in (0..self.nfields).rev(){
                self.data[i].insert(self.new_index, entry.pop().unwrap());
            }

            self.new_index = self.new_index+1;
        }


        pub fn print(&self){
            for i in self.data[0].keys(){
                for j in 0..self.nfields{
                    print!("{},",&self.data[j][i]);
                }
                println!("");
             }     
        }

        pub fn print_field(&self,field:usize){
            for i in self.data[field].keys(){
                println!("{}",self.data[field][i]);
            }
        }

        // filter_string (String, String, operation enum)
        //and performs the appropriate operation
        pub fn filter_string(&mut self, str:&String , opcode:FilterOpcodes,field:usize) -> Vec<usize>{
            assert!(field < self.nfields);

            //vector that stores all row indexes that must be kept
            let mut idx_vec:Vec<usize> = Vec::new();

            //filter all relevant indexes
            for i in self.data[field].keys(){
                if Self::compare_with_opcode_string(&self.data[field][i],str,&opcode) {
                    idx_vec.push(*i);
                }
            }
            return idx_vec;
        }   

        //given a string field that represents a numerical value this function converts it to i32 type
        //and performs the appropriate operation
        pub fn filter_numerical(&mut self, num:i32 , opcode:FilterOpcodes,field:usize) -> Vec<usize>{
            assert!(field < self.nfields);

            //vector that stores all row indexes that must be kept
            let mut idx_vec:Vec<usize> = Vec::new();

            //find all relevant indexes
            for i in self.data[field].keys(){
                if Self::compare_with_opcode_num(self.data[field][i].parse::<i32>().unwrap(),num,&opcode) {
                    idx_vec.push(*i);
                }
            }           
            return idx_vec;
        }

        pub fn apply_intermediate_result(&mut self, idx_vec:Vec<usize>) ->  &Table {
            for j in 0..self.nfields{
                self.data[j].retain(|i,_| idx_vec.contains(&i));
            }

            return self;
        }

        




        
        //Utility function that performs the appropriate operation on numerical values given an opcode 
        #[inline]
        fn compare_with_opcode_num(num1:i32, num2:i32, opcode:&FilterOpcodes) -> bool{
            match opcode{
                FilterOpcodes::Equal => return num1 == num2,
                FilterOpcodes::Greater => return num1 > num2,
                FilterOpcodes::GreaterEqual => return num1 >= num2,
                FilterOpcodes::Less => return num1 < num2,
                FilterOpcodes::LessEqual => return num1 <= num2,
            }
        }
        //Utility function that performs the appropriate operation on numerical values given an opcode 
        #[inline]
        fn compare_with_opcode_string(str1:&String, str2:&String, opcode:&FilterOpcodes) -> bool{
            match opcode{
                FilterOpcodes::Equal => return str1 == str2,
                FilterOpcodes::Greater => return str1 > str2,
                FilterOpcodes::GreaterEqual => return str1 >= str2,
                FilterOpcodes::Less => return str1 < str2,
                FilterOpcodes::LessEqual => return str1 <= str2,
            }
        }
    }

}

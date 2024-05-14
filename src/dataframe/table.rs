pub mod table {
    pub enum FilterOpcodes {
        Equal,
        Greater,
        GreaterEqual,
        Less,
        LessEqual,
    }

    pub struct Table {
        data: Vec<Vec<(usize, String)>>,
        projections: Vec<bool>,
        nfields: usize,
        new_index: usize,
    }

    impl Table {
        pub fn new(nfields: usize) -> Table {
            let mut tab = Table {
                data: Vec::new(),
                projections: Vec::new(),
                nfields: nfields,
                new_index: 0,
            };

            for _i in 0..nfields {
                tab.data.push(vec![]);
                tab.projections.push(false);
            }
            return tab;
        }
        pub fn len(&self) -> usize {
            return self.data[0].len();
        }

        pub fn push(&mut self, mut entry: Vec<String>) {
            assert!(self.nfields == entry.len());

            for i in (0..self.nfields).rev() {
                self.data[i].push((self.new_index, entry.pop().unwrap()));
            }

            self.new_index = self.new_index + 1;
        }

        pub fn select_projection(&mut self, field: usize) {
            self.projections[field] = true;
        }

        // pub fn is_projected(&self, idx: usize) -> bool {
        //     return self.projections[idx];
        // }

        // pub fn print(&self) {
        //     for i in self.data[0].keys() {
        //         for j in 0..self.nfields {
        //             if self.projections[j] == true {
        //                 print!("{},", &self.data[j][i]);
        //             } else {
        //                 continue;
        //             }
        //         }
        //         println!("");
        //     }
        // }

        // filter_string (String, String, operation enum)
        //and performs the appropriate operation
        pub fn filter_string(
            &mut self,
            str: &String,
            opcode: FilterOpcodes,
            field: usize,
        ) -> Vec<usize> {
            assert!(field < self.nfields);

            //vector that stores all row indexes that must be kept
            let mut idx_vec: Vec<usize> = Vec::new();

            //filter all relevant indexes
            for i in &self.data[field] {
                if Self::compare_with_opcode(&i.1, str, &opcode) {
                    idx_vec.push(i.0);
                }
            }
            return idx_vec;
        }

        //given a string field that represents a numerical value this function converts it to i64 type
        //and performs the appropriate operation
        pub fn filter_numerical(
            &mut self,
            num: i64,
            opcode: FilterOpcodes,
            field: usize,
        ) -> Vec<usize> {
            assert!(field < self.nfields);

            //vector that stores all row indexes that must be kept
            let mut idx_vec: Vec<usize> = Vec::new();

            //find all relevant indexes
            for i in (&self.data[field]).iter().enumerate() {
                if Self::compare_with_opcode(
                    i.1.1.parse::<i64>().unwrap(),
                    num,
                    &opcode,
                ) {
                    idx_vec.push(i.0);
                }
            }
            return idx_vec;
        }

        pub fn sum_field(&self, field_idx: usize) -> i64 {
            return self.data[field_idx]
                .iter()
                .map(|x| x.1.parse::<i64>().expect("Not an numerical value!"))
                .into_iter()
                .sum();
        }

        pub fn apply_intermediate_result(&mut self, idx_vec: &Vec<usize>) -> &Table {

            let mut new_data: Vec<Vec<(usize, String)>> =  Vec::new();

            for j in 0..self.nfields {
                new_data.push(Vec::new());
                for i in idx_vec{
                    let row = self.data[j].swap_remove(*i);
                    new_data[j].push(row);
                }   
                
            }

            self.data = new_data;
            return self;
        }

        //Utility function that performs the appropriate comparison operation
        #[inline]
        fn compare_with_opcode<T>(num1: T, num2: T, opcode: &FilterOpcodes) -> bool
        where
            T: std::cmp::Ord,
        {
            match opcode {
                FilterOpcodes::Equal => return num1 == num2,
                FilterOpcodes::Greater => return num1 > num2,
                FilterOpcodes::GreaterEqual => return num1 >= num2,
                FilterOpcodes::Less => return num1 < num2,
                FilterOpcodes::LessEqual => return num1 <= num2,
            }
        }
    }
}

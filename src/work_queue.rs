
pub mod work_queue{
    pub struct Queue {
        queue: Vec<i32>, 
    }
    
    impl Queue{
        pub fn new() -> Queue{
            return Queue{queue: Vec::new()}
        }

        pub fn push(&mut self,val:i32){
            self.queue.push(val);
        }
    
        pub fn pop(&mut self) -> i32{
            return self.queue.pop().unwrap();
        }
    
        pub fn size(&self) -> usize {
            return self.queue.len();
        }
        
    }

}

pub mod work_loader {
    use crate::work_queue::work_queue::Queue;

    pub struct WorkLoader {}

    impl WorkLoader {
        pub fn load_work(self, queue: &mut Queue) {
            queue.push(1);
            queue.push(2);
            queue.push(3);
            queue.push(4);
            queue.push(5);
        }
    }
}

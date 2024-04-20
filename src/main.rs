mod work_queue;
use crate::work_queue::work_queue::Queue;

mod work_loader;
use table::table::Table;
use work_loader::work_loader::WorkLoader;

mod table;


fn main() {
    worker();
}


fn worker(){
    let mut queue:Queue = Queue::new();

    let loader:WorkLoader = WorkLoader{};

    loader.load_work(&mut queue);

    let mut table:Table = Table::new(3);

    table.push(vec![String::from("Anargyros1"),String::from("Kwstantina1"),String::from("Xristina1")]);
    table.push(vec![String::from("Anargyros2"),String::from("Kwstantina2"),String::from("Xristina2")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3")]);
    
    println!("==============================");
    table.print();



    table.filter(select_oper, 0);

    println!("==============================");
    table.print();

}



fn select_oper(str:&String) -> bool{
    if str == "Anargyros3"{
        println!("found");
        return true;
    }else{
        return false;
    }
}
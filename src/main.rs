mod dataframe;
use dataframe::dataframe::Dataframe;


fn main() {
    worker();
}


fn worker(){
    let dataframe:Dataframe = Dataframe::read_from_csv("deniro.csv",1,10).unwrap();
    
    dataframe.print();

    // let mut table:Table = Table::new(4);

    // table.push(vec![String::from("Anargyros1"),String::from("Kwstantina1"),String::from("Xristina1"),String::from("1")]);
    // table.push(vec![String::from("Anargyros2"),String::from("Kwstantina2"),String::from("Xristina2"),String::from("2")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("3")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("1")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("2")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("3")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("1")]);
    // table.push(vec![String::from("Anargyros3"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("2")]);
    // table.push(vec![String::from("Anargyros2"),String::from("Kwstantina3"),String::from("Xristina3"),String::from("3")]);
    
    // println!("==============================");
    // table.print();
    // println!("==============================");

    // let intermediate = table.filter_numerical(3, FilterOpcodes::Equal, 3);
    // table.apply_intermediate_result(intermediate);

    // table.print();
    // println!("==============================");

    // let intermediate = table.filter_string(&"Anargyros3".to_string(), FilterOpcodes::Equal, 0);
    // table.apply_intermediate_result(intermediate);

    // println!("==============================");
    // table.print();
}
pub fn get_parameter_from_query(query: &str, name_of_parameter: &str) -> String {
    use regex::Regex;
    let regex = Regex::new(r"[?&]([^=#]+)=([^&#]*)").unwrap();

    for caps in regex.captures_iter(query) {
        if let Some(cap) = caps.get(1) {
            if cap.as_str() == name_of_parameter {
                if let Some(id) = caps.get(2) {
                    return id.as_str().to_owned();
                }
            }
        }
    }

    panic!(
        "Cant get parameter {} from query {} ",
        name_of_parameter, query
    );
}

#[macro_export]
macro_rules! hashmap {
    ( $($x:expr => $y:expr),* ) => ({
        let mut temp_map = hashbrown::HashMap::new();
        $(
            temp_map.insert($x, $y);
        )*
        temp_map
    });
    ( $($x:expr => $y:expr,)* ) => (
        hashmap!{$($x => $y),*}
    );
}

pub mod eqsat;
pub mod term;
pub mod term_sexp;
use crate::eqsat::*;
use crate::term_sexp::*;
use std::collections::HashSet;

fn main() {
    let timer = std::time::Instant::now();

    let nodes: Vec<Node> = saturate(read_term(
        &std::env::args()
            .nth(1)
            .unwrap_or("(+ (+ (. 1) (. 2)) (. 3))".to_string()),
    ));
    let classes: HashSet<Class> = nodes.iter().map(|n| n.class).collect();

    println!("{} classes, {} nodes", classes.len(), nodes.len());
    println!("{:?}", timer.elapsed());
}

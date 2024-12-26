use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use sexp::Sexp;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Class(usize);

fn next_class() -> Class {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    Class(COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Node {
    term: Term<Class>,
    class: Class,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Term<T> {
    Var(String),
    Lit(i64),
    Add(T, T),
    Sub(T, T),
    Mul(T, T),
    Div(T, T),
    Sqrt(T),
}

fn map_term<S, T>(t: &Term<S>, f: &dyn Fn(&S) -> T) -> Term<T> {
    use Term::*;
    match t {
        Var(x) => Var(x.clone()),
        Lit(n) => Lit(*n),
        Add(a, b) => Add(f(a), f(b)),
        Sub(a, b) => Sub(f(a), f(b)),
        Mul(a, b) => Mul(f(a), f(b)),
        Div(a, b) => Div(f(a), f(b)),
        Sqrt(a) => Sqrt(f(a)),
    }
}

fn arity_term<T>(t: &Term<T>) -> usize {
    use Term::*;
    match t {
        Var(_) => 0,
        Lit(_) => 0,
        Add(_, _) => 2,
        Sub(_, _) => 2,
        Mul(_, _) => 2,
        Div(_, _) => 2,
        Sqrt(_) => 1,
    }
}

fn get_in_term<T: Clone>(t: &Term<T>, i: usize) -> Option<T> {
    use Term::*;
    match (t, i) {
        (Add(r, _), 0) => Some(r.clone()),
        (Add(_, r), 1) => Some(r.clone()),
        (Sub(r, _), 0) => Some(r.clone()),
        (Sub(_, r), 1) => Some(r.clone()),
        (Mul(r, _), 0) => Some(r.clone()),
        (Mul(_, r), 1) => Some(r.clone()),
        (Div(r, _), 0) => Some(r.clone()),
        (Div(_, r), 1) => Some(r.clone()),
        (Sqrt(r), 0) => Some(r.clone()),
        _ => None,
    }
}
fn set_in_term<T: Clone>(t: &Term<T>, i: usize, n: T) -> Option<Term<T>> {
    use Term::*;
    match (t, i) {
        (Add(_, b), 0) => Some(Add(n, b.clone())),
        (Add(a, _), 1) => Some(Add(a.clone(), n)),
        (Sub(_, b), 0) => Some(Sub(n, b.clone())),
        (Sub(a, _), 1) => Some(Sub(a.clone(), n)),
        (Mul(_, b), 0) => Some(Mul(n, b.clone())),
        (Mul(a, _), 1) => Some(Mul(a.clone(), n)),
        (Div(_, b), 0) => Some(Div(n, b.clone())),
        (Div(a, _), 1) => Some(Div(a.clone(), n)),
        (Sqrt(_), 0) => Some(Sqrt(n)),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum NewTerm {
    Create(Box<Term<NewTerm>>),
    Leaf(Class),
}

#[derive(Clone, Debug)]
struct TTerm(Term<Arc<TTerm>>);

fn to_new_term(t: &TTerm) -> NewTerm {
    let TTerm(t) = t;
    NewTerm::Create(Box::new(map_term(t, &|t| to_new_term(t))))
}

use std::cell::RefCell;

fn collect_created_terms(t: &NewTerm, v: &RefCell<Vec<(Term<Class>, Class)>>) -> Class {
    use once_cell::sync::Lazy;
    use std::{collections::HashMap, sync::Mutex};

    // hash cons table
    static SEEN: Lazy<Mutex<HashMap<Term<Class>, Class>>> =
        Lazy::new(|| Mutex::new(HashMap::new()));

    match t {
        NewTerm::Create(t) => {
            // hash consing - only add new class if unseen
            let tc = map_term(t, &|t| collect_created_terms(&t, v));
            let c = SEEN
                .lock()
                .unwrap()
                .entry(tc.clone())
                .or_insert_with(|| next_class())
                .clone();
            v.borrow_mut().push((tc, c));
            c
        }
        NewTerm::Leaf(c) => *c,
    }
}

fn created_terms(t: &NewTerm, par: Option<Class>) -> Vec<(Term<Class>, Class)> {
    let v: RefCell<Vec<(Term<Class>, Class)>> = RefCell::new(Vec::new());
    collect_created_terms(t, &v);
    if let Some(c) = par {
        v.borrow_mut().last_mut().unwrap().1 = c;
    }
    v.into_inner()
}

enum SubSexpMatch<'a> {
    S(&'a str),
    I(i64),
    T(&'a Sexp),
}

fn sub_sexp_match(e: &Sexp) -> SubSexpMatch {
    match e {
        Sexp::Atom(sexp::Atom::S(s)) => SubSexpMatch::S(s),
        Sexp::Atom(sexp::Atom::I(n)) => SubSexpMatch::I(*n),
        _ => SubSexpMatch::T(e),
    }
}

fn sexp_match<'a>(e: &'a Sexp) -> Vec<SubSexpMatch> {
    match e {
        Sexp::List(vec) => vec
            .iter()
            .map(|e| sub_sexp_match(e))
            .collect::<Vec<SubSexpMatch>>(),
        Sexp::Atom(_) => vec![sub_sexp_match(e)],
    }
}

fn term_of_sexp(e: &Sexp) -> Arc<TTerm> {
    use SubSexpMatch::*;
    use Term::*;
    Arc::new(match sexp_match(&e).as_slice() {
        [S("."), I(n)] => TTerm(Lit(*n)),
        [S("!"), S(x)] => TTerm(Var(x.to_string())),
        [S("+"), T(a), T(b)] => TTerm(Add(term_of_sexp(a), term_of_sexp(b))),
        [S("-"), T(a), T(b)] => TTerm(Sub(term_of_sexp(a), term_of_sexp(b))),
        [S("*"), T(a), T(b)] => TTerm(Mul(term_of_sexp(a), term_of_sexp(b))),
        [S("/"), T(a), T(b)] => TTerm(Div(term_of_sexp(a), term_of_sexp(b))),
        [S("sqrt"), T(a)] => TTerm(Sqrt(term_of_sexp(a))),
        _ => panic!("cannot read term from sexp {}", e),
    })
}

fn sexp_of_term(t: &TTerm) -> Sexp {
    let TTerm(t) = t;
    use sexp::{atom_i, atom_s, list};
    use Term::*;
    match t {
        Lit(n) => atom_i(*n),
        Var(x) => list(&[atom_s("!"), atom_s(x)]),
        Add(a, b) => list(&[atom_s("+"), sexp_of_term(a), sexp_of_term(b)]),
        Sub(a, b) => list(&[atom_s("-"), sexp_of_term(a), sexp_of_term(b)]),
        Mul(a, b) => list(&[atom_s("*"), sexp_of_term(a), sexp_of_term(b)]),
        Div(a, b) => list(&[atom_s("/"), sexp_of_term(a), sexp_of_term(b)]),
        Sqrt(a) => list(&[atom_s("sqrt"), sexp_of_term(a)]),
    }
}

fn read_term(s: &str) -> TTerm {
    (*term_of_sexp(&sexp::parse(s).unwrap())).clone()
}

#[test]
fn test() {
    assert_eq!(
        sexp_of_term(&read_term("(+ (! x) (! y))")).to_string(),
        "(+ (! x) (! y))"
    );
}

fn main() {
    use NewTerm::*;
    use Term::*;

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut inits = worker.dataflow(|scope| {
            // let init_terms = terms_for_init.into_iter().to_stream(scope);
            // let (mut nodes_input, init_nodes) = scope.new_collection();
            let (init_terms_input, init_terms) = scope.new_collection();

            let final_nodes: Collection<_, Node> = init_terms
                .map(|(term, class)| Node { term, class })
                .iterate(|nodes: &Collection<_, Node>| {
                    // nodes.inspect(|x| println!("{:?}", x));

                    // e-matching
                    let adds = nodes.filter(|n| match n.term {
                        Add(_, _) => true,
                        _ => false,
                    });

                    let assoc_rewrites = adds
                        .map(|abc| {
                            (
                                match abc.term {
                                    Add(_, bc) => bc,
                                    _ => panic!(),
                                },
                                abc.clone(),
                            )
                        })
                        .join_map(&adds.map(|n| (n.class, n)), |_, abc, bc| {
                            (
                                abc.class,
                                match abc.term {
                                    Add(a, _) => match bc.term {
                                        Add(b, c) => Create(Box::new(Add(
                                            Create(Box::new(Add(Leaf(a), Leaf(b)))),
                                            Leaf(c),
                                        ))),
                                        _ => panic!(),
                                    },
                                    _ => panic!(),
                                },
                            )
                        });

                    let comm_rewrites = adds.map(|n| {
                        (
                            n.class,
                            match n.term {
                                Add(a, b) => Create(Box::new(Add(Leaf(b), Leaf(a)))),
                                _ => panic!(),
                            },
                        )
                    });

                    // collect rewrites, expand e-graph
                    let rewrites = comm_rewrites
                        .concat(&assoc_rewrites)
                        .map(|(c, n)| created_terms(&n, Some(c)));

                    let new_nodes = rewrites
                        .flat_map(|x| x)
                        .map(|(t, c)| Node { term: t, class: c });

                    let nodes = nodes.concat(&new_nodes).distinct();

                    // collapse e-classes
                    let repr = nodes
                        .map(|n| (n.term, n.class))
                        .reduce(|_key, input, output| {
                            let r = *input[0].0;
                            for (x, _) in input {
                                output.push(((**x, r), 1));
                            }
                        })
                        .map(|(_witness, edge)| edge)
                        .iterate(|repr| {
                            repr.map(|(child, parent)| (parent, child))
                                .join_map(repr, |_parent, child, grandparent| {
                                    (*child, *grandparent)
                                })
                                .distinct()
                        });
                    // .inspect(|x| println!("collapsed {:?}", x));

                    // rebuild e-graph

                    // - update term classes
                    let nodes = nodes
                        .map(|n| (n.class, n))
                        .join_map(&repr, |_, n, c| Node {
                            term: n.term.clone(),
                            class: *c,
                        })
                        .distinct();

                    // - update subterm classes: nodes must be a set at this point!
                    let updates = nodes
                        .flat_map(|n| {
                            (0..arity_term(&n.term))
                                .map(move |i| (get_in_term(&n.term, i).unwrap(), (n.clone(), i)))
                        })
                        .join_map(&repr, |_, (n, i), c| (n.clone(), (*i, *c)))
                        .reduce(|n, input, output| {
                            let mut t = n.term.clone();
                            for ((i, c), _) in input {
                                t = set_in_term(&t, *i, *c).unwrap();
                            }
                            output.push((
                                Node {
                                    term: t,
                                    class: n.class,
                                },
                                1,
                            ))
                        });

                    let nodes = nodes
                        .concat(&updates.map(|(_old, new)| new))
                        .concat(&updates.map(|(old, _new)| old).negate());

                    nodes.distinct()
                });
            // println!("saturated");
            let _ = final_nodes.inspect(|x| println!("final: {:?}", x));

            return init_terms_input;
        });

        inits.advance_to(0);
        for (t, c) in created_terms(
            &to_new_term(&read_term(
                // "(+ (. 1) (. 2))",
                "(+ (+ (. 1) (. 2)) (. 3))",
                // "(+ (+ (+ (. 7) (. 3)) (+ (. 4) (. 5))) (+ (. 1) (+ (. 2) (. 6))))",
            )),
            None,
        ) {
            inits.insert((t, c));
        }

        for (t, c) in created_terms(
            &to_new_term(&read_term(
                // "(+ (. 2) (. 1))",
                "(+ (+ (. 3) (. 2)) (. 1))",
                // "(+ (. 1) (+ (. 2) (+ (. 3) (+ (. 4) (+ (. 5) (+ (. 6) (+ (. 7) (. 8))))))))",
            )),
            None,
        ) {
            inits.insert((t, c));
        }
    })
    .expect("Computation terminated abnormally");
}

use abomonation_derive::Abomonation;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use sexp::Sexp;
use std::sync::Arc;
use timely::dataflow::operators::ToStream;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Serialize, Deserialize,
)]
struct Class(usize);

fn next_class() -> Class {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    Class(COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Serialize, Deserialize,
)]
struct Node {
    term: Term<Class>,
    class: Class,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Serialize, Deserialize,
)]
enum Term<T> {
    Var(String),
    Lit(i64),
    Add(T, T),
    Sub(T, T),
    Mul(T, T),
    Div(T, T),
    Sqrt(T),
}

fn map_term<S, T>(t: Term<S>, f: &dyn Fn(S) -> T) -> Term<T> {
    use Term::*;
    match t {
        Var(x) => Var(x),
        Lit(n) => Lit(n),
        Add(a, b) => Add(f(a), f(b)),
        Sub(a, b) => Sub(f(a), f(b)),
        Mul(a, b) => Mul(f(a), f(b)),
        Div(a, b) => Div(f(a), f(b)),
        Sqrt(a) => Sqrt(f(a)),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Abomonation, Serialize, Deserialize)]
enum NewTerm {
    Create(Box<Term<NewTerm>>),
    Leaf(Class),
}

struct TTerm(Term<Arc<TTerm>>);

fn to_new_term(t: &TTerm) -> NewTerm {
    let TTerm(t) = t;
    NewTerm::Create(Box::new(map_term(t.clone(), &|t| to_new_term(&t))))
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
            let tc = map_term(*t.clone(), &|t| collect_created_terms(&t, v));
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

fn term_of_sexp_(e: &Sexp) -> TTerm {
    use SubSexpMatch::*;
    use Term::*;
    match sexp_match(&e).as_slice() {
        [S("."), I(n)] => TTerm(Lit(*n)),
        [S("!"), S(x)] => TTerm(Var(x.to_string())),
        [S("+"), T(a), T(b)] => TTerm(Add(term_of_sexp(a), term_of_sexp(b))),
        [S("-"), T(a), T(b)] => TTerm(Sub(term_of_sexp(a), term_of_sexp(b))),
        [S("*"), T(a), T(b)] => TTerm(Mul(term_of_sexp(a), term_of_sexp(b))),
        [S("/"), T(a), T(b)] => TTerm(Div(term_of_sexp(a), term_of_sexp(b))),
        [S("sqrt"), T(a)] => TTerm(Sqrt(term_of_sexp(a))),
        _ => panic!("cannot read term from sexp {}", e),
    }
}

fn term_of_sexp(s: &Sexp) -> Arc<TTerm> {
    Arc::new(term_of_sexp_(s))
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
    term_of_sexp_(&sexp::parse(s).unwrap())
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
                    // let norm = nodes
                    //     .map(|n| (n.term, n.class))
                    //     .reduce(|_, input, output| for (t, cs) in input {});

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

                    let _rewrites = comm_rewrites
                        .concat(&assoc_rewrites)
                        .map(|(c, n)| created_terms(&n, Some(c)))
                        .inspect(|x| println!("{:?}", x))
                        .flat_map(|x| x)
                        .map(|(t, c)| Node { term: t, class: c });

                    nodes.distinct()
                });
            println!("saturated");
            let _ = final_nodes.inspect(|x| println!("{:?}", x));

            return init_terms_input;
        });

        inits.advance_to(0);
        let terms_for_init = created_terms(
            &to_new_term(&read_term(
                "(+ (+ (+ (. 7) (. 3)) (+ (. 4) (. 5))) (+ (. 1) (+ (. 2) (. 6))))",
            )),
            None,
        );
        for (t, c) in terms_for_init {
            inits.insert((t, c));
        }
    })
    .expect("Computation terminated abnormally");
}

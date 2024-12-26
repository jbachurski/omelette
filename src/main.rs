use abomonation_derive::Abomonation;
use differential_dataflow::{input::InputSession, Collection};
use sexp::Sexp;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
struct ClassId(usize);

fn get_class_id() -> ClassId {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    ClassId(COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Abomonation)]
struct Node {
    term: Term<Class>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
struct Class {
    id: ClassId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Abomonation)]
enum Term<T> {
    Var(String),
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
        Add(a, b) => Add(f(a), f(b)),
        Sub(a, b) => Sub(f(a), f(b)),
        Mul(a, b) => Mul(f(a), f(b)),
        Div(a, b) => Div(f(a), f(b)),
        Sqrt(a) => Sqrt(f(a)),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Abomonation)]
enum NewTerm {
    Create(Box<Term<NewTerm>>),
    Leaf(Class),
}

struct TTerm(Term<Arc<TTerm>>);

enum SubSexpMatch<'a> {
    S(&'a str),
    T(&'a Sexp),
}

fn sub_sexp_match(e: &Sexp) -> SubSexpMatch {
    match e {
        Sexp::Atom(sexp::Atom::S(s)) => SubSexpMatch::S(s),
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
        [S("!"), S(x)] => TTerm(Var(x.to_string())),
        [S("+"), T(a), T(b)] => TTerm(Add(term_of_sexp(a), term_of_sexp(b))),
        [S("-"), T(a), T(b)] => TTerm(Sub(term_of_sexp(a), term_of_sexp(b))),
        [S("*"), T(a), T(b)] => TTerm(Mul(term_of_sexp(a), term_of_sexp(b))),
        [S("/"), T(a), T(b)] => TTerm(Div(term_of_sexp(a), term_of_sexp(b))),
        [S("sqrt"), T(a)] => TTerm(Sqrt(term_of_sexp(a))),
        _ => panic!(),
    }
}

fn term_of_sexp(s: &Sexp) -> Arc<TTerm> {
    Arc::new(term_of_sexp_(s))
}

pub fn sexp_of_term(t: &TTerm) -> Sexp {
    let TTerm(t) = t;
    use sexp::{atom_s, list};
    match t {
        Term::Var(x) => list(&[atom_s("!"), atom_s(x)]),
        Term::Add(a, b) => list(&[atom_s("+"), sexp_of_term(a), sexp_of_term(b)]),
        Term::Sub(a, b) => list(&[atom_s("-"), sexp_of_term(a), sexp_of_term(b)]),
        Term::Mul(a, b) => list(&[atom_s("*"), sexp_of_term(a), sexp_of_term(b)]),
        Term::Div(a, b) => list(&[atom_s("/"), sexp_of_term(a), sexp_of_term(b)]),
        Term::Sqrt(a) => list(&[atom_s("sqrt"), sexp_of_term(a)]),
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
    use differential_dataflow::operators::Join;
    use NewTerm::*;
    use Term::*;

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut init_nodes = InputSession::new();

        fn new_node(term: Term<Class>) -> Node {
            Node { term }
        }

        worker.dataflow(|scope| {
            let nodes: Collection<_, Node> = init_nodes.to_collection(scope);
            let class_node = nodes.map(|n| (Class { id: get_class_id() }, n));

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
                .join_map(&class_node, |_, abc, bc| {
                    (
                        abc.clone(),
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
                    n.clone(),
                    match n.term {
                        Add(a, b) => Create(Box::new(Add(Leaf(b), Leaf(a)))),
                        _ => panic!(),
                    },
                )
            });
        });

        init_nodes.advance_to(0);
        init_nodes.insert(new_node(Var("x".to_string())));
    })
    .expect("Computation terminated abnormally");
}

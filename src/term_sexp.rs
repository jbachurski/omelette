use crate::term::*;
use sexp::Sexp;
use std::sync::Arc;

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

fn sexp_match(e: &Sexp) -> Vec<SubSexpMatch> {
    match e {
        Sexp::List(vec) => vec
            .iter()
            .map(|e| sub_sexp_match(e))
            .collect::<Vec<SubSexpMatch>>(),
        Sexp::Atom(_) => vec![sub_sexp_match(e)],
    }
}

pub fn term_of_sexp(e: &Sexp) -> Arc<TTerm> {
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

pub fn sexp_of_term(t: &TTerm) -> Sexp {
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

pub fn read_term(s: &str) -> TTerm {
    (*term_of_sexp(&sexp::parse(s).unwrap())).clone()
}

#[test]
fn test() {
    assert_eq!(
        sexp_of_term(&read_term("(+ (! x) (! y))")).to_string(),
        "(+ (! x) (! y))"
    );
}

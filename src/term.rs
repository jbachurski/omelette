use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Term<T> {
    Var(String),
    Lit(i64),
    Add(T, T),
    Sub(T, T),
    Mul(T, T),
    Div(T, T),
    Sqrt(T),
}

pub fn map_term<S, T>(t: &Term<S>, f: &dyn Fn(&S) -> T) -> Term<T> {
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

pub fn arity_term<T>(t: &Term<T>) -> usize {
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

pub fn get_in_term<T: Clone>(t: &Term<T>, i: usize) -> Option<T> {
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

pub fn set_in_term<T: Clone>(t: &Term<T>, i: usize, n: T) -> Option<Term<T>> {
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

#[derive(Clone, Debug)]
pub struct TTerm(pub Term<Arc<TTerm>>);

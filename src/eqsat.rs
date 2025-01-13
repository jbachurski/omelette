use crate::term::*;
use arrange::ArrangeByKey;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::capture::{Capture, Extract};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Class(usize);

fn next_class() -> Class {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    Class(COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub term: Term<Class>,
    pub class: Class,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum NewTerm {
    Create(Box<Term<NewTerm>>),
    Leaf(Class),
}

fn to_new_term(t: &TTerm) -> NewTerm {
    let TTerm(t) = t;
    NewTerm::Create(Box::new(map_term(t, &|t| to_new_term(t))))
}

use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Mutex;

// hash cons table
static SEEN: Lazy<Mutex<HashMap<Term<Class>, Class>>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn collect_created_terms(t: &NewTerm, v: &RefCell<Vec<(Term<Class>, Class)>>) -> Class {
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

pub fn saturate(initial_term: TTerm) -> Vec<Node> {
    use NewTerm::*;
    use Term::*;

    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut inits = worker.dataflow(|scope| {
            // let init_terms = terms_for_init.into_iter().to_stream(scope);
            // let (mut nodes_input, init_nodes) = scope.new_collection();
            let (init_terms_input, init_terms) = scope.new_collection();

            let final_nodes: Collection<_, Node> = init_terms
                .map(|(term, class)| Node { term, class })
                .iterate(|nodes: &Collection<_, Node>| {
                    // nodes.inspect(|x| println!("{:?}", x));

                    nodes.consolidate().inspect(|(n, _, _)| {
                        SEEN.lock()
                            .unwrap()
                            .entry(n.term.clone())
                            .insert_entry(n.class);
                    });

                    // e-matching
                    let adds = nodes.filter(|n| match n.term {
                        Add(_, _) => true,
                        _ => false,
                    });

                    let assoc_rl_rewrites = adds
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

                    let assoc_lr_rewrites = adds
                        .map(|abc| {
                            (
                                match abc.term {
                                    Add(ab, _) => ab,
                                    _ => panic!(),
                                },
                                abc.clone(),
                            )
                        })
                        .join_map(&adds.map(|n| (n.class, n)), |_, abc, ab| {
                            (
                                abc.class,
                                match abc.term {
                                    Add(_, c) => match ab.term {
                                        Add(a, b) => Create(Box::new(Add(
                                            Leaf(a),
                                            Create(Box::new(Add(Leaf(b), Leaf(c)))),
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
                        .concat(&assoc_rl_rewrites)
                        .concat(&assoc_lr_rewrites);

                    let new_nodes = rewrites
                        .flat_map(|(c, n)| created_terms(&n, Some(c)))
                        .map(|(t, c)| Node { term: t, class: c });

                    let nodes = nodes.concat(&new_nodes).distinct();

                    nodes.iterate(|nodes| {
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
                            })
                            .arrange_by_key();
                        // .inspect(|x| println!("collapsed {:?}", x));

                        // rebuild e-graph

                        // - update term classes
                        let nodes = nodes
                            .map(|n| (n.class, n))
                            .join_core(&repr, |_, n, c| {
                                Some(Node {
                                    term: n.term.clone(),
                                    class: *c,
                                })
                            })
                            .distinct();

                        // - update subterm classes: nodes must be a set at this point!
                        let updates = nodes
                            .flat_map(|n| {
                                (0..arity_term(&n.term)).map(move |i| {
                                    (get_in_term(&n.term, i).unwrap(), (n.clone(), i))
                                })
                            })
                            .join_core(&repr, |_, (n, i), c| Some((n.clone(), (*i, *c))))
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
                    })
                });

            let send = send.lock().unwrap().clone();
            final_nodes.consolidate().inner.capture_into(send);

            return init_terms_input;
        });

        // println!("{:?}\tdefined", timer.elapsed());

        inits.advance_to(0);

        for (t, c) in created_terms(&to_new_term(&initial_term), None) {
            inits.insert((t, c));
        }

        // println!("{:?}\tgiven", timer.elapsed());
    })
    .expect("Computation terminated abnormally");

    recv.extract()
        .into_iter()
        .flat_map(|(_time, c)| {
            // println!("time {}", _time);
            c.into_iter().map(|(node, _, cnt)| {
                assert!(cnt == 1);
                // println!("{:?}", node);
                node
            })
        })
        .collect()
}

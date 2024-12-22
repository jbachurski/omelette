extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;

fn main() {
    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args(), move |worker| {
        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        worker.dataflow(|scope| {
            // create a new collection from our input.
            let manages = input.to_collection(scope);

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .map(|(m2, m1)| (m1, m2))
                .join(&manages)
                .inspect(|x| println!("{:?}", x));
        });

        // Set a size for our organization from the input.
        let size = std::env::args()
            .nth(1)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(10);

        // Load input (a binary tree).
        input.advance_to(0);
        for person in 0..size {
            input.insert((person / 2, person));
        }
    })
    .expect("Computation terminated abnormally");
}

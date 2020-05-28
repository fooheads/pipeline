# pipeline

**STATUS**: *Pre-alpha*, in design and prototyping phase.

Description will come later. See tests for examples.


## TODO

* [x] Move examples from test to examples
* [x] Replace malli with spec
* [x] Add action and transformation construction functions
* [ ] Add missing accessors
* [ ] add pipeline binding and get-binding accessor
* [ ] add step binding and get-step-binding and a second optional argument to get-binding
* [ ] param "spec" (required keys and preds)
* [ ] print/return pipeline (static structure)
* [ ] print/return pipeline run (results of steps)
* [ ] print-call, not only print-failed-call (for putting in logs)
* [ ] allow both simple and qualified keywords as well as paths as input and output
* [ ] validate pipeline w/ explain
* [ ] repl support for platform objects, e.g. data sources
* [ ] option can contain "spec" validation function (malli, spec, schema, ...)
* [ ] store only changed "keys" in trace
* [ ] validate dependencies (all should resolve)
* [ ] fork/join, paralellisation when executing





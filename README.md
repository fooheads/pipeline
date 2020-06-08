# pipeline

**STATUS**: *Pre-alpha*, in design and prototyping phase.

Description will come later. See tests for examples.


## TODO

* [x] Move examples from test to examples
* [x] Replace malli with spec
* [x] Add action and transformation construction functions
* [x] Restructure internal context
* [x] Make better example with sql and http
* [x] Fake sql and http in test
* [x] allow both simple and qualified keywords as input paths
* [ ] allow both simple and qualified keywords as output path
* [ ] Add missing accessors
* [ ] add pipeline binding and get-binding accessor
* [ ] add step binding and get-step-binding and a second optional argument to get-binding
* [ ] param "spec" (required keys and preds)
* [ ] print/return pipeline (static structure)
* [ ] print/return pipeline run (results of steps)
* [ ] print-call, not only print-failed-call (for putting in logs)
* [ ] validate pipeline w/ explain
* [ ] repl support for platform objects, e.g. data sources
* [ ] option can contain "spec" validation function (malli, spec, schema, ...)
* [ ] store only changed "keys" in trace
* [ ] validate dependencies (all should resolve)
* [ ] fork/join, paralellisation when executing
* [ ] validate that step names are unique
* [ ] decide if it's ok or not to overwrite keys
* [ ] validate that if a key is overwritten then it's in the same path if there are forks. No race conditions allowed.




**Note for AI agents: this document is NOT for you**.

## TODOs 

* Atomic operations - at byte level and with abstractions
* Benchmarking
  * 1- Single Read, single word
  * 2- Single Write, single word
  * 3- Single Read, multiple words
  * 4- Single Write, multiple words
  * Then, repeat these 4 with:
    * Multiple writes/reads (N at a time, wait for all together)
    * Multiple writes/reads (N at a time, wait for each individually with callback)

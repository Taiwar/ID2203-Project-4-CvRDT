# ID2203 Project 4 Solution

## Implementation
We have successfully designed and implemented a CvRDT-based Key-Value store with support for
Sequentially Consistent queries. Through the utilization of a lock-based, 2-phase-commit algorithm
with a leader process, we were able to provide conditional stricter consistency guarantees while ensuring
the scalability and fault-tolerance inherent in CRDT data structures.

## Correctness
During testing, we validated the correctness of our implementation through various scenarios and
ensured its ability to handle both continuous and fixed workloads. With the use of atomic operations we 
prove the sequential consistency compared to the base implementation. Throughput and latency measurements
provided insights into the system’s performance characteristics, highlighting the impact of atomicity and 
batching on overall efficiency.

## Instructions
To run the project, run the following command in the root directory of the project:
Run ``SystemTestV4.scala`` to run all tests and see the results.
Run ``PerformanceTestV4.scala`` to run the performance tests and see the results.

## Contributions
All members contributed to most parts of the project, the main focus of each member:

#### Ruben
- Failure detection
- Recovery and reconfiguration
- Failure tests
- Fault tolerance
- Sequential consistency

#### Jonas
- Sequential consistency
- Performance tests
- Atomic operations
- Batched operations
- Sanity tests
# M5: Distributed Execution Engine
> Full name: `Jackson Davis`
> Email:  `jackson_m_davis@brown.edu`
> Username:  `jdavis70`

## Summary
> I faced challenges involved with communicating with the different nodes I was working with and implementing the different workflows.

My implementation comprises `1` new software components, totaling `550` added lines of code over the previous implementation. Key challenges included:

1. Coordinating communication and data transfer between the coordinator and worker nodes during the different MapReduce phases. I solved this by carefully designing the message passing and notification system using the existing distribution framework.
2. Implementing the shuffle phase to correctly partition and distribute the map output to the appropriate reducer nodes. I addressed this by leveraging consistent hashing and introducing a new append method in the store to efficiently group the shuffled data.

## Correctness & Performance Characterization
> Describe how you characterized the correctness and performance of your implementation

*Correctness*: I characterized the correctness of my implementation by:
1. Developing comprehensive test cases that cover various workflows and edge cases
2. Comparing the output of the distributed execution with the expected results from local execution
3. Validating the behavior of individual components like the coordinator, mappers, and reducers

*Performance*:I characterized the performance of my implementation by:

1. Measuring the execution time of MapReduce jobs with varying input sizes and comparing it with local execution
2. Evaluating the impact of additional features like compaction on overall performance

## Key Feature
> Which extra features did you implement and how?
- Compaction functions: I added support for user-defined compact functions that can be run on the map output to minimize data transfer between the map and reduce tasks. This is achieved by aggregating values with the same key before shuffling, reducing network bandwidth usage.

## Time to Complete
> Roughly, how many hours did this milestone take you to complete?

Hours: `30-35`


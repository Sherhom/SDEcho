# SDEcho: Efficient Explanation of Aggregated Sequence Difference 

We introduced a novel framework for explanation searching, SDEcho, which addresses the challenge of explaining aggregated sequence differences. SDEcho employs pruning techniques from three perspectives – pattern, order, and dimension – to ensure concise and accurate explanations while considering the characteristics of each pruning algorithm for hybrid optimization. 

## Directory Layout

- `DataInput/` - this section will introduce you how to input dataset that we use in the experiment.

- `SDEcho/` - this section contains all the files necessary to the implementation of SDEcho and will introduce you how to use SDEcho for Sequential Explanation Queries.

- `baseline/` - this section contains all the files necessary to the implementation of baselines ( `BOExplain` `TSExplain` `DIFF` ) and will introduce you how to use them for Sequential Explanation Queries.

- We provided 11 queries distributed over 4 datasets to compare the performance of `SDEcho` and baselines( `BOExplain` `TSExplain` `DIFF` )


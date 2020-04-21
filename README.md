# Ethereum-Analysis

The goal is to analyse the full set of transactions which have occurred on the Ethereum network; from the first transactions (14-02-2016) till 30-06-2019

### Part A - Time Analysis:

Bar plot showing the number of transactions which occurred every month between the start and end of the dataset.

### Part B - Top Ten most popular services:

To evaluate the top 10 smart contracts by total Ether received.

JOB 1 - INITIAL AGGREGATION

To workout which services are the most popular,first have to aggregate transactions to see how much each address within the user space has been involved in. 

JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING

Once obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contractsi.e.,to join the to_address field from the output of Job 1 with the address field of contracts.

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.

JOB 3 - TOP TEN

Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer.

### Part C - Comparitive Evaluation

Comparison between spark and Map/Reduce jobs

Note: Scam Analysis not complete

For implementation details please have a look at Report.pdf

# InfoFlow
An Apache Spark implementation of the InfoMap community detection algorithm

## How to use

### (Optional)

Run `sbt test` to run unit tests

### Edit config.json:

  `Graph`: file path of the graph file. Currently supported graph file formats are local Pajek net format (.net) and Parquet format. If Parquet format is used, then two Parquet files are required, one holding the vertex information (node index, node name, module index) and one holding edge information (from index, to index, edge weight). The file path points to a Json file with `.parquet` extension, in this format:
```
{
  "Vertex File": "path to vertex file",
  "Edge File": "path to edge file"
}
```

  `spark configs`: Spark related configurations

  `Master`: spark master config, probably `local[*]` or `yarn`

  `PageRank`: Page Rank related parameter values

  `PageRank, tele`: probability of teleportation in PageRank, equal 1-(damping factor)

  `PageRank, error threshold factor`: PageRank termination condition is set to termination when Euclidean distance between previous vector and current vector is within 1/(node number)/(PageRank error threshold factor).

  `Community Detection`: community detection algorithm parametersInfoFlow

  `name`: InfoFlow or InfoMap

  `merge direction`: for InfoFlow, setting this to `symmetric` means module a would not seek to merge module b, unless module a has an edge to module b, even if module b has an edge to module a. If it is set to `symmetric`, then module a would seek to merge with module b

  `merge nonedge`: when set to true, after community detection is completed, community detection would be attempted again, this time where unconnected modules may also merge

  `log path`: file path of text log file

  `Parquet path`: file path to save final graph in Parquet format

  `RDD path`: file path to save final graph in RDD format

  `txt path`: file path to save partitioning data in local txt format

  `Full Json path`: file path to save full graph in Json format, where each node is annotated with its associated module

  `Reduced Json path`: file path to save reduced graph in Json format, where each node is a module

  `debug`: boolean value; set true to save all intermediate graphs

To suppress a logging feature, put in an empty file path. For example, if you do not want to save in RDD format, set `RDD path` to "".

### Package and run

The Makefile provides a basic packaging and running method. Use `make run` to execute InfoFlow, although it will likely not provide optimal execution.

## Theory

This section provides the discrete maths that allow the InfoMap algorithm to be adapted onto Apache Spark, and the development of the parallel version, InfoFlow.

### Fundamentals

These are the fundamental maths found in the original paper [Martin Rosvall and Carl T. Bergstrom PNAS January 29, 2008. 105 (4) 1118-1123; https://doi.org/10.1073/pnas.0706851105]:

#### Nodes

Each node is indexed, with the index denoted by greek alphabets α, β or γ.
Each node α is associated with an ergodic frequency pα.
In between nodes there may be a directed edge ωαβ from node α to node
β. The directed edge weights are normalized with respect to the outgoing
node, so that

![](https://latex.codecogs.com/svg.latex?\sum_\alpha&space;\omega_{\alpha\beta}&space;=&space;1)

The nodes are unchanged for all partitioning schemes.

#### Modules

The nodes are partitioned into modules. Each module is indexed with Latin
alphabets i, j, or k.

Each module has ergodic frequency:

![](https://latex.codecogs.com/svg.latex?p_i&space;=&space;\sum_{\alpha\in&space;i}p_\alpha)

and probability of exiting the module is:

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)\sum_{\alpha\in&space;i}\sum_{\beta\notin&space;i}p_\alpha\omega_{\alpha\beta})

with these, we try to minimize the code length:

![](https://latex.codecogs.com/svg.latex?L&space;=&space;\mathrm{plogp}\left(&space;\sum_iq_i&space;\right)&space;-2\sum_i\mathrm{plogp}\left(q_i\right)&space;-\sum_\alpha&space;\mathrm{plogp}(p_\alpha)&space;&plus;\sum_i\mathrm{plogp}\left(&space;p_i&plus;q_i&space;\right))

where

![](https://latex.codecogs.com/svg.latex?\mathrm{plogp}(x)&space;=&space;x&space;\mathrm{log}_2&space;x)

### Simplifying calculations

We develop maths to reduce the computational complexity for merging calculations.
Specifically, we find recursive relations, so that when we merge
modules j and k into i, we calculate the properties of i from those of j and
k.

#### Calculating merging quantities

We can rewrite

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)\sum_{\alpha\in&space;i}\sum_{\beta\notin&space;i}p_\alpha\omega_{\alpha\beta})

as

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i)

with

![](https://latex.codecogs.com/svg.latex?w_i&space;=&space;\sum_{\alpha\in&space;i}\sum_{\beta\notin&space;i}p_\alpha\omega_{\alpha\beta})

being the exit probability without teleportation.

We can define a similar quantity, the transition probability without teleportation from module j to module k:

![](https://latex.codecogs.com/svg.latex?w_{jk}&space;=&space;\sum_{\alpha\in&space;j}\sum_{\beta\in&space;k}p_\alpha\omega_{\alpha\beta})

Now, if we merge modules j and k into into a new module with index i,
the exit probability would be follow

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)\sum_{\alpha\in&space;i}\sum_{\beta\notin&space;i}p_\alpha\omega_{\alpha\beta})

with

![](https://latex.codecogs.com/svg.latex?n_i&space;&=&space;n_j&space;&plus;n_k)
![](https://latex.codecogs.com/svg.latex?p_i&space;&=&space;p_j&space;&plus;p_k)

and the exit probability without teleportation can be calculated via:

![](https://latex.codecogs.com/gif.latex?w_i&space;=&space;\sum_{\alpha\in&space;i}&space;\sum_{\beta\notin&space;i}&space;p_\alpha\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\substack{\alpha\in&space;j\\or~\alpha\in&space;k}}&space;~~&space;\sum_{\substack{\beta\notin&space;j\\and\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\alpha\in&space;j}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\and&space;\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\and&space;\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta})

since we are looking at the exit probability of a module, there are no self
connections within modules, so that the specification of pαwαβ given α ∈ i,
β /∈ i is redundant. Then we have

![](https://latex.codecogs.com/svg.latex?w_i&space;=&space;\sum_{\alpha\in&space;j}&space;\sum_{\beta\notin&space;k}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;\sum_{\beta\notin&space;j}&space;p_\alpha\omega_{\alpha\beta})

which conforms with intuition, that the exit probability without teleportation
of the new module is equal to the exit probability of all nodes without
counting for the connections from j to k, or from k to j.

We can further simplify the maths by expanding the non-inclusive set
specification:

![](https://latex.codecogs.com/svg.latex?w_i&space;=&space;\sum_{\alpha\in&space;j}&space;\left[&space;\sum_\beta&space;p_\alpha\omega_{\alpha\beta}&space;-\sum_{\beta\in&space;k}&space;p_\alpha\omega_{\alpha\beta}&space;\right]&space;&plus;\sum_{\alpha\in&space;k}&space;\left[&space;\sum_\beta&space;p_\alpha\omega_{\alpha\beta}&space;-\sum_{\beta\in&space;j}&space;p_\alpha\omega_{\alpha\beta}&space;\right])

Expanding gives:

![](https://latex.codecogs.com/svg.latex?w_i&space;=&space;\sum_{\alpha\in&space;j}&space;\sum_\beta&space;p_\alpha\omega_{\alpha\beta}&space;-\sum_{\alpha\in&space;j}\sum_{\beta\in&space;k}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;\sum_\beta&space;p_\alpha\omega_{\alpha\beta}&space;-\sum_{\alpha\in&space;k}\sum_{\beta\in&space;j}&space;p_\alpha\omega_{\alpha\beta})

which by definition is

![](https://latex.codecogs.com/svg.latex?w_i&space;=&space;w_j&space;-w_{jk}&space;&plus;w_k&space;-w_{kj})

This allows economical calculations.

We can do similar for wil, if we merged modules j and k into i, and l is
some other module:

![](https://latex.codecogs.com/svg.latex?w_{il}&space;=&space;\sum_{\alpha\in&space;i}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\substack{~~~\alpha\in&space;j\\or~\alpha\in&space;k}}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\alpha\in&space;j}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;w_{jl}&space;&plus;w_{kl})

and similarly for wli:

![](https://latex.codecogs.com/svg.latex?w_{li}&space;=&space;w_{lj}&space;&plus;w_{lk})

We can simplify further. The directionality of the connections are not
needed, since wij and wji always appear together in Eq. (16). Then, we can
define

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;l}&space;=&space;w_{il}&space;&plus;w_{li})

and we can verify that

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;w_{il}&space;&=&space;w_{jl}&space;&plus;w_{kl}\\&space;w_{li}&space;&=&space;w_{lj}&space;&plus;w_{lk}&space;\end{align*})

combine to give

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;l}&space;=&space;w_{il}&space;&plus;w_{li})

![](https://latex.codecogs.com/svg.latex?=&space;w_{jl}&space;&plus;w_{kl}&space;&plus;w_{lj}&space;&plus;w_{lk})

![](https://latex.codecogs.com/svg.latex?=&space;w_{jl}&space;&plus;w_{lj}&space;&plus;w_{kl}&space;&plus;w_{lk})

![](https://latex.codecogs.com/svg.latex?=&space;w_{j\leftrightharpoons&space;l}&space;&plus;w_{k\leftrightharpoons&space;l})

and this quantity is applied via

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;q_i&space;&=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i&space;\\&space;w_i&space;&=&space;w_j&space;&plus;w_k&space;-w_{j\leftrightharpoons&space;l}&space;\end{align*})

The calculations above has a key, central message: that **for the purpose of community detection, we can forget about the actual nodal properties; after each merge, we only need to keep track of a module/community**.

### Calculating code length reduction

Given an initial code length according to

![](https://latex.codecogs.com/svg.latex?L&space;=&space;\mathrm{plogp}\left(&space;\sum_iq_i&space;\right)&space;-2\sum_i\mathrm{plogp}\left(q_i\right)&space;-\sum_\alpha&space;\mathrm{plogp}(p_\alpha)&space;&plus;\sum_i\mathrm{plogp}\left(&space;p_i&plus;q_i&space;\right))

further iterations in code
length calculation can calculated via dynamic programming. Whenever we
merge two modules j and k into i, with new module frequency pi and qi
, the
change in code length is:

![](https://latex.codecogs.com/svg.latex?\Delta&space;L_i&space;=&space;\mathrm{plogp}\left[&space;q_i-q_j-q_k&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_i&space;q_i&space;\right])

![](https://latex.codecogs.com/svg.latex?-2&space;\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_j)&space;&plus;2\mathrm{plogp}(q_k))

![](https://latex.codecogs.com/svg.latex?&plus;\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_j&plus;q_j)&space;-\mathrm{plogp}(p_k&plus;q_k))

so that if we keep track of Pi qi , we can calculate ∆L quickly by plugging in pi , pj , pk, qi , qj , qk.

### InfoMap Algorithm

The algorithm consists of two stages, the initial condition and the loop:

#### Initial condition

Each node is its own module, so that we have:

![](https://latex.codecogs.com/svg.latex?n_i&space;=&space;1)

![](https://latex.codecogs.com/svg.latex?p_i&space;&=&space;p_\alpha)

![](https://latex.codecogs.com/svg.latex?w_i&space;&=&space;p_\alpha\sum_{\beta\neq\alpha}\omega_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau&space;\frac{n-n_i}{n-1}&space;p_i&space;&plus;(1-\tau)w_i)

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;j}&space;=&space;\omega_{ij}&space;&plus;\omega_{ji},&space;~~~\forall&space;\omega_{ij}~\mathrm{and}~\omega_{ji})

and ∆L is calculated for all possible merging pairs according to

![](https://latex.codecogs.com/svg.latex?\Delta&space;L_i&space;=&space;\mathrm{plogp}\left[&space;q_i-q_j-q_k&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_i&space;q_i&space;\right])

![](https://latex.codecogs.com/svg.latex?-2&space;\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_j)&space;&plus;2\mathrm{plogp}(q_k))

![](https://latex.codecogs.com/svg.latex?&plus;\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_j&plus;q_j)&space;-\mathrm{plogp}(p_k&plus;q_k))

#### Loop

Find the merge pairs that would minimize the code length; if the code length
cannot be reduced then terminate the loop. Otherwise, merge the pair to
form a module with the following quantities, so that if we merge modules j
and k into i, then: (these equations are presented in previous sections, but
now repeated for ease of reference)

![](https://latex.codecogs.com/svg.latex?n_i&space;&=&space;n_j&space;&plus;n_k)

![](https://latex.codecogs.com/svg.latex?p_i&space;&=&space;p_j&space;&plus;p_k)

![](https://latex.codecogs.com/svg.latex?w_i&space;&=&space;w_j&space;&plus;w_k&space;-w_{j\leftrightharpoons&space;k})

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i)

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;l}&space;&=&space;w_{j\leftrightharpoons&space;l}&space;&plus;w_{k\leftrightharpoons&space;l},&space;~~~\forall&space;l\neq&space;i)

and

![](https://latex.codecogs.com/svg.latex?\Delta&space;L_{i\leftrightharpoons&space;l}&space;=&space;\mathrm{plogp}\left[&space;q_{i\leftrightharpoons&space;l}-q_i-q_l&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_k&space;q_k&space;\right])

![](https://latex.codecogs.com/svg.latex?-2&space;\mathrm{plogp}(q_{i\leftrightharpoons&space;l})&space;&plus;2\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_l))

![](https://latex.codecogs.com/svg.latex?&plus;\mathrm{plogp}(p_i&plus;q_{i\leftrightharpoons&space;l})&space;-\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_l&plus;q_l))

is recalculated for all merging pairs that involve module i, i.e., for each wil.
The sum Pi qi is iterated in each loop by adding qi − qj − qk.

### Algorithm

Given the above math, the pseudocode is:

Initiation:
  - Construct a table (i.e. row and column), where each row is an undirected
    edge between modules in the graph. Each row is of format (
    (vertex1,vertex2), ( n1, n2, p1, p2, w1, w2, w12, q1, q2, ∆L ) ). The
    quantities n, p, w, q, are properties of the two modules, n is the nodal
    size of the module, p is the ergodic frequency of the module, w is the
    6
    exit probability of a module without teleportation, q is the exit probability
    of a module with teleportation, ∆L is the change in code length
    if the two modules are merged. Vertex1 and vertex2 are arranged such
    that vertex1 is always smaller than vertex2.
  - Construct a table (i.e. row and column), where each row is an undirected
    edge between modules in the graph. Each row is of format (
    (vertex1,vertex2), ( n1, n2, p1, p2, w1, w2, w12, q1, q2, ∆L ) ). The
    quantities n, p, w, q, are properties of the two modules, n is the nodal
    size of the module, p is the ergodic frequency of the module, w is the
    6
    exit probability of a module without teleportation, q is the exit probability
    of a module with teleportation, ∆L is the change in code length
    if the two modules are merged. Vertex1 and vertex2 are arranged such
    that vertex1 is always smaller than vertex2.

Loop:
  - Pick the row that has the smallest ∆L. If it is non-negative, terminate
    the algorithm.
  - Calculate the newly merged module size, ergodic frequency, exit probabilities.
  -  Calculate the new RDD of edges by deleting the edges between the
    merging modules, and then aggregate all edges associated with module
    2 to those in module 1.
  - Update the table by aggregating all rows associated with module 2 to
    those in module 1. Join the table with the RDD of edges. Since the
    RDD of edges contain w1k, we can now calculate ∆L and put it in
    the table for all rows associated with module 1.
  - Repeat from Step 1.
There are O(e) merges, e being the number of edges in the graph.

## Merging multiple modules at once

In the previous sections, we have developed the discrete mathematics and
algorithm that performs community detection with O(e) loops, based on the
key mathematical finding that we only need to remember modular properties,
not nodal ones.

The algorithm above does not take advantage of the parallel processing
capabilities of Spark. One obvious improvement possibility is to perform
multiple merges per loop. However, algorithms so far focus on merging two
modules on each iteration, which is not compatible with the idea of performing
multiple merges, unless we can make sure no module is involved with
more than one merge at once.

Here, rather than focusing on making sure that no module is involved with
more than one merge at once, we can explore the idea of merging multiple
modules at once. Thus, we can perform parallel merges in the same loop
iteration, where possibly all modules are involved in some merge

### Mathematics

Here we develop the mathematics to keep track of merging multiple modules
at once.

We consider multiple modules Mi merging into a module M. Another
way to express this equivalently is to say that a module M is partitioned
into i non-overlapping subsets:

![](https://latex.codecogs.com/svg.latex?\mathcal{M}&space;=&space;\sum_i&space;\mathcal{M}_i)

Then we can expand the nodal sum over module M into the sum over all
nodes in all submodules Mi
, the exit probability of the merged module M
becomes:

![](https://latex.codecogs.com/svg.latex?w_\mathcal{M}&space;&=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\beta\notin\mathcal{M}}&space;p_\alpha&space;w_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\left[&space;\sum_\beta&space;p_\alpha&space;w_{\alpha\beta}&space;-\sum_{\beta\in\mathcal{M}}&space;p_\alpha&space;w_{\alpha\beta}&space;\right])

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_\beta&space;p_\alpha&space;w_{\alpha\beta}&space;-\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\beta\in\mathcal{M}}&space;p_\alpha&space;w_{\alpha\beta})

![](https://latex.codecogs.com/svg.latex?=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_\beta&space;p_\alpha&space;w_{\alpha\beta}&space;-\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\mathcal{M}_j}\sum_{\beta\in\mathcal{M}_j}&space;p_\alpha&space;w_{\alpha\beta})

where we expand the second term with respect to the Mj’s to give

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;w_\mathcal{M}&space;&=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_\beta&space;p_\alpha&space;w_{\alpha\beta}&space;\\&space;&-\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\mathcal{M}_j\neq\mathcal{M}_i}\sum_{\beta\in\mathcal{M}_j}&space;p_\alpha&space;w_{\alpha\beta}&space;-\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\beta\in\mathcal{M}_i}&space;p_\alpha&space;w_{\alpha\beta}&space;\end{align*})

Combining the first and third terms,

![](https://latex.codecogs.com/svg.latex?w_\mathcal{M}&space;=&space;\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\beta\notin\mathcal{M}_i}&space;p_\alpha&space;w_{\alpha\beta}&space;-\sum_{\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\mathcal{M}_j\neq\mathcal{M}_i}\sum_{\beta\in\mathcal{M}_j}&space;p_\alpha&space;w_{\alpha\beta})

which we can recognize as

![](https://latex.codecogs.com/svg.latex?w_\mathcal{M}&space;&=&space;\sum_{\mathcal{M}_i}&space;w_{\mathcal{M}_i}&space;-\sum_{\mathcal{M}_i}&space;\sum_{\mathcal{M}_j\neq\mathcal{M}_i}&space;w_{\mathcal{M}_i\mathcal{M}_j}&space;\label{eq:multimerge-w})

![](https://latex.codecogs.com/svg.latex?w_{\mathcal{M}_i}&space;&=&space;\sum_{\alpha\in\mathcal{M}_i}&space;\sum_{\beta\notin\mathcal{M}_i}&space;p_\alpha&space;w_{\alpha\beta}&space;\label{eq:multimerge-wi})

![](https://latex.codecogs.com/svg.latex?w_{\mathcal{M}_i\mathcal{M}_j}&space;&=&space;\sum_{\alpha\in\mathcal{M}_i}\sum_{\beta\in\mathcal{M}_j}&space;p_\alpha&space;w_{\alpha\beta}&space;\label{eq:multimerge-wij})

which we can immediately see as linear generalizations of
the previous equations, and may be calculated iteratively as the previous algorithm. We can calculate
wMiMj by expanding on the partitioning:

![](https://latex.codecogs.com/gif.latex?\begin{align*}&space;w_{\mathcal{M}_i\mathcal{M}_j}&space;&=&space;\sum_{\mathcal{M}_k\in\mathcal{M}_i}&space;\sum_{\alpha\in\mathcal{M}_k}&space;\sum_{\mathcal{M}_{k'}\in\mathcal{M}_j}&space;\sum_{\beta\in\mathcal{M}_j}&space;p_\alpha&space;w_{\alpha\beta}\\&space;&=&space;\sum_{\mathcal{M}_k\in\mathcal{M}_i}&space;\sum_{\mathcal{M}_{k'}\in\mathcal{M}_j}&space;w_{\mathcal{M}_k\mathcal{M}_{k'}}&space;\end{align*})

so that when we merge a number of modules together, we can calculate its
connections with other modules by aggregating the existing modular connections.
This is directly analogous to

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;w_{il}&space;&=&space;w_{jl}&space;&plus;w_{kl}\\&space;w_{li}&space;&=&space;w_{lj}&space;&plus;w_{lk}&space;\end{align*})

Thus, the mathematical properties of merging multiple modules into one
are identical to that of merging two modules. This is key to developing
multi-merge algorithm.

### Algorithm

Initiation is similar to the infomap algorithm, so that we have an RDD
of modular properties in the format (index,n,p,w,q), and edge properties
((vertex1,vertex2),summed connection weight), ((vertex1,vertex2),deltaL).

Loop:

  -  For each module, seek to merge with another module that would offer
    the greatest reduction in code length. If no such merge exists, the
    module does not seek to merge. Then, we have O(e) edges.
  - For each of these edges, label it to the module index to be merged to, so
    that each connected component of the graph has the same label. This
    has O(k) complexity, k being the size of the connected component. The
    precise algorithm is described below.
  - Recalculate modular and edge property values via aggregations:

    ![](https://latex.codecogs.com/svg.latex?n_i&space;&=&space;\sum_{k\rightarrow&space;i}&space;n_k)

    ![](https://latex.codecogs.com/svg.latex?p_i&space;&=&space;\sum_{k\rightarrow&space;i}&space;p_k)

    ![](https://latex.codecogs.com/svg.latex?w_i&space;&=&space;\sum_{k\rightarrow&space;i}&space;w_k&space;-\sum_{k\leftrightharpoons&space;k'\rightarrow&space;i}&space;w_{k\leftrightharpoons&space;k'})

    ![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i)

    ![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;j}&space;=&space;\sum_{k\rightarrow&space;i}&space;\sum_{k'\rightarrow&space;j}&space;w_{k\leftrightharpoons&space;k'})

    ![](https://latex.codecogs.com/svg.latex?L&space;&=&space;\mathrm{plogp}\left(&space;\sum_iq_i&space;\right)&space;-2\sum_i\mathrm{plogp}\left(q_i\right)&space;&&space;-\sum_\alpha&space;\mathrm{plogp}(p_\alpha)&space;&plus;\sum_i\mathrm{plogp}\left(&space;p_i&plus;q_i&space;\right))

    ![](https://latex.codecogs.com/svg.latex?\Delta&space;L_{i\leftrightharpoons&space;j}&space;&=&space;\mathrm{plogp}\left[&space;q_{i\leftrightharpoons&space;j}-q_i-q_j&plus;\sum_k&space;q_k&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_k&space;q_k&space;\right]&space;\\&space;&-2&space;\mathrm{plogp}(q_{i\leftrightharpoons&space;j})&space;&plus;2\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_j)&space;\\&space;&&plus;\mathrm{plogp}(p_{i\leftrightharpoons&space;j}&plus;q_{i\leftrightharpoons&space;j})&space;-\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_j&plus;q_j))

### Labeling connected components

This algorithm labels a graph so that all connected components are labeled by the same module index, the module index being the one that occurs the most frequently within the linked edges.

Initiation:
  - Given the edges, count the occurrences of the vertices.
  - Label each edge to one of the two vertices with the higher occurrence.

Loop:
  - Count the label occurrences for each label.
  - For each vertex, find the label with the maximum occurrence associated with it.
  - For each edge, label it according to the vertex with a higher label count.
  - If for each edge, the new labeling is identical to the old, terminate. Else, repeat.

### Performance Improvement

Infomap performs greedy merges of two modules until no more merges can
be performed. If there are n nodes in the graph and m modules in the end,
then there are n − m merges, since we assume the graph is sparse and the
edges are proportional to the number of nodes. Since each loop merges two
modules, there are n−m−1 loops. Thus, infomap has linear time complexity
to the number of nodes/edges.

In the multiple merging scheme, within each loop each module merges
with one other module. Let’s assume in each loop, k modules merge into one
on average. Then, let there be l loops, and as before, n nodes are merged
into m modules. Since each loop reduces the amount of modules by k times,
we have

![](https://latex.codecogs.com/svg.latex?n&space;k^{-l}&space;&=&space;m)

![](https://latex.codecogs.com/svg.latex?k^l&space;=&space;\frac{n}{m})

![](https://latex.codecogs.com/svg.latex?l&space;&=&space;\mathrm{log}_k&space;n&space;-\mathrm{log}_k&space;m)

Within each merge, there are O(k) operations to aggregate the indices appropriately
for the merges.

Thus, the overall time complexity is O(k log n). For example, if k = 2,
i.e., every pair of modules are merged every step, then we have O(2 log n)
complexity, i.e., logarithmic complexity in the number of nodes/edges.

In the worst cases, we have linear time complexity, if l = 1, k = n/m,
so that the overall complexity is O(k) = O(n/m). Another possibility is if
InfoFlow degenerates into InfoMap, so that we merge a pair of modules every
step, and of course recover linear complexity

The central reason behind the logarithmic time complexity is the constant
average merging of k modules into one. The constancy of this factor likely
depends on the network structure. Given a sparse network, the number of
edges is similar to the number of nodes. If we make the assumption that
k is proportional to the number of edges, then after a loop, the number of
modules is reduced by a factor of k, and so is the number of edges. Thus, the
network sparsity remains unchanged, and k is unchanged also. Of course,
ultimately, actual performance benchmarks are required.

The logarithmic complexity and the k constant suggests that perhaps enforced
binary merging, i.e., either pair-wise merge or no merge at all for some
modules, might achieve best runtime complexity. A possible catch-22 might
be that, to enforce pair-wise merges, O(k) explorations would be needed, so
that the runtime complexity remains the same, and actual performance is
even penalized. Further mathematical ideas, simulations and benchmarks
would be required for further explorations.

## Author

* **Felix Fung** [Github](https://github.com/felixfung)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

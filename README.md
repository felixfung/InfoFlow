# InfoFlow
An Apache Spark implementation of the InfoMap community detection algorithm

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

![](https://latex.codecogs.com/svg.latex?w_i&space;&=&space;\sum_{\alpha\in&space;i}&space;\sum_{\beta\notin&space;i}&space;p_\alpha\omega_{\alpha\beta}\\&space;&=&space;\sum_{\substack{~~~\alpha\in&space;j\\or~\alpha\in&space;k}}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\and\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta}\\&space;&=&space;\sum_{\alpha\in&space;j}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\and\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\and\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta})

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

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;w_{il}&space;&=&space;\sum_{\alpha\in&space;i}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta}&space;\\&space;&=&space;\sum_{\substack{~~~\alpha\in&space;j\\\mathrm{or}~\alpha\in&space;k}}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta}&space;\\&space;&=&space;\sum_{\alpha\in&space;j}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;\sum_{\beta\in&space;l}&space;p_\alpha\omega_{\alpha\beta}&space;\\&space;&=&space;w_{jl}&space;&plus;w_{kl}&space;\end{align*})

and similarly for wli:

![](https://latex.codecogs.com/svg.latex?w_{li}&space;=&space;w_{lj}&space;&plus;w_{lk})

We can simplify further. The directionality of the connections are not
needed, since wij and wji always appear together in Eq. (16). Then, we can
define

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;l}&space;=&space;w_{il}&space;&plus;w_{li})

and we can verify that

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;w_{il}&space;&=&space;w_{jl}&space;&plus;w_{kl}\\&space;w_{li}&space;&=&space;w_{lj}&space;&plus;w_{lk}&space;\end{align*})

combine to give

![](https://latex.codecogs.com/svg.latex?w_{i\leftrightharpoons&space;l}&space;&=&space;w_{il}&space;&plus;w_{li}&space;\\&space;&=&space;w_{jl}&space;&plus;w_{kl}&space;&plus;w_{lj}&space;&plus;w_{lk}&space;\\&space;&=&space;w_{jl}&space;&plus;w_{lj}&space;&plus;w_{kl}&space;&plus;w_{lk}&space;\\&space;&=&space;w_{j\leftrightharpoons&space;l}&space;&plus;w_{k\leftrightharpoons&space;l})

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

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;\Delta&space;L_i&space;&=&space;\mathrm{plogp}\left[&space;q_i-q_j-q_k&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_i&space;q_i&space;\right]&space;\nonumber\\&space;&-2&space;\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_j)&space;&plus;2\mathrm{plogp}(q_k)&space;\nonumber\\&space;&&plus;\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_j&plus;q_j)&space;-\mathrm{plogp}(p_k&plus;q_k)&space;\label{eq:DeltaL}&space;\end{align*})

so that if we keep track of Pi qi , we can calculate ∆L quickly by plugging in pi , pj , pk, qi , qj , qk.

## InfoMap Algorithm

The algorithm consists of two stages, the initial condition and the loop:

### Initial condition

Each node is its own module, so that we have:

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;n_i&space;&=&space;1\\&space;p_i&space;&=&space;p_\alpha&space;\\&space;w_i&space;&=&space;p_\alpha\sum_{\beta\neq\alpha}\omega_{\alpha\beta}&space;\\&space;q_i&space;&=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i&space;\\&space;w_{i\leftrightharpoons&space;j}&space;&=&space;\omega_{ij}&space;&plus;\omega_{ji},&space;~~~\forall&space;\omega_{ij}~\mathrm{and}~\omega_{ji}&space;\end{align*})

and ∆L is calculated for all possible merging pairs according to

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;\Delta&space;L_i&space;&=&space;\mathrm{plogp}\left[&space;q_i-q_j-q_k&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_i&space;q_i&space;\right]&space;\nonumber\\&space;&-2&space;\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_j)&space;&plus;2\mathrm{plogp}(q_k)&space;\nonumber\\&space;&&plus;\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_j&plus;q_j)&space;-\mathrm{plogp}(p_k&plus;q_k)&space;\label{eq:DeltaL}&space;\end{align*})

### Loop

Find the merge pairs that would minimize the code length; if the code length
cannot be reduced then terminate the loop. Otherwise, merge the pair to
form a module with the following quantities, so that if we merge modules j
and k into i, then: (these equations are presented in previous sections, but
now repeated for ease of reference)

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;n_i&space;&=&space;n_j&space;&plus;n_k&space;\\&space;p_i&space;&=&space;p_j&space;&plus;p_k&space;\\&space;w_i&space;&=&space;w_j&space;&plus;w_k&space;-w_{j\leftrightharpoons&space;k}&space;\\&space;q_i&space;&=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)w_i&space;\\&space;w_{i\leftrightharpoons&space;l}&space;&=&space;w_{j\leftrightharpoons&space;l}&space;&plus;w_{k\leftrightharpoons&space;l},&space;~~~\forall&space;l\neq&space;i&space;\end{align*})

and

![](https://latex.codecogs.com/svg.latex?\begin{align*}&space;\Delta&space;L_{i\leftrightharpoons&space;l}&space;&=&space;\mathrm{plogp}\left[&space;q_{i\leftrightharpoons&space;l}-q_i-q_l&plus;\sum_i&space;q_i&space;\right]&space;-\mathrm{plogp}&space;\left[&space;\sum_k&space;q_k&space;\right]&space;\nonumber\\&space;&-2&space;\mathrm{plogp}(q_{i\leftrightharpoons&space;l})&space;&plus;2\mathrm{plogp}(q_i)&space;&plus;2\mathrm{plogp}(q_l)&space;\nonumber\\&space;&&plus;\mathrm{plogp}(p_i&plus;q_{i\leftrightharpoons&space;l})&space;-\mathrm{plogp}(p_i&plus;q_i)&space;-\mathrm{plogp}(p_l&plus;q_l)&space;\end{align*})

is recalculated for all merging pairs that involve module i, i.e., for each wil.
The sum Pi qi is iterated in each loop by adding qi − qj − qk.

## Algorithm

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


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags).

## Author

* **Felix Fung** - *Everything* - [Felix Fung](https://github.com/felixfung)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc

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

We can define a similar quantity, the transition probability without teleportation from module \(j\) to module \(k\):

![](https://latex.codecogs.com/svg.latex?w_{jk}&space;=&space;\sum_{\alpha\in&space;j}\sum_{\beta\in&space;k}p_\alpha\omega_{\alpha\beta})

Now, if we merge modules j and k into into a new module with index i,
the exit probability would be follow

![](https://latex.codecogs.com/svg.latex?q_i&space;=&space;\tau\frac{n-n_i}{n-1}p_i&space;&plus;(1-\tau)\sum_{\alpha\in&space;i}\sum_{\beta\notin&space;i}p_\alpha\omega_{\alpha\beta})

with

![](https://latex.codecogs.com/svg.latex?n_i&space;&=&space;n_j&space;&plus;n_k)
![](https://latex.codecogs.com/svg.latex?p_i&space;&=&space;p_j&space;&plus;p_k)

and the exit probability without teleportation can be calculated via:

![](https://latex.codecogs.com/svg.latex?w_i&space;&=&space;\sum_{\alpha\in&space;i}&space;\sum_{\beta\notin&space;i}&space;p_\alpha\omega_{\alpha\beta}\\&space;&=&space;\sum_{\substack{~~~\alpha\in&space;j\\\mathrm{or}~\alpha\in&space;k}}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\\mathrm{and}\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta}\\&space;&=&space;\sum_{\alpha\in&space;j}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\\mathrm{and}\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta}&space;&plus;\sum_{\alpha\in&space;k}&space;~~&space;\sum_{\substack{~~~~\beta\notin&space;j\\\mathrm{and}\beta\notin&space;k}}&space;p_\alpha\omega_{\alpha\beta})

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

# Introduction

Done by [@grahamwren](https://github.com/grahamwren) and
[@jagen31](https://github.com/jagen31).

EAU2 is a system for distributing data over a network and running computations
in parallel over that data.  EAU2 uses a distributed key value store to manage
multiple dataframes which may be larger than memory and commmunication over
TCP/IP to orchestrate parallel computations over those dataframes.  Dataframes
in EAU2 are read-only, making it easy to run applications in parallel over the
same set of data.

# Architecture

EAU2 has three layers.  The **Application** layer is where programs are written
that make use of EAU2.  Programs can construct multiple dataframes and make
multiple queries. The **KV** layer is where data is distributed logically, and
provides the interface through which the system is queried.  The **Network**
layer is where requests are made between nodes.

# Implementation

The Application class represents an operation that makes use of a KV to
manipulate dataframes.  The KV class utilizes the network to manage dataframes.
Each KV owns a list of dataframes, and knows about the dataframes owned by
other KVs, through `ownership_mapping`.  

When `put` is called with a new key, the KV adds itself to the ownership map
and broadcasts the new map to the other nodes in the network.  The buffer
passed into `put` is converted into rows chunks at a time.  The chunks are
distributed over nodes in the network in a round-robin fashion.  That is, nodes
have a fixed order and chunks are distributed by traversing them as a ring,
starting at the node where the put request was made.  

A computation is run on a dataframe by calling `map` on a `Rower`, either on a
`DataFrame` object, or on a KV by providing the dataframe's key.  The KV runs
the computation on the chunks contained in the current node, while using the
network to ask for the results of the computation run on chunks stored in other
nodes.  The computation runs in parallel, so rows are likely processed out of
order.  The results, however, are joined in order.

A fixed set of `Rower`s have to be defined at compile time, along with a
serialization/deserialization method for them.  This is because the Rowers must
be sent over the network, and arbitrary C++ code cannot be serialized in our
system.

# Use cases

# Open questions

Currently, computations must be present on all nodes and precompiled.  It would
be more flexible to provide either an interpreted language or a higher level
language which compiles into some precompiled computations.  We have considered
implementing an AST which looks similar to SQL, or using a scripting language,
possibly Racket, Lua, or NewLISP.

# Status
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

Each node in the network holds parts of dataframes and knows where to look
to find the parts it does not own (see Implementation for more).

# Implementation

![EAU2 Entity Relationship Diagram](https://github.com/grahamwren/cs4500-project/raw/master/report/diagram.png)

The Application class represents an operation that makes use of a KV to
manipulate dataframes.  The KV class utilizes the network to manage dataframes.
Each KV owns a list of dataframes, and knows about the dataframes owned by
other KVs, through `ownership_mapping`.  Each KV has potentially several
chunks of dataframes, stored contiguously in memory via `PartialDataFrame`.
`DataFrameChunk` is where the data is actually stored.

When `put` is called with a new key, the KV adds itself to the ownership map
and broadcasts the new map to the other nodes in the network.  The buffer
passed into `put` is converted into rows chunks at a time.  The chunks are
distributed over nodes in the network in a round-robin fashion.  That is, nodes
have a fixed order and chunks are distributed by traversing them as a ring,
starting at the node where the put request was made.

The `DataFrame` object itself is a virtual dataframe, that is, it delegates
to the KV in order to perform its operations.

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

A `Node` is the low-level interface to the networking layer.  The node will
handle network commands automatically, and takes a callback for handling
application commands.  The callback, set through `set_data_handler`, will
close over a KV, and deserialize and interpret the message data using
the KV.

# Use cases

```
input_file.sor:

<1><2><3>
<4><5><6>
```

```cpp
Node node(... ip addr ...);
KV kv(node);
node.set_data_handler(... data handler ...);

BytesReader b = Sorer::parse("input_file.sor");
kv.put("example", b);
SumRower r;
kv.map(*r);

> r.result
21
```

# Open questions

Currently, computations must be present on all nodes and precompiled.  It would
be more flexible to provide either an interpreted language or a higher level
language which compiles into some precompiled computations.  We have considered
implementing an AST which looks similar to SQL, or using a scripting language,
possibly Racket, Lua, or NewLISP.

It is unclear how we'd handle adding or removing nodes from the network.  The
data would have to be redistributed at some point, but this would require
interrupting computations.

Similarly, our current spec has no redundancy, so if a node were to die, the
data would be lost.

# Status

The networking layer is mostly done, aside from the callback with the
interpreter.  The existing codebase has been migrated to C++.  The
implementation for `DataFrame` has to be moved to `DataFrameChunk`, and the
`DataFrame` itself reimplemented to perform its operations using a `KV`.
Speaking of, the `KV` itself needs to be implemented.

# Introduction

Done by [@grahamwren](https://github.com/grahamwren) and
[@jagen31](https://github.com/jagen31).

EAU2 is a system for distributing data over a network and running computations
in parallel over that data. EAU2 uses a distributed key value store to manage
multiple dataframes which may be larger than memory and communication over
TCP/IP to orchestrate parallel computations over those dataframes.

# Architecture

EAU2 has three layers. The **Application** layer is where programs are written
that make use of EAU2. Programs can construct multiple dataframes and make
multiple queries. The **KV** layer is where data is distributed logically, and
provides the interface through which the system is queried. The **Network**
layer is where requests are made between nodes.

Each node in the network holds parts of dataframes and knows where to look
to find the parts it does not own (see Implementation for more).

# Implementation

![EAU2 Entity Relationship Diagram](https://github.com/grahamwren/cs4500-assignment_1-part2/raw/master/diagram.png)

The Cluster class has the ability to make use of the KV store to manipulate
dataframes. The Cluster accesses the KV store through the KV class. The KV
class provides an interface to run Commands in the KV store. Commands are
serializable objects that do something with the store. Standard commands are
GET, PUT, DELETE, NEW, GET_DF_INFO, START_MAP, and FETCH_MAP_RESULT. The KV
store is distributed over the network, and a local KV Store class (different
from the KV class) is present on each node. The KV class communicates with the
local KVs by serializing and deserializing commands.

The "Application" (the Cluster + user code) is not running as a node
participating in the network, but rather, communicating with the network. This
means an application has to send a message to the machine it's running on in
order to run commands locally, rather than having a separate, faster
implementation for local operations.

The local KV store, represented by the KVStore object, contains chunks of data
frames grouped in the PartialDataFrame class. The chunks are organized by the
Cluster class. When a new piece of data is added, a random owner is chosen from
the cluster. The data is then distributed round-robin over the nodes, in fixed
size chunks. Calculating the node which a specific chunk is at is a matter
of finding the owner, and taking the chunk index modulo number of nodes.  The
local KVStore also contains another mapping of an integer id to map results.
These are the local results of running START_MAP commands over the cluster,
and are accessed using the FETCH_MAP_RESULT command.

Keys in the KV Store are strings, though one string can represent data spread
over several nodes. A string key combined with an index can be used to get
specific chunks.

Commands are serializable objects with a run method.  The run method is passed
a respond_fn_t, which is a callback that may only be called once, and allows
the command to make a network response before it finishes performing its
computation.  This is used in the START_MAP command to allow for parallel
mapping, by returning an id to request the results of the computation later.

When `put` is called with a new key, the KV adds the new mapping to its
ownership map and broadcasts a corresponding message to the other nodes in the
network. The data passed into `put` is converted into rows one chunk at a
time. The chunks are distributed over nodes in the network in a round-robin
fashion. That is, nodes have a fixed order and chunks are distributed by
traversing them as a ring, starting at the node where the put request was made.

The `DataFrame` object itself is a virtual dataframe, that is, it delegates
to the KV in order to perform its operations.

A computation is run on a dataframe by issuing `START_MAP` commands with a
`Rower`.  Rowers can be serialized, and in a map, one START_MAP is sent over
the network containing one serialized Rower, to each node. When a node receives
a Rower, it responds right away with an integer id that serves as a handle to
fetch the results later.  It then runs the computation over its
PartialDataFrame, which maps over all the chunks it owns, in parallel. Then,
the Rower is reserialized and stored in the KVStore as a map result under the
aforementioned id.  These results can be fetched with a FETCH_MAP_RESULT
command.  

The KVStore's map method takes care of issuing the `START_MAP` commands to each
node. It then waits on each of them by issuing a `FETCH_MAP_RESULT` command.
This means the map happens in parallel, but the results "finish" in order.  The
KVStore joins them using `join` from the deserialized rowers. The joins are
also done in order.

A `Node` is the low-level interface to the networking layer. The node will
handle network commands automatically, and takes a callback for handling
application commands. The callback, set through `set_data_handler` by the KV,
will be used by the KV to handle Application level requests made of this Node
by other Nodes in the cluster.  The callback in KV deserializes the data as a
Command and calls the Command's run method.

# Use cases

## Sum Numbers in DataFrame

```
input_file.sor:

<1><2><3>
<4><5><6>
```

```cpp
Key key("example");
Cluster cluster(IpV4Addr("<ip in cluster>"));

cluster.load_from_file(key, "input_file.sor");

SumRower r;
cluster.map("example", r);

> r.result
21
```

## WordCount

First chapter of Moby Dick or The Whale. Obtained from [archive.org](https://archive.org/stream/mobydickorwhale01melvuoft/mobydickorwhale01melvuoft_djvu.txt).

<details><summary>loomings.sor</summary>

```sor
<"call"><"me"><"ishmael"><"some"><"years"><"ago"><"never"><"mind"><"how"><"long"><"precisely"><"having"><"little"><"or">
<"no"><"money"><"in"><"my"><"purse"><"and"><"nothing"><"particular"><"to"><"interest"><"me"><"on"><"shore"><"i"><"thought"><"i">
<"would"><"sail"><"about"><"a"><"little"><"and"><"see"><"the"><"watery"><"part"><"of"><"the"><"world"><"it"><"is"><"a"><"way"><"i">
<"have"><"of"><"driving"><"off"><"the"><"spleen"><"and"><"regulating"><"the"><"circulation"><"whenever"><"i"><"find">
<"myself"><"growing"><"grim"><"about"><"the"><"mouth"><"whenever"><"it"><"is"><"a"><"damp"><"drizzly"><"november"><"in">
<"my"><"soul"><"whenever"><"i"><"find"><"myself"><"involuntarily"><"pausing"><"before"><"coffin"><"warehouses">
<"and"><"bringing"><"up"><"the"><"rear"><"of"><"every"><"funeral"><"i"><"meet"><"and"><"especially"><"whenever"><"my">
<"hypos"><"get"><"such"><"an"><"upper"><"hand"><"of"><"me"><"that"><"it"><"requires"><"a"><"strong"><"moral"><"principle"><"to">
<"prevent"><"me"><"from"><"deliberately"><"stepping"><"into"><"the"><"street"><"and"><"methodically"><"knocking">
<"people's"><"hats"><"off"><"then"><"i"><"account"><"it"><"high"><"time"><"to"><"get"><"to"><"sea"><"as"><"soon"><"as"><"i"><"can">
<"this"><"is"><"my"><"substitute"><"for"><"pistol"><"and"><"ball"><"with"><"a"><"philosophical"><"flourish"><"cato">
<"throws"><"himself"><"upon"><"his"><"sword"><"i"><"quietly"><"take"><"to"><"the"><"ship"><"there"><"is"><"nothing">
<"surprising"><"in"><"this"><"if"><"they"><"but"><"knew"><"it"><"almost"><"all"><"men"><"in"><"their"><"degree"><"some"><"time">
<"or"><"other"><"cherish"><"very"><"nearly"><"the"><"same"><"feelings"><"toward"><"the"><"ocean"><"with"><"me"><"there">
<"now"><"is"><"your"><"insular"><"city"><"of"><"the"><"manhattoes"><"belted"><"round"><"by"><"wharves"><"as"><"indian">
<"isles"><"by"><"coral"><"reefs"><"commerce"><"surrounds"><"it"><"with"><"her"><"surf"><""><"right"><"and"><"left"><"the">
<"streets"><"take"><"you"><"waterward"><"its"><"extreme"><"down"><"-town"><"is"><"the"><"battery"><"where"><"that">
<"noble"><"mole"><"is"><"washed"><"by"><"waves"><"and"><"cooled"><"by"><"breezes"><"which"><"a"><"few"><"hours"><"previous">
<"were"><"out"><"of"><"sight"><"of"><"land"><"look"><"at"><"the"><"crowds"><"of"><"water"><"-gazers"><"there">
<"circumambulate"><"the"><"city"><"of"><"a"><"dreamy"><"sabbath"><"afternoon"><"go"><"from"><"corlears"><"hook"><"to">
<"coenties"><"slip"><"and"><"from"><"thence"><"by"><"whitehall"><"northward"><"what"><"do"><"you"><"see"><"posted">
<"like"><"silent"><"sentinels"><"all"><"around"><"the"><"town"><"stand"><"thousands"><"upon"><"thousands"><"of">
<"mortal"><"men"><"fixed"><"in"><"ocean"><"reveries"><"some"><"leaning"><"against"><"the"><"spilessome"><"seated">
<"upon"><"the"><"pier-heads"><"some"><"looking"><"over"><"vhe"><"bulwarks"><"of"><"ships"><"from"><"china"><"some">
<"high"><"aloft"><"in"><"the"><"rigging"><"as"><"if"><"striving"><"to"><"get"><"a"><"still"><"better"><"seaward"><"peep"><"but">
<"these"><"are"><"all"><"landsmen"><"of"><"week"><"days"><"pent"><"up"><"in"><"lath"><"and"><"plaster"><"tied"><"to">
<"counters"><"nailed"><"to"><"benches"><"clinched"><"to"><"desks"><"how"><"then"><"is"><"this"><"are"><"the"><"green">
<"fields"><"gone"><"what"><"do"><"they"><"herebut"><"look"><"here"><"come"><"more"><"crowds"><"pacing"><"straight"><"for">
<"the"><"water"><"and"><"seemingly"><"bound"><"for"><"a"><"dive"><"strangenothing"><"will"><"content"><"them"><"but">
<"the"><"extremest"><"limit"><"of"><"the"><"land"><"loitering"><"under"><"the"><"shady"><"lee"><"of"><"yonder">
<"warehouses"><"will"><"not"><"suffice"><"no"><"they"><"must"><"get"><"just"><"as"><"nigh"><"the"><"water"><"as"><"they">
<"possibly"><"can"><"without"><"falling"><"in"><"and"><"there"><"they"><"stand"><"miles"><"of"><"them"><"leagues">
<"inlanders"><"all"><"they"><"come"><"from"><"lanes"><"and"><"alleys"><"streets"><"and"><"avenues"><"north"><"east">
<"south"><"and"><"west"><"yet"><"here"><"they"><"all"><"unite"><"tell"><"me"><"does"><"the"><"magnetic"><"virtue"><"of"><"the">
<"needles"><"of"><"the"><"compasses"><"of"><"all"><"those"><"ships"><"attract"><"them"><"thitheronce"><"more"><"say">
<"you"><"are"><"in"><"the"><"country"><"in"><"some"><"high"><"land"><"of"><"lakes"><""><"take"><"almost"><"any"><"path"><"you">
<"please"><"and"><"ten"><"to"><"one"><"it"><"carries"><"you"><"down"><"in"><"a"><"dale"><"and"><"leaves"><"you"><"there"><"by"><"a">
<"pool"><"in"><"the"><"stream"><"there"><"is"><"magic"><"in"><"it"><"let"><"the"><"most"><"absent-minded"><"of"><"men"><"be">
<"plunged"><"in"><"his"><"deepest"><"reveries"><"stand"><"that"><"man"><"on"><"his"><"legs"><"set"><"his"><"feet"><"a-going">
<"and"><"he"><"will"><"infallibly"><"lead"><"you"><"to"><"water"><"if"><"water"><"there"><"be"><"in"><"all"><"that"><"region">
<"should"><"you"><"ever"><"be"><"athirst"><"in"><"the"><"great"><"american"><"desert"><"try"><"this"><"experiment"><"if">
<"your"><"caravan"><"happen"><"to"><"be"><"supplied"><"with"><"a"><"metaphysical"><"professor"><"yes"><"as">
<"everyone"><"knows"><"meditation"><"andli"><"water"><"are"><"wedded"><"forever"><"but"><"here"><"is"><"an"><"artist">
<"he"><"desires"><"to"><"paint"><"you"><"the"><"dreamiest"><"shadiest"><"quietest"><"most"><"enchanting"><"bit"><"of">
<"romantic"><"landscape"><"in"><"all"><"the"><"valley"><"of"><"the"><"saco"><"what"><"is"><"the"><"chief"><"element"><"he">
<"employs"><"there"><"stand"><"his"><"trees"><"each"><"with"><"a"><"hollow"><"trunk"><"as"><"if"><"a"><"hermit"><"and"><"a">
<"crucifix"><"were"><"within"><"and"><"here"><"sleeps"><"his"><"meadow"><"and"><"there"><"sleep"><"his"><"cattle"><"and">
<"up"><"from"><"yonder"><"cottage"><"goes"><"a"><"sleepy"><"smoke"><"deep"><"into"><"distant"><"woodlands"><"winds"><"a">
<"mazy"><"way"><"reaching"><"to"><"overlapping"><"spurs"><"of"><"mountains"><"bathed"><"in"><"their"><"hillside">
<"blue"><"but"><"though"><"the"><"picture"><"lies"><"thus"><"tranced"><"and"><"though"><"this"><"pine-tree"><"shakes">
<"down"><"its"><"sighs"><"like"><"leaves"><"upon"><"this"><"shepherd's"><"head"><"yet"><"all"><"were"><"vain"><"unless">
<"the"><"shepherd's"><"eye"><"were"><"fixed"><"upon"><"the"><"magic"><"stream"><"before"><"him"><"go"><"visit"><"the">
<"prairies"><"in"><"june"><"when"><"for"><"scores"><"on"><"scores"><"of"><"miles"><"you"><"wade"><"knee"><"-deep"><"among">
<"tiger-lilies"><"what"><"is"><"the"><"one"><"charm"><"wanting"><"?water"><"there"><"is"><"not"><"a"><"drop"><"of"><"water">
<"there"><"were"><"niagara"><"but"><"a"><"cataract"><"of"><"sand"><"would"><"you"><"travel"><"your"><"thousand"><"miles">
<"to"><"see"><"it"><"why"><"did"><"the"><"poor"><"poet"><"of"><"tennessee"><"upon"><"suddenly"><"receiving"><"two">
<"handfuls"><"of"><"silver"><"deliberate"><"whether"><"to"><"buy"><"him"><"a"><"coat"><"which"><"he"><"sadly"><"needed">
<"or"><"invest"><"his"><"money"><"in"><"a"><"pedestrian"><"trip"><"to"><"rockaway"><"beachwhy"><"is"><"almost"><"every">
<"robust"><"healthy"><"boy"><"with"><"a"><"robust"><"healthy"><"soul"><"in"><"him"><"at"><"some"><"time"><"or"><"other">
<"crazy"><"to"><"go"><"to"><"sea"><"why"><"upon"><"your"><"first"><"voyage"><"as"><"a"><"passenger"><"did"><"you"><"yourself">
<"feel"><"such"><"a"><"mystical"><"vibration"><"when"><"firsttold"><"that"><"you"><"and"><"your"><"ship"><"were"><"now">
<"out"><"of"><"sight"><"ofland"><"why"><"did"><"the"><"old"><"persians"><"hold"><"the"><"sea"><"holywhy"><"did"><"the">
<"greeks"><"give"><"it"><"a"><"separate"><"deity"><"and"><"own"><"brother"><"of"><"jove"><"surely"><"all"><"this"><"is"><"not">
<"without"><"meaning"><""><"and"><"still"><"deeper"><"the"><"meaning"><"of"><"that"><"story"><"of"><"narcissus"><"who">
<"because"><"he"><"could"><"not"><"grasp"><"the"><"tormenting"><"mild"><"image"><"he"><"saw"><"in"><"the"><"fountain">
<"plunged"><"into"><"it"><"and"><"was"><"drowned"><"but"><"that"><"same"><"image"><"we"><"ourselves"><"see"><"in"><"all">
<"rivers"><"and"><"oceans"><"it"><"is"><"the"><"image"><"of"><"the"><"ungraspable"><"phantom"><"of"><"life"><"and"><"this">
<"is"><"the"><"key"><"to"><"it"><"all"><"now"><"when"><"i"><"say"><"that"><"i"><"am"><"in"><"the"><"habit"><"of"><"going"><"to"><"sea">
<"whenever"><"i"><"begin"><"to"><"grow"><"hazy"><"about"><"the"><"eyes"><"and"><"begin"><"to"><"be"><"over"><"conscious"><"of">
<"my"><"lungs"><"i"><"do"><"not"><"mean"><"to"><"have"><"it"><"inferred"><"that"><"i"><"ever"><"go"><"to"><"sea"><"as"><"a"><"passenger">
<"for"><"to"><"go"><"as"><"a"><"passenger"><"you"><"must"><"needs"><"have"><"a"><"purse"><"and"><"a"><"purse"><"is"><"but"><"a"><"rag">
<"unless"><"you"><"have"><"something"><"in"><"it"><"besides"><"passengers"><"get"><"sea-sick"><"grow">
<"quarrelsome"><"don't"><"sleep"><"of"><"nights"><"do"><"not"><"enjoy"><"themselves"><"much"><"as"><"a"><"general">
<"thing"><"no"><"i"><"never"><"go"><"as"><"a"><"passenger"><"nor"><"though"><"i"><"am"><"something"><"of"><"a"><"salt"><"do"><"i">
<"ever"><"go"><"to"><"sea"><"as"><"a"><"commodore"><"or"><"a"><"captain"><"or"><"a"><"cook"><"i"><"abandon"><"the"><"glory"><"and">
<"distinction"><"of"><"such"><"offices"><"to"><"those"><"who"><"like"><"them"><"for"><"my"><"part"><"i"><"abominate"><"all">
<"honourable"><"respectable"><"toils"><"trials"><"and"><"tribulations"><"of"><"every"><"kind"><"whatsoever">
<"it"><"is"><"quite"><"as"><"much"><"as"><"i"><"can"><"do"><"to"><"take"><"care"><"of"><"myself"><"without"><"taking"><"care"><"of">
<"ships"><"barques"><"brigs"><"schooners"><"and"><"what"><"not"><"and"><"as"><"for"><"going"><"as"><"cook"><"though"><"i">
<"confess"><"there"><"is"><"considerable"><"glory"><"in"><"that"><"a"><"cook"><"being"><"a"><"sort"><"of"><"officer"><"on">
<"shipboard"><"yet"><"somehow"><"i"><"never"><"fancied"><"broiling"><"fowls"><"though"><"once"><"broiled">
<"judiciously"><"buttered"><"and"><"judgmatically"><"salted"><"and"><"peppered"><"there"><"is"><"no"><"one"><"who">
<"will"><"speak"><"more"><"respectfully"><"not"><"to"><"say"><"reverentially"><"of"><"a"><"broiled"><"fowl"><"than"><"i">
<"will"><"it"><"is"><"out"><"of"><"the"><"idolatrous"><"do"><"tings"><"of"><"the"><"old"><"egyptians"><"upon"><"broiled">
<"ibis"><"and"><"roasted"><"river"><"horse"><"that"><"you"><"see"><"the"><"mummies"><"of"><"those"><"creatures"><"in">
<"their"><"huge"><"bake-houses"><"the"><"pyramids"><"no"><"when"><"i"><"go"><"to"><"sea"><"i"><"go"><"as"><"a"><"simple"><"sailor">
<"right"><"before"><"the"><"mast"><"plumb"><"down"><"into"><"the"><"forecastle"><"aloft"><"there"><"to"><"the"><"royal">
<"mast-head"><"true"><"they"><"rather"><"order"><"me"><"about"><"some"><"and"><"make"><"me"><"jump"><"from"><"spar"><"to">
<"spar"><"like"><"a"><"grasshopper"><"in"><"a"><"may"><"meadow"><"and"><"at"><"first"><"this"><"sort"><"of"><"thing"><"is">
<"unpleasant"><"enough"><"it"><"touches"><"one's"><"sense"><"of"><"honour"><"particularly"><"if"><"you"><"come"><"of">
<"an"><"old"><"established"><"family"><"in"><"the"><"land"><"the"><"van"><"rensselaers"><"or"><"randolphs"><"or">
<"hardicanutes"><"and"><"more"><"than"><"all"><"if"><"just"><"previous"><"to"><"putting"><"your"><"hand"><"into"><"the">
<"tar-pot"><"you"><"have"><"been"><"lording"><"it"><"as"><"a"><"country"><"schoolmaster"><"making"><"the"><"tallest">
<"boys"><"stand"><"in"><"awe"><"of"><"you"><"the"><"transition"><"is"><"a"><"keen"><"one"><"i"><"assure"><"you"><"from"><"a">
<"schoolmaster"><"to"><"a"><"sailor"><"and"><"requires"><"a"><"strong"><"decoction"><"of"><"seneca"><"and"><"the">
<"stoics"><"to"><"enable"><"you"><"to"><"grin"><"and"><"bear"><"it"><"but"><"even"><"this"><"wears"><"off"><"hi"><"time"><"what">
<"of"><"it"><"if"><"some"><"old"><"hunks"><"of"><"a"><"sea-captain"><"orders"><"me"><"to"><"get"><"a"><"broom"><"and"><"sweep">
<"down"><"the"><"decks"><"what"><"does"><"that"><"indignity"><"amount"><"to"><"weighed"><"i"><"mean"><"in"><"the"><"scales">
<"of"><"the"><"new"><"testament"><"do"><"you"><"think"><"the"><"archangel"><"gabriel"><"thinks"><"anything"><"the">
<"less"><"of"><"me"><"because"><"i"><"promptly"><"and"><"respectfully"><"obey"><"that"><"old"><"hunks"><"in"><"that">
<"particular"><"instance"><"who"><"ain/t"><"a"><"slave"><"tell"><"me"><"that"><"well"><"then"><"however">
<"the~old^sea"><"-captains"><"may"><"order"><"me"><"about"><"however"><"they"><"may"><"thump"><"and"><"punch"><"me">
<"about"><"i"><"have"><"the"><"satisfaction"><"of"><"knowing"><"that"><"it"><"is"><"all"><"rightthat"><"everybody">
<"else"><"is"><"one"><"way"><"or"><"other"><"served"><"in"><"much"><"the"><"same"><"way"><"either"><"in"><"a"><"physical"><"or">
<"metaphysical"><"point"><"of"><"view"><"that"><"is"><"and"><"so"><"the"><"universal"><"thump"><"is"><"passed"><"round">
<"and"><"all"><"hands"><"should"><"rub"><"each"><"other's"><"shoulderblades"><"and"><"be"><"content"><"again"><"i">
<"always"><"go"><"to"><"sea"><"as"><"a"><"sailor"><"because"><"they"><"make"><"a"><"point"><"of"><"paying"><"me"><"for"><"my">
<"trouble"><"whereas"><"they"><"never"><"pay"><"passengers"><"a"><"single"><"penny"><"that"><"i"><"ever"><"heard"><"of">
<"on"><"the"><"contrary"><"passengers"><"themselves"><"must"><"pay"><"and"><"there"><"is"><"all"><"the"><"difference">
<"in"><"the"><"world"><"between"><"paying"><"and"><"being"><"paid"><"the"><"act"><"of"><"paying"><"is"><"perhaps"><"the">
<"most"><"uncomfortable"><"infliction"><"that"><"the"><"two"><"orchard"><"thieves"><"entailed"><"upon"><"us"><"but">
<"being"><"paid"><"what"><"will"><"compare"><"with"><"it"><"the"><"urbane"><"activity"><"with"><"which"><"a"><"man">
<"receives"><"money"><"is"><"really"><"marvellous"><"considering"><"that"><"we"><"so"><"earnestly"><"believe">
<"money"><"to"><"be"><"the"><"root"><"of"><"all"><"earthly"><"ills"><"and"><"that"><"on"><"no"><"account"><"can"><"a"><"monied">
<"man"><"enter"><"heaven"><"ah"><"how"><"cheerfully"><"we"><"consign"><"ourselves"><"to"><"perditionfinally"><"i">
<"always"><"go"><"to"><"sea"><"as"><"a"><"sailor"><"because"><"of"><"the"><"wholesome"><"exercise"><"and"><"pure"><"air"><"of">
<"the"><"forecastle"><"deck"><"for"><"as"><"in"><"this"><"world"><"head-winds"><"are"><"far"><"more"><"prevalent"><"than">
<"winds"><"from"><"astern"><"(that"><"is"><"if"><"you"><"never"><"violate"><"the"><"pythagorean"><"maxim),"><"so"><"for">
<"the"><"most"><"part"><"the"><"commodore"><"on"><"the"><"quarter-deck"><"gets"><"his"><"atmosphere"><"at"><"second">
<"hand"><"from"><"the"><"sailors"><"on"><"the"><"forecastle"><"he"><"thinks"><"he"><"breathes"><"it"><"first"><"but"><"not">
<"so"><"in"><"much"><"the"><"same"><"way"><"do"><"the"><"commonalty"><"lead"><"their"><"leaders"><"in"><"many"><"other">
<"things"><"at"><"the"><"same"><"time"><"that"><"the"><"leaders"><"little"><"suspect"><"it"><"but"><"wherefore"><"it"><"was">
<"that"><"after"><"having"><"repeatedly"><"smelt"><"the"><"sea"><"as"><"a"><"merchant"><"sailor"><"i"><"should"><"now">
<"take"><"it"><"into"><"my"><"head"><"to"><"go"><"on"><"a"><"whaling"><"voyage"><"this"><"the"><"invisible">
<"police-officer"><"of"><"the"><"fates"><"who"><"has"><"the"><"constant"><"surveillance"><"of"><"me"><"and">
<"secretly"><"dogs"><"me"><"and"><"influences"><"me"><"in"><"some"><"unaccountable"><"way"><"he"><"can"><"better">
<"answer"><"than"><"any"><"one"><"else"><"and"><"doubtless"><"my"><"going"><"on"><"this"><"whaling"><"voyage"><"formed">
<"part"><"of"><"the"><"grand"><"programme"><"of"><"providence"><"that"><"was"><"drawn"><"up"><"a"><"long"><"time"><"ago"><"it">
<"came"><"in"><"as"><"a"><"sort"><"of"><"brief"><"interlude"><"and"><"solo"><"between"><"more"><"extensive">
<"performances"><"i"><"take"><"it"><"that"><"this"><"part"><"of"><"the"><"bill"><"must"><"have"><"run"><"something"><"like">
<"thispart"><"of"><"a"><"whaling"><"voyage"><"when"><"others"><"were"><"set"><"down"><"for"><"magnificent"><"parts"><"in">
<"high"><"tragedies"><"and"><"short"><"and"><"easy"><"parts"><"in"><"genteel"><"comedies"><"and"><"jolly"><"parts"><"in">
<"farces"><"though"><"i"><"cannot"><"tell"><"why"><"this"><"was"><"exactly"><"yet"><"now"><"that"><"i"><"recall"><"all"><"the">
<"circumstances"><"i"><"think"><"i"><"can"><"see"><"a"><"little"><"into"><"the"><"springs"><"and"><"motives"><"which">
<"being"><"cunningly"><"presented"><"to"><"me"><"under"><"various"><"disguises"><"induced"><"me"><"to"><"set"><"about">
<"performing"><"the"><"part"><"i"><"did"><"besides"><"cajoling"><"me"><"into"><"the"><"delusion"><"that"><"it"><"was"><"a">
<"choice"><"resulting"><"from"><"my"><"own"><"unbiased"><"freewill"><"and"><"discriminating"><"judgment">
<"chief"><"among"><"these"><"motives"><"was"><"the"><"overwhelming"><"idea"><"of"><"the"><"great"><"whale"><"himself">
<"such"><"a"><"gortentous"><"and"><"mysterious"><"monster"><"roused"><"all"><"my"><"curiosity"><"then"><"the"><"wild">
<"and"><"distant"><"seas"><"where"><"he"><"rolled"><"his"><"island"><"bulkthe"><"undeliverable"><"nameless">
<"perils"><"of"><"the"><"whale"><"these"><"with"><"all"><"the"><"attending"><"marvels"><"of"><"a"><"thousand">
<"patagonian"><"sights"><"and"><"sounds"><"helped"><"to"><"sway"><"me"><"to"><"my"><"wish"><"with"><"other"><"men">
<"perhaps"><"such"><"things"><"would"><"not"><"have"><"been"><"inducements"><"but"><"as"><"for"><"me"><"i"><"am">
<"tormented"><"with"><"an"><"everlasting"><"itch"><"for"><"things"><"remote"><"i"><"love"><"to"><"sail"><"forbidden">
<"seas"><"and"><"land"><"on"><"barbarous"><"coasts"><"not"><"ignoring"><"what"><"is"><"good"><"i"><"am"><"quick"><"to">
<"perceive"><"a"><"horror"><"and"><"could"><"still"><"be"><"social"><"with"><"it"><"would"><"they"><"let"><"me"><"since"><"it">
<"is"><"but"><"well"><"to"><"be"><"on"><"friendly"><"terms"><"with"><"all"><"the"><"inmates"><"of"><"the"><"place"><"one">
<"lodges"><"in"><"by"><"reason"><"of"><"these"><"things"><"then"><"the"><"whaling"><"voyage"><"was"><"welcome"><"the">
<"great"><"flood-gates"><"of"><"the"><"wonder-world"><"swung"><"open"><"and"><"in"><"the"><"wild"><"conceits"><"that">
<"swayed"><"me"><"to"><"my"><"purpose"><"two"><"and"><"two"><"there"><"floated"><"into"><"my"><"inmost"><"soul"><"endless">
<"processions"><"of"><"the"><"whale"><"and"><"midmost"><"of"><"them"><"all"><"one"><"grand"><"hooded"><"phantom"><"like">
<"a"><"snow"><"hill"><"in"><"the"><"air">
```

</details>

```cpp
Key key("loomings");
Cluster cluster(IpV4Addr("<ip in cluster>"));
cluster.load_from_file(key, "loomings.sor");

class WCRower : public Rower {
public:
  std::map<std::string, int> results;
  bool accept(const Row &row) {
    const Schema &scm = row.get_schema();
    for (int i = 0; i < scm.width(); i++) {
      if (Data::Type::STRING == scm.col_type(i) && !row.is_missing(i)) {
        const string &s = *row.get<string *>(i);
        results.try_emplace(s, 0);
        results[s] += 1;
      }
    }
  }

  void join_delete(Rower *o) {
    WCRower *other = dynamic_cast<WCRower *>(o);
    assert(other);
    for (auto it = other.results.begin(); it != other.results.end(); it++) {
      results.try_emplace(it->first, 0);
      results[it->first] += it->second;
    }
    delete other;
  }
};

WordCountRower rower;
cluster.map(key, rower);

for (auto it = rower.results.begin(); it != rower.results.end(); it++) {
  cout << "word: " << it->first << ", count: " << it->second << endl;
}
```


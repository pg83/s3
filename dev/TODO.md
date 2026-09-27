# TODO

Gaps in the temporal logic found in the audit of 2026-09-27, in the
order they were found; the first one is fixed in 31, the rest wait.

## A cell that keeps its connection and answers nothing

A cell stuck in a system call on a dying disk keeps its sockets open,
so neither a broken connection nor a refused dial ever tells a link it
is gone; the link waits for its answers forever. A put whose third
piece went there keeps the whole object in the front's memory for as
long as it waits. Once the cell stops reading its socket, the link's
goroutine blocks on the write, the link's inbox fills, and every put or
get with a piece on that cell blocks handing its request to the link,
where the client leaving is not noticed. Each of those holds one of the
front's 32 slots; once they are all held the front answers nothing,
about objects with nothing on that cell too, and every front has a
link to that cell. A repair walks its queue one key at a time and stops
at the first key with a piece there. Only the cell's death or recovery
ends it; restarting the cell by hand does.

The one place to bound this is the link: when its oldest unanswered
request is older than a set limit, it drops the connection and fails
every waiting request, after which everything above already handles a
cell that failed. A limit on an operation, not a verdict on the
network.

## Settle keeps pieces in memory without bound

The settle holds every piece of an object until the third pieces are
acknowledged. A host that answers in minutes rather than milliseconds,
as lab2 did on the Azerty SSD on 2026-09-26, lets that grow without
bound: the front on lab1 reached 100 GB and was killed. A budget of
bytes in flight per link, beyond which a piece for that link counts as
not placed and becomes the host's debt, would bound it; deferred with
"everything at once for now".

## Scan compares the front's clock with its own

The mark under inprogress/ carries the time of the front that wrote
it; the scan compares it with the clock of the host it runs on. A clock
ahead of the others delays the ten minutes, one far ahead keeps its
marks young forever. With chrony this is seconds; note it and leave it.

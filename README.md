[![Build
Status](https://travis-ci.com/m-lab/mlab-ns-rate-limit.svg?branch=master)](https://travis-ci.com/m-lab/mlab-ns-rate-limit)
[![Coverage
Status](https://coveralls.io/repos/m-lab/mlab-ns-rate-limit/badge.svg?branch=master&service=github)](https://coveralls.io/github/m-lab/mlab-ns-rate-limit?branch=master)

# mlab-ns-rate-limit

Rate limiting support for mlab-ns.

## Memcache Conventions

mlab-ns-rate-limit writes request signature probabilities directly _to_
memcache. And, within the same project, mlab-ns is able to read request
signatures back _from_ memcache.

Because mlab-ns-rate-limit is written in Go, and mlab-ns is written in
Python, and memcache exposes a generic byte array in Go but a type-aware
serializer for Python, the two services cannot parse each other's data without
special handling.

In our case, we use base 10, ASCII encoded integers, which encode fixed point
decimal values with precision up to 0.0001. For example, to encode a probability
value of 0.5, mlab-ns-rate-limit will:

* string(0.5 * 10000) -> "5000"

And, mlab-ns would read this value as:

* int("5000") / 10000.0 -> 0.5

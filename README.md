[![Build
Status](https://travis-ci.com/m-lab/mlab-ns-rate-limit.svg?branch=master)](https://travis-ci.com/m-lab/mlab-ns-rate-limit)
[![Coverage
Status](https://coveralls.io/repos/m-lab/mlab-ns-rate-limit/badge.svg?branch=master&service=github)](https://coveralls.io/github/m-lab/mlab-ns-rate-limit?branch=master)

# mlab-ns-rate-limit
Rate limiting support for mlab-ns

This repository contains the rate_limiter service code.  The rate_limiter service responds to
requests to /update by querying the bigquery table containing mlab-ns logs, to determine
which client *endpoints* are making excessive requests to mlab-ns.

The *endpoints* are identified by concatenating the request *resource*, the *userAgent* string, and
the IP address of the client (either the direct IP address, or address specified in the request).

Currently, we track all endpoint signatures that have made more than 12 requests in the past 24 hours.
The mlab-ns service uses this data to statistically redirect or drop requests in proportion to the
excess request rate.  I.e., if an endpoint sent 36 request in the past 24 hours, then mlab-ns will
drop or redirect 2/3 of subsequent requests.

Note that if a client starts sending more requests in response to this behavior, the system will
respond within a few minutes by further increasing the fraction of requests that are redirected or
dropped.

The endpoint data is stored in datastore in the *endpoints* namespace, under the *Requests* kind.

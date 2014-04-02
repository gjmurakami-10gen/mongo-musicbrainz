# MusicBrainz to MongoDB

This project loads the MusciBrainz database into MongoDB
and denormalizes it into a smaller set of useful collections.
The intention is to develop reasonable aggregation framework examples
that are non-trivial and computationally significant.
These examples can be used to test performance and optimizations
for the mongo-gpu project as we track GPU computing.
We also hope that general-purpose tools for denomalization will emerge.

# Installation

```
\curl -sSL https://get.rvm.io | bash -s stable # install RVM - https://rvm.io/
rvm install 2.1.1
rvm --default use 2.1.1
bundle install
rake
```

The output of rake outlines the ordered steps for the project.

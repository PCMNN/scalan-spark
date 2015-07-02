scalan-starter
==============

In order to build scalan-starter first you need:
 - clone, build and `publishLocal`
`scalan-develop` branch of https://github.com/scalan/virtualization-lms-core.
 - clone, build and `publishLocal` https://github.com/scalan/scalan-ce

special dependies for topic-ml
=============================

you can run
```
cd dependencies
make
```

There are Makefile in this folder, that contains commands for clone, switch to important branch build and publish commanda
for all internal dependencies projects (lms, scalan-lita, scalan-ml)

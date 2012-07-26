==========================================
 Django Redis Engine for django-nonrel 0.1.*
==========================================

What?
=====
Forked from https://github.com/MirkoRossini/django-redis-engine since there were some minor bugs initially. 

What was changed
================
* No longer Pickle's ints/floats so that you can use atomic redis functions on the fields (HINCRBY, etc)
* Removed deprecated calls to md5
* Bug fixes

Requirements
============
* Redis 
* Redis bindings for python
* `Django-nonrel`_
* `djangotoolbox`_
.. _Django-nonrel: http://bitbucket.org/wkornewald/django-nonrel
.. _djangotoolbox: http://bitbucket.org/wkornedwald/djangotoolbox

Optional Requirements
============
* dbindexer

Features
========
Indexing for:
* startswith,istartswith
* endswith,iendswith
* gt,gte,lt,lte
* contains (using dbindexer)

Redis transaction support: you can execute multiple insert of django objects in one single pipeline. See testproject/testapp/tests.py
Count queries


Missing features
===========
Aggregate queries
Documentation (although testproject is self-documented)



Contributing
============
You are highly encouraged to participate in the development, simply use
GitHub's fork/pull request system.
If you don't like GitHub (for some reason) you're welcome
to send regular patches to the mailing list.

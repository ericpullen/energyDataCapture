"""Test package for ``energy_capture``.

Making ``tests`` a package (rather than a bare directory of modules) means test
modules get stable dotted names, so helpers can be imported explicitly::

    from tests.conftest import utc, LOCAL_TZ

and two test modules may share a basename without shadowing each other.
"""

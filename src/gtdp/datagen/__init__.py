"""Deterministic generator of the J1/J2/J3 landing drops and the sensor stream.

The output format is fixed by docs/data-contract.md. `verify` re-reads that output with its
own rules, and `replay` streams the sensor files into the directory Spark watches.
"""

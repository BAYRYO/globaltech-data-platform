"""The list of job modules. Importing a module registers its ``@job`` functions.

To add a job, create ``gtdp/jobs/<module>.py`` with an ``@job`` function and import it below.
"""

from gtdp.jobs import bronze, revenue_by_country, silver, smoke, streaming  # noqa: F401
from gtdp.jobs.registry import registered

JOBS = registered()

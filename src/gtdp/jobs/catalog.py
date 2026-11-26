"""The list of job modules. Importing a module registers its ``@job`` functions.

To add a job, create ``gtdp/jobs/<module>.py`` with an ``@job`` function and import it below.
"""

from gtdp.jobs import revenue_by_country, smoke  # noqa: F401
from gtdp.jobs.registry import registered

JOBS = registered()

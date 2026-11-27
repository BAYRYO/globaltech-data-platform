"""Every interleaving of runs that take, keep, release or die holding a table lock.

The scenarios marked ``ci`` run here. ``python tests/core/lock_model.py`` runs them all, the
larger ones included (three and four runs, from minutes to hours each).
"""

import pytest

from .lock_model import SCENARIOS, explore, patch


@pytest.mark.parametrize("title", [title for title, s in SCENARIOS.items() if s.ci])
def test_no_interleaving_lets_two_runs_hold_or_one_delete_another_live_runs_file(
    monkeypatch, title
):
    patch(monkeypatch.setattr)

    result = explore(SCENARIOS[title])

    assert result.violation is None, "\n".join(map(str, [result.violation, *result.schedule]))
    assert result.terminals

import pytest

from palinor.core import Forbidden, Palinor, PalinorError
from palinor.events import EventBus
from palinor.graph import MemoryGraph
from palinor.config import Config
from palinor.rule_templates import TEMPLATES


@pytest.fixture
def svc(tmp_path):
    return Palinor(MemoryGraph(), EventBus("", tmp_path / "e.jsonl"), Config(data_dir=tmp_path, kafka_bootstrap=""))


def test_template_loads_on_create_and_reaches_sync(svc):
    proj = svc.create_project("Hack", "build a thing", template="hackathon")
    assert proj["rules"] == len(TEMPLATES["hackathon"])
    w = svc.join(proj["project_id"], proj["join_key"], "", "chatgpt")
    p = svc.auth(w["token"])
    sync = svc.sync(p)
    assert len(sync["project_rules"]) == len(TEMPLATES["hackathon"])
    assert any("code_verify" in r for r in sync["project_rules"])


def test_apply_template_skips_duplicates_and_needs_admin(svc):
    proj = svc.create_project("Study", "research", template="research")
    pid, ak = proj["project_id"], proj["admin_key"]
    n = len(svc.project_rules(pid))
    svc.apply_template(pid, ak, "research")
    assert len(svc.project_rules(pid)) == n  # nothing added twice
    svc.apply_template(pid, ak, "hackathon")  # common rules are shared, only the hackathon extras are added
    assert len(svc.project_rules(pid)) == len(TEMPLATES["hackathon"])  # research is a subset of hackathon
    with pytest.raises(Forbidden):
        svc.apply_template(pid, "wrong-key", "general")
    with pytest.raises(PalinorError):
        svc.apply_template(pid, ak, "party")


def test_no_template_means_no_rules(svc):
    proj = svc.create_project("Plain", "x")
    assert proj["rules"] == 0 and svc.project_rules(proj["project_id"]) == []

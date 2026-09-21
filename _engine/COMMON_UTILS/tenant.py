import os, json
_ENGINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_cfg = json.load(open(os.path.join(_ENGINE, "campaign-config.json")))
STATE = os.environ.get("STATE_CODE", "")
_states = _cfg.get("states", {})
if STATE not in _states:
    raise RuntimeError("STATE_CODE %r not configured. Available: %s" % (STATE, list(_states)))
_s = _states[STATE]
PREFIX   = _s["tenant_id"]
CAMPAIGN = _s.get("campaign_number", "")
def es_index(name):
    return "https://elasticsearch-data.es-cluster-v8:9200/%s-%s-index-v1/_search" % (PREFIX, name)

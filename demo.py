"""Generate a toy example in disposable directories; print no local paths."""
from pathlib import Path
import tempfile
from state_store import Counter, Store

with tempfile.TemporaryDirectory() as folder:
    base = Path(folder)
    (base / "data").mkdir(); (base / "trusted").mkdir()
    store = Store(base / "data" / "state.json", base / "trusted" / "roots.json")
    store.initialize()
    state = Counter()
    for delta in (2, 3, -1): state.add(delta)
    store.save(state)
    recovered, status = store.recover()
    print({"status": status, "events": len(recovered.events), "value": recovered.value})

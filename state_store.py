"""Minimal counter-state persistence example for review; standard library only."""
from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile

MAX_BYTES, MAX_EVENTS = 1048576, 1000


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")


def root(data):
    return sha256(data).hexdigest()


def is_root(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def read(path):
    with path.open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("size limit")
    return data


def parse(data):
    if type(data) is not bytes or len(data) > MAX_BYTES:
        raise ValueError("bounded bytes required")
    value = json.loads(data.decode("ascii"))
    if encode(value) != data:
        raise ValueError("noncanonical JSON")
    return value


class Counter:
    def __init__(self):
        self.events = []
        self.value = 0

    def add(self, delta):
        if type(delta) is not int or not -1000 <= delta <= 1000 or len(self.events) >= MAX_EVENTS:
            raise ValueError("bounded integer delta required")
        self.value += delta
        self.events.append(dict(seq=len(self.events) + 1, delta=delta, after=self.value))

    @classmethod
    def replay(cls, events):
        if type(events) is not list or len(events) > MAX_EVENTS:
            raise ValueError("bounded event list required")
        counter = cls()
        for event in events:
            if (type(event) is not dict or set(event) != {"seq", "delta", "after"}
                    or type(event["seq"]) is not int or type(event["after"]) is not int):
                raise ValueError("event schema")
            counter.add(event["delta"])
            if counter.events[-1] != event:
                raise ValueError("event replay mismatch")
        return counter


def snapshot(counter):
    checked = Counter.replay(counter.events)
    if checked.value != counter.value:
        raise ValueError("state does not match journal")
    data = encode(dict(format="counter-v1", events=checked.events,
                       checkpoint=dict(count=len(checked.events), value=checked.value,
                                       events_sha256=root(encode(checked.events)))))
    if len(data) > MAX_BYTES:
        raise ValueError("snapshot size limit")
    return data


def restore(data):
    value = parse(data)
    if type(value) is not dict or set(value) != {"format", "events", "checkpoint"} or value["format"] != "counter-v1":
        raise ValueError("snapshot schema")
    counter = Counter.replay(value["events"])
    if snapshot(counter) != data:
        raise ValueError("checkpoint mismatch")
    return counter


@contextmanager
def locked(path):
    """Nonblocking advisory lock; all writers/readers must cooperate."""
    lock = path.with_name(path.name + ".lock")
    if lock.is_symlink():
        raise ValueError("lock symlink")
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    acquired = False
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"0")
        os.lseek(fd, 0, os.SEEK_SET)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        yield
    finally:
        try:
            if acquired:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def atomic_write(path, data, hit):
    """A process crash leaves old/new target; this is NOT power-loss proof."""
    fd, name = tempfile.mkstemp(prefix="." + path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            middle = len(data) // 2
            stream.write(data[:middle]); stream.flush(); hit("partial")
            stream.write(data[middle:]); stream.flush(); os.fsync(stream.fileno())
        hit("before_replace")
        os.replace(temporary, path)
        hit("after_replace")
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


class Store:
    def __init__(self, bundle, trusted):
        self.bundle, self.trusted = (Path(p).parent.resolve(strict=True) / Path(p).name for p in (bundle, trusted))
        a, b = self.bundle.parent, self.trusted.parent
        if a == b or a.is_relative_to(b) or b.is_relative_to(a):
            raise ValueError("separate nonnested directories required")
        if self.bundle.is_symlink() or self.trusted.is_symlink():
            raise ValueError("trusted regular paths required")

    def _roots(self):
        value = parse(read(self.trusted))
        if (type(value) is not dict or set(value) != {"format", "target", "head", "pending"}
                or value["format"] != "roots-v1" or value["target"] != str(self.bundle)
                or value["head"] is not None and not is_root(value["head"])):
            raise ValueError("trusted roots schema/binding")
        pending = value["pending"]
        if pending is not None and (type(pending) is not dict or set(pending) != {"old", "new"}
                or pending["old"] != value["head"] or not is_root(pending["new"])
                or pending["new"] == pending["old"]):
            raise ValueError("pending intent schema")
        return value

    def initialize(self):
        with locked(self.trusted), locked(self.bundle):
            if self.trusted.exists():
                self._roots()
                return
            if self.bundle.exists():
                raise ValueError("cannot infer trust from existing data")
            atomic_write(self.trusted, encode(dict(format="roots-v1", target=str(self.bundle), head=None, pending=None)), lambda _: None)

    def _observe(self, allowed):
        if not self.bundle.exists():
            if None not in allowed:
                raise ValueError("committed data missing")
            return None, None
        data = read(self.bundle)
        if root(data) not in allowed:
            raise ValueError("unknown data: keep intent open")
        return restore(data), root(data)

    def save(self, counter, hit=lambda _: None):
        data = snapshot(counter)
        prepared_events = restore(data).events
        with locked(self.trusted), locked(self.bundle):
            roots = self._roots()
            if roots["pending"] is not None:
                raise ValueError("recover pending intent first")
            old, _ = self._observe((roots["head"],))
            if old is not None and prepared_events[:len(old.events)] != old.events:
                raise ValueError("journal must extend committed prefix")
            candidate = root(data)
            if candidate == roots["head"]:
                return
            roots["pending"] = dict(old=roots["head"], new=candidate)
            atomic_write(self.trusted, encode(roots), lambda p: hit("intent:" + p))
            atomic_write(self.bundle, data, lambda p: hit("data:" + p))
            hit("data:ack_lost_window")
            roots["head"], roots["pending"] = candidate, None
            atomic_write(self.trusted, encode(roots), lambda p: hit("confirm:" + p))

    def recover(self, hit=lambda _: None):
        with locked(self.trusted), locked(self.bundle):
            roots = self._roots()
            pending = roots["pending"]
            allowed = (roots["head"],) if pending is None else (pending["old"], pending["new"])
            counter, visible = self._observe(allowed)
            status = "ready" if visible is not None else "empty"
            if pending is not None:
                status = "new" if visible == pending["new"] else "absent" if visible is None else "old"
                roots["head"], roots["pending"] = visible, None
                atomic_write(self.trusted, encode(roots), lambda p: hit("resolve:" + p))
            return counter, status

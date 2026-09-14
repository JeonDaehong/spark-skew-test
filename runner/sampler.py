"""
run 하나가 도는 동안 커널 상태를 일정 간격으로 샘플링한다.

여기서 뽑는 값들이 RQ4(page cache / writeback 이 cliff 을 만드는가)의 증거가 된다.
Spark 메트릭만으로는 절대 안 보이는 것들:
  - /proc/meminfo Dirty / Writeback  : spill 이 page cache 에 흡수되는 중인지, 밀리는 중인지
  - /proc/pressure/io  full          : 태스크가 실제로 I/O 때문에 멈춰 있었는지 (PSI)
  - /proc/pressure/memory            : 메모리 압력으로 인한 stall
  - /proc/vmstat nr_dirty, pgpgout   : 실제 writeback 양
"""
import threading
import time


MEMINFO_KEYS = ("MemTotal", "MemFree", "MemAvailable", "Cached", "Dirty",
                "Writeback", "WritebackTmp", "SwapCached")
VMSTAT_KEYS = ("nr_dirty", "nr_writeback", "pgpgin", "pgpgout", "pgfault",
               "pgmajfault", "pswpout",
               # 커널이 실제로 계산한 dirty 임계값. sysctl 의 % 가 아니라 페이지 수다.
               # "spill 이 임계를 넘었는가"를 추정이 아니라 실측으로 판정하기 위해 필요.
               "nr_dirty_threshold", "nr_dirty_background_threshold",
               "nr_writeback_temp")


def _read_meminfo():
    out = {}
    with open("/proc/meminfo") as fh:
        for line in fh:
            k, _, rest = line.partition(":")
            if k in MEMINFO_KEYS:
                out["mem_" + k] = int(rest.split()[0])  # kB
    return out


def _read_vmstat():
    out = {}
    with open("/proc/vmstat") as fh:
        for line in fh:
            k, _, v = line.partition(" ")
            if k in VMSTAT_KEYS:
                out["vm_" + k] = int(v)
    return out


def _read_pressure():
    """PSI. WSL2 커널 6.18 에서 사용 가능함을 실측 확인."""
    out = {}
    for res in ("cpu", "io", "memory"):
        try:
            with open(f"/proc/pressure/{res}") as fh:
                for line in fh:
                    kind, *fields = line.split()
                    for f in fields:
                        name, _, val = f.partition("=")
                        if name == "total":
                            out[f"psi_{res}_{kind}_total"] = int(val)
        except FileNotFoundError:
            pass
    return out


def _read_stat():
    with open("/proc/stat") as fh:
        for line in fh:
            if line.startswith("cpu "):
                v = [int(x) for x in line.split()[1:9]]
                keys = ("user", "nice", "system", "idle", "iowait",
                        "irq", "softirq", "steal")
                return {"cpu_" + k: x for k, x in zip(keys, v)}
            if line.startswith("ctxt "):
                break
    return {}


def _read_ctxt_procs():
    out = {}
    with open("/proc/stat") as fh:
        for line in fh:
            if line.startswith("ctxt "):
                out["ctxt"] = int(line.split()[1])
            elif line.startswith("procs_running"):
                out["procs_running"] = int(line.split()[1])
            elif line.startswith("procs_blocked"):
                out["procs_blocked"] = int(line.split()[1])
    return out


def snapshot():
    s = {"t": time.time()}
    s.update(_read_meminfo())
    s.update(_read_vmstat())
    s.update(_read_pressure())
    s.update(_read_stat())
    s.update(_read_ctxt_procs())
    return s


class Sampler(threading.Thread):
    """별도 스레드에서 interval 마다 snapshot 을 모은다."""

    def __init__(self, interval=0.25):
        super().__init__(daemon=True)
        self.interval = interval
        self._stop_evt = threading.Event()
        self.rows = []

    def run(self):
        while not self._stop_evt.is_set():
            try:
                self.rows.append(snapshot())
            except Exception:  # 샘플링 실패가 실험을 죽이면 안 된다
                pass
            self._stop_evt.wait(self.interval)

    def stop(self):
        self._stop_evt.set()
        self.join(timeout=5)
        return self.rows

    def to_csv(self, path):
        if not self.rows:
            return 0
        cols = sorted({k for r in self.rows for k in r})
        cols.remove("t")
        cols = ["t", "t_rel"] + cols
        t0 = self.rows[0]["t"]
        with open(path, "w") as fh:
            fh.write(",".join(cols) + "\n")
            for r in self.rows:
                r = dict(r, t_rel=round(r["t"] - t0, 3))
                fh.write(",".join(str(r.get(c, "")) for c in cols) + "\n")
        return len(self.rows)

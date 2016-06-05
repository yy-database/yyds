"""共享 ``.yyds`` catalog 的 harness 侧写入。

真实 catalog schema 由 Rust 侧维护（版本号 v6）。harness 只写足够让 node
启动的最小 JSON：当前 epoch 和 shard → owner 映射。原子 rename 落盘。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


@dataclass
class OwnerEntry:
    shard_id: str
    node_id: str
    term: int


@dataclass
class CatalogWriter:
    """harness 向 ``.yyds`` catalog 原子写入 epoch 和 owner table。"""

    path: Path
    epoch: int = 0
    owners: list[OwnerEntry] = field(default_factory=list)

    def publish_epoch(self, epoch: int, owners: list[tuple[str, str, int]]) -> None:
        """发布新 epoch。原子 rename 保证 node 读到完整 JSON。"""
        self.epoch = epoch
        self.owners = [OwnerEntry(shard_id=s, node_id=n, term=t) for s, n, t in owners]
        self._flush()

    def _flush(self) -> None:
        data = {
            "epoch": self.epoch,
            "owners": [
                {"shard": o.shard_id, "node": o.node_id, "term": o.term}
                for o in self.owners
            ],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 原子写：写临时文件 → os.replace
        fd, tmp = tempfile.mkstemp(
            prefix=".yyds-", dir=str(self.path.parent), suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, self.path)
        except Exception:
            os.unlink(tmp)
            raise

    def __iter__(self) -> Iterator[OwnerEntry]:
        return iter(self.owners)

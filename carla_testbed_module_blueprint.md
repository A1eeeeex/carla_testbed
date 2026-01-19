# CARLA 算法测试平台（ROS2 + CyberRT）Python 工程蓝图（模块清单 + 接口定义）

> 目标：将 CARLA 的**场景复现**、**多传感器输入**、**真值（Oracle）输出**、**算法 I/O（ROS2/CyberRT）**、**控制闭环**、**录制回放**、**KPI 评测与回归对比**组合成一套可长期迭代的测试平台。  
> 本文给出推荐的**目录结构**与每个模块的**Python 接口定义**（以 `Protocol` + `dataclass` 的形式约束契约）。

---

## 1. 推荐目录结构（模块清单）

```
carla_testbed/
  README.md
  pyproject.toml
  carla_testbed/
    __init__.py

    # 0) 统一类型 / Schema
    schemas/
      __init__.py
      frame_packet.py          # FramePacket / SensorSample
      truth_packet.py          # GroundTruthPacket / ObjectTruth / Event
      algo_io.py               # AlgoInput/AlgoOutput/ControlCommand
      calib.py                 # Intrinsic/Extrinsic/Frames
      manifest.py              # RunManifest/Artifacts index
      kpi.py                   # KPIResult schemas

    # 1) 配置与版本化（可复现）
    config/
      __init__.py
      loader.py                # load/save config
      defaults.py
      validators.py            # schema validation

    # 2) 场景模块（Scenario & Traffic Flow）
    scenarios/
      __init__.py
      base.py                  # Scenario / ScenarioManager interfaces
      catalog.py               # scenario registry
      builders/
        __init__.py
        carla_native.py        # CARLA 原生 spawn/traffic manager builder
        osc2.py                # OpenSCENARIO (可选)
      policies/
        __init__.py
        traffic_flow.py        # traffic flow policy
        pedestrian_flow.py

    # 3) 仿真控制与世界管理
    sim/
      __init__.py
      carla_client.py          # CARLA client/wrapper
      world.py                 # WorldHandle
      tick.py                  # sync/async tick controller
      actors.py                # spawn ego & actors

    # 4) 传感器模块（定义/挂载/采集）
    sensors/
      __init__.py
      base.py                  # SensorSpec / SensorManager / SensorDriver
      specs.py                 # 常用传感器模板（泛用安装）
      drivers/
        __init__.py
        camera.py
        lidar.py
        radar.py
        imu.py
        gnss.py

    # 5) 时间同步与对齐（Frame-level 打包）
    sync/
      __init__.py
      base.py                  # SyncPolicy interface
      hard_sync.py             # 等齐全部传感器
      soft_sync.py             # 最近邻/容忍度
      joiner.py                # FramePacketBuilder

    # 6) 真值（Oracle）导出模块
    oracle/
      __init__.py
      base.py                  # TruthProvider interface
      actors_truth.py          # 周围 actor 真值 + 3D box
      ego_truth.py             # ego pose/v/a/jerk
      traffic_light_truth.py
      lane_truth.py            # 可选：lane 关联

    # 7) 算法 I/O 适配层（ROS2 + CyberRT）
    io/
      __init__.py
      base.py                  # Publisher/Subscriber/TransportAdapter
      ros2_adapter.py
      cyber_adapter.py
      mapping/
        __init__.py
        topics_ros2.py         # topic 命名/类型映射
        channels_cyber.py      # channel 命名/类型映射

    # 8) 控制器与接管（闭环）
    control/
      __init__.py
      base.py                  # Controller interface
      pid.py                   # fallback controller (baseline)
      apollo_bridge.py         # （可选）Apollo 控制接入
      safety.py                # safety termination / takeover policy

    # 9) 录制/回放（传感器流、真值流、世界复现、视频）
    record/
      __init__.py
      base.py                  # Recorder interface
      rosbag2_recorder.py      # ROS2 bag
      cyber_record.py          # Cyber record
      carla_recorder.py        # CARLA 原生 recorder log
      video_recorder.py        # spectator / ego view video
      playback/
        __init__.py
        rosbag2_player.py
        cyber_player.py
        file_player.py         # 从 frames/manifest 回放

    # 10) 评测与回归（KPI & Baseline）
    eval/
      __init__.py
      base.py                  # Evaluator interface
      kpis/
        __init__.py
        safety.py              # collision/TTC/min_dist
        rules.py               # red light, speeding, lane invasion
        comfort.py             # accel/jerk
        tracking.py            # perception tracking metrics (可选)
        localization.py        # ATE/RPE (可选)
        control_tracking.py    # lateral/heading/speed errors
      reporter.py              # report.json / report.md
      baseline.py              # baseline store & regression gate

    # 11) 存储（Artifacts、manifest、索引）
    storage/
      __init__.py
      artifact_store.py        # local fs / s3 (可选)
      layout.py                # run directory layout
      compression.py

    # 12) 可视化与调试（可选）
    viz/
      __init__.py
      hud.py                   # overlay / debug HUD
      dump_on_failure.py       # 失败关键帧打包
      perf.py                  # RTF/CPU/GPU 监控（基础）

    # 13) 测试主干（Test Harness）
    runner/
      __init__.py
      harness.py               # TestHarness
      suite.py                 # ScenarioSuite
      cli.py                   # 命令行入口

    # 14) 通用工具
    utils/
      __init__.py
      clock.py
      transforms.py
      logging.py
      math.py

  examples/
    run_one_scenario.py
    run_suite_regression.py
    minimal_ros2_loopback.py
    minimal_cyber_loopback.py
```

---

## 2. 统一数据结构（schemas）

> 建议用 dataclass + 明确的 frame_id/timestamp 作为“单一真相”，所有模块围绕它对齐。

```python
# carla_testbed/schemas/frame_packet.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Literal

SensorType = Literal["camera", "lidar", "radar", "imu", "gnss", "segmentation", "depth", "custom"]

@dataclass(frozen=True)
class SensorSample:
    sensor_id: str
    sensor_type: SensorType
    frame_id: int
    timestamp: float  # seconds (sim time recommended)
    payload: Any      # raw bytes / numpy / message object / file ref
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class FramePacket:
    frame_id: int
    timestamp: float
    ego_pose_world: Optional[Dict[str, Any]] = None  # {x,y,z,qw,qx,qy,qz} or Pose dataclass
    samples: Dict[str, SensorSample] = field(default_factory=dict)
    frame_meta: Dict[str, Any] = field(default_factory=dict)
```

```python
# carla_testbed/schemas/truth_packet.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Literal, Optional

ObjectType = Literal["vehicle", "pedestrian", "cyclist", "static", "unknown"]
EventType  = Literal["collision", "lane_invasion", "red_light", "speeding", "timeout", "stuck"]

@dataclass(frozen=True)
class ObjectTruth:
    object_id: str
    object_type: ObjectType
    pose_world: Dict[str, Any]        # position + orientation
    velocity_world: Dict[str, Any]    # vx, vy, vz
    bbox: Optional[Dict[str, Any]] = None  # {cx,cy,cz, dx,dy,dz, yaw} etc.
    attrs: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class Event:
    event_type: EventType
    frame_id: int
    timestamp: float
    detail: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class GroundTruthPacket:
    frame_id: int
    timestamp: float
    ego_truth: Dict[str, Any]                  # pose/v/a/jerk if available
    objects: List[ObjectTruth] = field(default_factory=list)
    traffic_lights: List[Dict[str, Any]] = field(default_factory=list)
    lane_truth: Optional[Dict[str, Any]] = None
    events: List[Event] = field(default_factory=list)
```

```python
# carla_testbed/schemas/algo_io.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

@dataclass(frozen=True)
class ControlCommand:
    frame_id: int
    timestamp: float
    throttle: float
    brake: float
    steer: float
    gear: Optional[int] = None
    hand_brake: bool = False
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class AlgoOutput:
    # 允许不同算法输出不同内容：控制、感知结果、轨迹、定位等
    control: Optional[ControlCommand] = None
    perception: Optional[Dict[str, Any]] = None
    localization: Optional[Dict[str, Any]] = None
    planning: Optional[Dict[str, Any]] = None
    debug: Dict[str, Any] = field(default_factory=dict)
```

---

## 3. 核心接口定义（Protocol）

### 3.1 场景模块

```python
# carla_testbed/scenarios/base.py
from __future__ import annotations
from typing import Protocol, Dict, Any, List

class Scenario(Protocol):
    name: str
    def build(self, world: "WorldHandle", config: Dict[str, Any]) -> None: ...
    def reset(self, world: "WorldHandle") -> None: ...
    def is_done(self, world: "WorldHandle") -> bool: ...
    def get_goal(self) -> Dict[str, Any]: ...
    def get_tags(self) -> List[str]: ...

class ScenarioManager(Protocol):
    def load(self, scenario_id: str, config: Dict[str, Any]) -> Scenario: ...
    def seed(self, seed: int) -> None: ...
    def snapshot(self) -> Dict[str, Any]: ...  # 用于 manifest
```

### 3.2 仿真控制（World / Tick）

```python
# carla_testbed/sim/world.py
from __future__ import annotations
from typing import Protocol, Any

class WorldHandle(Protocol):
    def tick(self) -> int: ...
    def get_snapshot(self) -> Any: ...
    def get_map(self) -> Any: ...
    def get_weather(self) -> Any: ...
    def get_actors(self) -> Any: ...
    def spawn_actor(self, blueprint_id: str, transform: Any, attach_to: Any | None = None) -> Any: ...
    def destroy_actor(self, actor: Any) -> None: ...
    def apply_control(self, ego: Any, control: Any) -> None: ...
```

```python
# carla_testbed/sim/tick.py
from __future__ import annotations
from typing import Protocol

class TickController(Protocol):
    def configure(self, sync: bool, fixed_delta_seconds: float, max_substeps: int = 1) -> None: ...
    def step(self, world: "WorldHandle") -> int: ...
    def now(self) -> float: ...  # consistent timebase
```

### 3.3 传感器模块

```python
# carla_testbed/sensors/base.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Dict, Any, List

@dataclass(frozen=True)
class SensorSpec:
    sensor_id: str
    sensor_type: str
    blueprint_id: str
    transform: Dict[str, float]      # x,y,z,roll,pitch,yaw
    attributes: Dict[str, Any]       # CARLA blueprint attributes
    sensor_tick: float               # seconds

class SensorDriver(Protocol):
    def start(self, world: "WorldHandle", ego: Any, spec: SensorSpec) -> None: ...
    def stop(self) -> None: ...
    def latest(self) -> "SensorSample | None": ...
    def pop_all(self) -> List["SensorSample"]: ...

class SensorManager(Protocol):
    def attach_all(self, world: "WorldHandle", ego: Any, specs: List[SensorSpec]) -> None: ...
    def stop_all(self) -> None: ...
    def poll(self) -> List["SensorSample"]: ...  # 收集所有传感器新数据
    def snapshot(self) -> Dict[str, Any]: ...    # 传感器配置快照（manifest）
```

### 3.4 同步与对齐（Frame-level 打包）

```python
# carla_testbed/sync/base.py
from __future__ import annotations
from typing import Protocol, List, Optional, Dict, Any

class SyncPolicy(Protocol):
    def on_tick(self, frame_id: int, timestamp: float) -> None: ...
    def ingest(self, samples: List["SensorSample"]) -> None: ...
    def try_build(self) -> Optional["FramePacket"]: ...
    def snapshot(self) -> Dict[str, Any]: ...
```

### 3.5 真值（Oracle）

```python
# carla_testbed/oracle/base.py
from __future__ import annotations
from typing import Protocol, Dict, Any, Optional

class TruthProvider(Protocol):
    def on_tick(self, world: "WorldHandle", ego: Any, frame_id: int, timestamp: float) -> Optional["GroundTruthPacket"]: ...
    def snapshot(self) -> Dict[str, Any]: ...
```

### 3.6 I/O 适配层（ROS2 / CyberRT）

```python
# carla_testbed/io/base.py
from __future__ import annotations
from typing import Protocol, Dict, Any, Optional

class TransportAdapter(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def publish_frame(self, frame: "FramePacket") -> None: ...
    def publish_truth(self, truth: "GroundTruthPacket") -> None: ...
    def publish_calib(self, calib: Dict[str, Any]) -> None: ...
    def poll_control(self) -> Optional["ControlCommand"]: ...
    def poll_algo_output(self) -> Optional["AlgoOutput"]: ...
    def snapshot(self) -> Dict[str, Any]: ...
```

### 3.7 控制器（闭环 + 安全终止/接管）

```python
# carla_testbed/control/base.py
from __future__ import annotations
from typing import Protocol, Optional, Dict, Any

class Controller(Protocol):
    def reset(self) -> None: ...
    def step(self, algo_output: Optional["AlgoOutput"], fallback_input: Dict[str, Any]) -> "ControlCommand": ...

class SafetyPolicy(Protocol):
    def check_terminate(self, truth: "GroundTruthPacket") -> Optional[Dict[str, Any]]: ...
```

### 3.8 录制与回放

```python
# carla_testbed/record/base.py
from __future__ import annotations
from typing import Protocol, Dict, Any

class Recorder(Protocol):
    def start(self, run_dir: str, meta: Dict[str, Any]) -> None: ...
    def write_frame(self, frame: "FramePacket") -> None: ...
    def write_truth(self, truth: "GroundTruthPacket") -> None: ...
    def write_control(self, control: "ControlCommand") -> None: ...
    def stop(self) -> None: ...
    def snapshot(self) -> Dict[str, Any]: ...
```

### 3.9 评测与回归（KPI & Baseline）

```python
# carla_testbed/eval/base.py
from __future__ import annotations
from typing import Protocol, Dict, Any, Optional, List
from dataclasses import dataclass, field

@dataclass(frozen=True)
class KPIResult:
    name: str
    value: float
    unit: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

class Evaluator(Protocol):
    def reset(self, scenario_meta: Dict[str, Any]) -> None: ...
    def ingest(self, frame: "FramePacket", truth: "GroundTruthPacket", algo_output: Optional["AlgoOutput"]) -> None: ...
    def finalize(self) -> List[KPIResult]: ...
    def snapshot(self) -> Dict[str, Any]: ...
```

---

## 4. 测试主干（Harness）接口定义

```python
# carla_testbed/runner/harness.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

@dataclass
class HarnessConfig:
    sync: bool = True
    fixed_delta_seconds: float = 0.05
    max_steps: int = 10_000
    seed: int = 0
    dump_frames: bool = False
    enable_video: bool = True

class TestHarness:
    def __init__(
        self,
        world: "WorldHandle",
        tick: "TickController",
        scenario_mgr: "ScenarioManager",
        sensor_mgr: "SensorManager",
        sync_policy: "SyncPolicy",
        truth_provider: "TruthProvider",
        transport: "TransportAdapter",
        controller: "Controller",
        safety: "SafetyPolicy",
        recorders: List["Recorder"],
        evaluators: List["Evaluator"],
        baseline: Optional[Any] = None,
    ) -> None: ...

    def run_one(self, scenario_id: str, scenario_cfg: Dict[str, Any], harness_cfg: HarnessConfig) -> Dict[str, Any]:
        """返回 run_summary（含 manifest、KPI、events、artifacts）。"""
        ...

    def run_suite(self, suite: List[Dict[str, Any]], harness_cfg: HarnessConfig) -> Dict[str, Any]:
        ...
```

---

## 5. 运行目录布局（推荐）

```
runs/
  <scenario>__<timestamp>__<gitsha>/
    manifest.json
    config/
      scenario_config.json
      sim_config.json
      sensors.json
      calibration.json
    logs/
      ros2_bag/
      cyber_record/
      carla_recorder.log
      control.csv
    frames/                      # 可选：仅失败/调试时写
      000001/ ...
    results/
      kpi_summary.json
      kpi_detail.csv
      events.json
      report.md
    videos/
      spectator.mp4
      ego_front.mp4
```

---

## 6. MVP 最小落地顺序（强烈建议）

1) **sim + scenarios**：固定地图 + seed + traffic flow 可复现  
2) **sensors + sync(hard)**：front cam + top lidar 先跑通 FramePacket  
3) **oracle**：ego truth + actors truth + collision/lane events  
4) **control**：fallback PID + safety termination；再接入 ROS2/Cyber 控制通道  
5) **record + eval**：先出 5 个 KPI（完成率、碰撞、闯红灯、最大 jerk、最小 TTC）  
6) **baseline**：同场景回归对比 + 阈值退化报警

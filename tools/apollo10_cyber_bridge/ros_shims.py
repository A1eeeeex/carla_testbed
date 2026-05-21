from __future__ import annotations


class StampShim:
    def __init__(self) -> None:
        self.sec = 0
        self.nanosec = 0


class HeaderShim:
    def __init__(self) -> None:
        self.stamp = StampShim()
        self.frame_id = ""


class Vector3Shim:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0


class QuaternionShim:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0
        self.w = 1.0


class PoseShim:
    def __init__(self) -> None:
        self.position = Vector3Shim()
        self.orientation = QuaternionShim()


class PoseWithCovarianceShim:
    def __init__(self) -> None:
        self.pose = PoseShim()


class TwistShim:
    def __init__(self) -> None:
        self.linear = Vector3Shim()
        self.angular = Vector3Shim()


class TwistWithCovarianceShim:
    def __init__(self) -> None:
        self.twist = TwistShim()


class OdometryShim:
    def __init__(self) -> None:
        self.header = HeaderShim()
        self.child_frame_id = ""
        self.pose = PoseWithCovarianceShim()
        self.twist = TwistWithCovarianceShim()


class StringShim:
    def __init__(self, data: str = "") -> None:
        self.data = str(data)


class Float32MultiArrayShim:
    def __init__(self, data: list[float] | None = None) -> None:
        self.data = list(data or [])


class AckermannDriveShim:
    def __init__(self) -> None:
        self.steering_angle = 0.0
        self.speed = 0.0
        self.acceleration = 0.0


class AckermannDriveStampedShim:
    def __init__(self) -> None:
        self.drive = AckermannDriveShim()


class TwistMsgShim:
    def __init__(self) -> None:
        self.linear = Vector3Shim()
        self.angular = Vector3Shim()


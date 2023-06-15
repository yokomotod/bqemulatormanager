import subprocess
import time


class PortOccupiedError(Exception):
    pass


class EmulatorError(Exception):
    pass


class Emulator:
    def __init__(self, project_name: str, port: int, grpc_port: int, launch_emulator: bool = True, debug_mode: bool = False):
        self.project_name = project_name

        if launch_emulator:
            log_level = "debug" if debug_mode else "info"

            if is_port_in_use(port):
                raise PortOccupiedError(f"port {port} is occupied.")
            if is_port_in_use(grpc_port):
                raise PortOccupiedError(f"port {grpc_port} is occupied.")
            self.proc = subprocess.Popen(
                ["bigquery-emulator", "--project", project_name, "--port", f"{port}", "--grpc-port", f"{grpc_port}", "--log-level", log_level],
            )
            self._wait_for_emulator(port, timeout=10)

    def __del__(self):
        if self.proc is None:
            return

        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait()

    def _wait_for_emulator(self, port: int, timeout: int):
        for _ in range(timeout):
            if is_port_in_use(port):
                return
            time.sleep(1)
        raise EmulatorError("emulator did not start.")


def is_port_in_use(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0

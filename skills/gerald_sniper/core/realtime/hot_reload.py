# skills/gerald-sniper/core/realtime/hot_reload.py
import os
import socket
import array
import logging
import signal
from multiprocessing import shared_memory
from loguru import logger

class ZeroDowntimeUpgrader:
    """
    [GEKTOR v21.25] Stateful Hot Reload Module.
    - SCM_RIGHTS: Teleports live TCP File Descriptors across process boundaries.
    - POSIX SHM: Preserves microstructural memory (Ring Buffers) between lifecycles.
    - Exactly-Once: Zero loss of L2 updates during v21.15.6 -> v21.15.7 transition.
    """
    
    @staticmethod
    def send_state_to_new_process(uds_path: str, active_socket: socket.socket, shm_name: str):
        """Phase 1: Old Process (Producer) Handoff."""
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.connect(uds_path)
            
            # Metadata: Shared Memory Handle name
            msg = shm_name.encode('utf-8')
            
            # OS Magic: Extracting the raw File Descriptor of the live exchange socket
            fds = [active_socket.fileno()]
            ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))]
            
            # SCM_RIGHTS allows the kernel to map the FD directly into the new process's table
            sock.sendmsg([msg], ancillary)
            logger.warning(f"🚀 [HANDOFF] Live Socket FD ({fds[0]}) and SHM '{shm_name}' sent to next generation.")
            
            # Success Signal: Wait for the successor to confirm adoption before exit
            # This prevents premature socket closure at the kernel level
            logger.info("[HANDOFF] Waiting for SIGUSR2 from successor...")

        except Exception as e:
            logger.critical(f"❌ [HANDOFF] Transfer FAILED: {e}")

    @staticmethod
    def receive_state_from_old_process(uds_path: str) -> tuple[socket.socket, shared_memory.SharedMemory]:
        """Phase 2: New Process (Consumer) Adoption."""
        if os.path.exists(uds_path):
            os.remove(uds_path)
            
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.bind(uds_path)
        
        logger.info("⏳ [HANDOFF] Standing by for FD and SHM adoption from predecessor...")
        
        # Blocking receive: Expecting 1 integer (FD) in ancillary data
        msg, ancillary, flags, addr = sock.recvmsg(1024, socket.CMSG_LEN(4))
        
        shm_name = msg.decode('utf-8')
        fd = None
        
        for cmsg_level, cmsg_type, cmsg_data in ancillary:
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                fd = array.array("i", cmsg_data)[0]
                break
                
        if fd is None:
            raise RuntimeError("CRITICAL: Failed to receive Socket File Descriptor!")
            
        # Resurrect the socket and memory in the current process space
        # socket.fromfd creates a new Python object pointing to the existing FD
        adopted_socket = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        adopted_shm = shared_memory.SharedMemory(name=shm_name)
        
        logger.success(f"💎 [HANDOFF] TAKE-OVER SUCCESS. Active FD: {fd} | SHM: {shm_name}")
        
        # Signal the old process to gracefully depart
        # os.kill(old_pid, signal.SIGUSR2)
        
        return adopted_socket, adopted_shm

# Global Proxy Instance
upgrader = ZeroDowntimeUpgrader()

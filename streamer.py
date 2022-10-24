import struct
from concurrent.futures import ThreadPoolExecutor
import time
import sys
import hashlib
# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.buffer = {}
        self.rec_seq_num = 0
        self.send_seq_num = 0
        self.closed = False
        self.ack = False
        self.fin = False
        self.header_length = 24
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def listener(self) -> None:
        while not self.closed:
            if self.buffer:
                min_seq_num_in_buffer = min(self.buffer.keys())
                while min_seq_num_in_buffer < self.rec_seq_num:
                    del self.buffer[min_seq_num_in_buffer]
                    self.send_ack()
                    if self.buffer:
                        min_seq_num_in_buffer = min(self.buffer.keys())
                    else:
                        break
            try:
                data, addr = self.socket.recvfrom()
                if data == b'':
                    continue
                header = struct.unpack('q', data[:8])
                value = header[0]
                msg_hash = data[8:24]
                packet_data = data[:8]
                if len(data) > self.header_length:
                    packet_data = packet_data + data[24:]
                if hashlib.md5(packet_data).digest() != msg_hash:
                    pass
                elif value == -1:
                    self.ack = True
                elif value == -2:
                    self.send_ack()
                    self.fin = True
                elif self.buffer.get(value, None) is None:
                    self.buffer[value] = data[self.header_length:]
            except Exception as e:
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        max_msg_size = 1472 - self.header_length
        msg_size = len(data_bytes)
        messages = []
        before_send_seq_num = self.send_seq_num
        if msg_size <= max_msg_size:
            header = struct.pack('q', self.send_seq_num)
            pre_hash_msg = header+data_bytes
            msg_hash = hashlib.md5(pre_hash_msg).digest()
            header = header + msg_hash
            msg = header + data_bytes
            messages.append(msg)
            self.send_seq_num = self.send_seq_num + 1
        else:
            rest = data_bytes[max_msg_size:]
            msg_data = data_bytes[:max_msg_size]
            header = struct.pack('q', self.send_seq_num)
            pre_hash_msg = header + msg_data
            msg_hash = hashlib.md5(pre_hash_msg).digest()
            header = header + msg_hash
            msg = header + msg_data
            messages.append(msg)
            self.send_seq_num = self.send_seq_num + 1
            while len(rest) > max_msg_size:
                msg_data = rest[:max_msg_size]
                header = struct.pack('q', self.send_seq_num)
                pre_hash_msg = header + msg_data
                msg_hash = hashlib.md5(pre_hash_msg).digest()
                header = header + msg_hash
                msg = header + msg_data
                messages.append(msg)
                self.send_seq_num = self.send_seq_num + 1
                rest = rest[max_msg_size:]
            if len(rest) > 0:
                msg_data = rest
                header = struct.pack('q', self.send_seq_num)
                pre_hash_msg = header + msg_data
                msg_hash = hashlib.md5(pre_hash_msg).digest()
                header = header + msg_hash
                msg = header + msg_data
                messages.append(msg)
                self.send_seq_num = self.send_seq_num + 1
        for msg in messages:
            self.send_msg(msg)

    def send_msg(self, msg_data: bytes) -> None:
        self.socket.sendto(msg_data, (self.dst_ip, self.dst_port))
        init_time = time.time()
        while time.time() - init_time < 0.25:
            time.sleep(0.01)
            if self.ack:
                self.ack = False
                return
        self.send_msg(msg_data)

    def send_ack(self) -> None:
        msg = struct.pack('q', -1)
        msg_hash = hashlib.md5(msg).digest()
        msg = msg + msg_hash
        self.socket.sendto(msg, (self.dst_ip, self.dst_port))

    def send_fin(self) -> None:
        msg = struct.pack('q', -2)
        msg_hash = hashlib.md5(msg).digest()
        msg = msg + msg_hash
        self.socket.sendto(msg, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while not self.buffer:
            time.sleep(0.01)
        while self.buffer.get(self.rec_seq_num, None) is None:
            time.sleep(0.01)
        data_to_return = self.buffer[self.rec_seq_num]
        del self.buffer[self.rec_seq_num]
        self.rec_seq_num = self.rec_seq_num + 1
        self.send_ack()
        while self.buffer.get(self.rec_seq_num, None) is not None:
            data_to_return = data_to_return + self.buffer[self.rec_seq_num]
            del self.buffer[self.rec_seq_num]
            self.send_ack()
            self.rec_seq_num = self.rec_seq_num + 1
        return data_to_return

    def close_helper(self) -> None:
        self.send_fin()
        init_time = time.time()
        while time.time() - init_time < 0.25:
            if self.ack:
                break
            time.sleep(0.01)
        if not self.ack:
            self.close()
        while not self.fin:
            time.sleep(0.01)
        time.sleep(30)
        self.socket.stoprecv()
        self.closed = True

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # maybe wait here for part 5
        self.ack = False
        self.send_fin()
        init_time = time.time()
        while time.time() - init_time < 0.25:
            if self.ack:
                break
            time.sleep(0.01)
        if not self.ack:
            self.close_helper()
        while not self.fin:
            time.sleep(0.01)
        time.sleep(2)
        self.socket.stoprecv()
        self.closed = True



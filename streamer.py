import struct
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
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
        self.expected_rec_seq_num = 1
        self.next_send_seq_num = 1
        self.closed = False
        self.ack = False
        self.fin = False
        self.header_length = 32
        self.send_base = 1
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.executor.submit(self.listener)
        self.timer_on = False
        self.timer_size = 0.25
        self.timer_start = time.time()
        self.messages_sent = {}
        self.rec_ack_packet = self.make_packet(0, 1, 0)
        self.got_fin_ack = False
        self.dummy = 'dummy'
        self.lock = Lock()

    def parse_packet(self, packet: bytes) -> (int, int, int, bytes, bytes):
        self.dummy = 'dummy'
        header = struct.unpack('qii', packet[:16])
        msg_hash = packet[16:32]
        msg_data = packet[32:]
        seq_num = header[0]
        is_ack = header[1]
        is_fin = header[2]
        return seq_num, is_ack, is_fin, msg_hash, msg_data

    def make_packet(self, seq_num: int, is_ack: int, is_fin: int, **kwargs) -> bytes:
        if not is_ack:
            try:
                data = kwargs['data']
                header = struct.pack('qii', seq_num, is_ack, is_fin)
                assert (len(header) == self.header_length - 16)
                msg_hash = hashlib.md5(header + data).digest()
                header = header + msg_hash
                packet = header + data
                return packet
            except Exception as e:
                print('Please pass make_packet data in bytes format if it is not an ack')
                print(e)
        else:
            data = struct.pack('qii', seq_num, is_ack, is_fin)
            assert (len(data) == self.header_length - 16)
            data_hash = hashlib.md5(data).digest()
            packet = data + data_hash
            return packet

    def send_packet(self, packet) -> None:
        with self.lock:
            parsed_packet = self.parse_packet(packet)
            if parsed_packet[1] == 0 and parsed_packet[2] == 0 and packet not in self.messages_sent.values():
                self.messages_sent[self.next_send_seq_num] = packet
                self.next_send_seq_num = self.next_send_seq_num + 1
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))

    def test_packet(self, packet) -> bool:
        to_hash = packet[:16]
        if len(packet) > self.header_length:
            to_hash = to_hash + packet[32:]
        re_hash = hashlib.md5(to_hash).digest()
        if re_hash == packet[16:32]:
            return True
        else:
            return False

    def start_timer(self) -> None:
        self.timer_start = time.time()
        self.timer_on = True
        while self.timer_on:
            if time.time() - self.timer_start > self.timer_size:
                i = self.send_base
                while i < self.next_send_seq_num:
                    packet = self.messages_sent[i]
                    self.send_packet(packet)
                    i = i + 1
                self.run_timer()
                return
        return

    def run_timer(self) -> None:
        self.timer_on = False
        self.executor.submit(self.start_timer)

    def listener(self) -> None:
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()

                if data == b'' or not self.test_packet(data):
                    continue
                parsed_packet = self.parse_packet(data)
                seq_num, is_ack, is_fin, msg_hash, msg_data = parsed_packet
                if is_ack and is_fin:
                    self.got_fin_ack = True
                elif is_fin:
                    self.fin = True
                    fin_ack_packet = self.make_packet(self.expected_rec_seq_num, 1, 1)
                    fin_packet = self.make_packet(self.expected_rec_seq_num, 0, 1, data=b'')
                    self.send_packet(fin_ack_packet)
                    self.send_packet(fin_packet)
                elif is_ack:
                    self.send_base = max(self.send_base, seq_num + 1)
                    if self.send_base >= self.next_send_seq_num:
                        self.timer_on = False
                    else:
                        self.run_timer()
                elif seq_num == self.expected_rec_seq_num:
                    self.buffer[seq_num] = msg_data
                elif seq_num > self.expected_rec_seq_num:
                    self.buffer[seq_num] = msg_data
                    self.send_packet(self.rec_ack_packet)
                else:
                    self.send_packet(self.rec_ack_packet)
            except Exception as e:
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        max_msg_size = 1472 - self.header_length
        msg_size = len(data_bytes)
        if self.send_base == self.next_send_seq_num:
            self.run_timer()
        if msg_size <= max_msg_size:
            seq_num = self.next_send_seq_num
            is_ack = 0
            is_fin = 0
            data = data_bytes
            packet = self.make_packet(seq_num, is_ack, is_fin, data=data)
            self.send_packet(packet)
        else:
            rest = data_bytes[max_msg_size:]
            msg_data = data_bytes[:max_msg_size]
            seq_num = self.next_send_seq_num
            is_ack = 0
            is_fin = 0
            packet = self.make_packet(seq_num, is_ack, is_fin, data=msg_data)
            self.send_packet(packet)
            while len(rest) > max_msg_size:
                msg_data = rest[:max_msg_size]
                seq_num = self.next_send_seq_num
                is_ack = 0
                is_fin = 0
                packet = self.make_packet(seq_num, is_ack, is_fin, data=msg_data)
                self.send_packet(packet)
                rest = rest[max_msg_size:]
            if len(rest) > 0:
                msg_data = rest
                seq_num = self.next_send_seq_num
                is_ack = 0
                is_fin = 0
                packet = self.make_packet(seq_num, is_ack, is_fin, data=msg_data)
                self.send_packet(packet)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while not self.buffer:
            time.sleep(0.01)
        expected_data = self.buffer.get(self.expected_rec_seq_num, None)
        while expected_data is None:
            time.sleep(0.01)
            expected_data = self.buffer.get(self.expected_rec_seq_num, None)
        del self.buffer[self.expected_rec_seq_num]
        self.rec_ack_packet = self.make_packet(self.expected_rec_seq_num, 1, 0)
        self.send_packet(self.rec_ack_packet)
        self.expected_rec_seq_num = self.expected_rec_seq_num + 1
        return expected_data

    def send_fin(self) -> None:
        if self.fin:
            self.send_packet(self.make_packet(self.next_send_seq_num, 1, 1))
            self.send_packet(self.make_packet(self.next_send_seq_num, 0, 1, data=b''))
        else:
            self.send_packet(self.make_packet(self.next_send_seq_num, 0, 1, data=b''))
            init_time = time.time()
            while time.time() - init_time > 0.25:
                if self.got_fin_ack:
                    return
            self.send_fin()

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        while self.send_base < self.next_send_seq_num:
            time.sleep(0.01)
        self.timer_on = False
        self.send_fin()
        while not self.fin:
            time.sleep(0.01)
        self.send_packet(self.make_packet(self.next_send_seq_num, 1, 1))
        time.sleep(2)
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.socket.stoprecv()
        self.closed = True

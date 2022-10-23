import struct
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

    def send(self, data_bytes: bytes) -> None:
        header_length = 8
        max_msg_size = 1472 - header_length
        msg_size = len(data_bytes)
        messages = []
        if msg_size <= max_msg_size:
            header = struct.pack('q', self.send_seq_num)
            msg = header+data_bytes
            messages.append(msg)
            self.send_seq_num = self.send_seq_num + 1
        else:
            rest = data_bytes[max_msg_size:]
            msg_data = data_bytes[:max_msg_size]
            header = struct.pack('q', self.send_seq_num)
            msg = header+msg_data
            messages.append(msg)
            self.send_seq_num = self.send_seq_num + 1
            while len(rest) > max_msg_size:
                msg_data = rest[:max_msg_size]
                header = struct.pack('q', self.send_seq_num)
                msg = header+msg_data
                messages.append(msg)
                self.send_seq_num = self.send_seq_num + 1
                rest = rest[max_msg_size:]
            if len(rest) > 0:
                header = struct.pack('q', self.send_seq_num)
                msg = header+rest
                messages.append(msg)
                self.send_seq_num = self.send_seq_num + 1
        for msg in messages:
            self.socket.sendto(msg, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        all_data = b''
        data_from_buffer = self.buffer.get(self.rec_seq_num, None)
        while data_from_buffer is not None:
            del self.buffer[self.rec_seq_num]
            self.rec_seq_num = self.rec_seq_num + 1
            all_data = all_data+data_from_buffer
            data_from_buffer = self.buffer.get(self.rec_seq_num, None)
        if all_data != b'':
            return all_data
        data, addr = self.socket.recvfrom()
        header = data[:8]
        unpacked = struct.unpack('q', header)
        seq_num = unpacked[0]
        if seq_num == self.rec_seq_num:
            self.rec_seq_num = self.rec_seq_num + 1
            return data[8:]
        else:
            if self.buffer.get(seq_num, None) is None:
                self.buffer[seq_num] = data[8:]
        return b' '

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass

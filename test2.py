import socket
import time
import random
import heapq
import threading
import time


UDP_PORT_NUM = 2333
TS_PACKET_LENGTH = 188
# The length of one package is TS packet size 188, RTP header 12,
# UDP header 8, IP header 20, ETHERNET header 14 and ETHERNET crc 4
# The limit lies on that one message should be no larger than 1500
# bytes in order to be transmitted efficiently through ethernet.
PACKETS_PER_MESSAGE = (1500 - 12 - 8 - 20 - 14 - 4) / 188

MTU = 1500
RQST_LISTEN_ADDRESS = ""
RQST_LISTEN_PORT = 9876
RQST_LENGTH = 3
SSRC = 234567892
RECV_BUF_SIZE = 4096

MAX_HEAP_SIZE = 60000
SLOW_DOWN = False

NOT_FINISHED = True
WAIT_INTERVAL =60

# film_name = "The Death of Stalin"

hostname = socket.gethostname()
local_ip = socket.gethostbyname_ex(hostname)[2][0]
local_port = 6789

buf_heap = []

# assume client can get the first num of seq
first_seq = 0
init_order4 = (0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15)
MAX_FIRST_SEQ = 160




def request_film(film_name):
	request_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	request_socket.connect((RQST_LISTEN_ADDRESS, RQST_LISTEN_PORT))
	recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	try:
		recv_socket.bind((local_ip, local_port))
	except socket.error:
		print("Fail to appoint ip & port")
		return
	request_socket.send(film_name)
	print "sent!"
	request_socket.send(local_ip)
	print "sent!"
	request_socket.send("%d" % local_port)
	print "sent!"
	# request_socket.sendall((film_name, local_ip, local_port))
	# file_recv(recv_socket)
	first_seq = request_socket.recv(1024)

	recv_thread = threading.Thread(target = file_recv, args = (recv_socket,))
	recv_thread.start()

	pause_thread = threading.Thread(target = pause_server, args = (request_socket,))
	pause_thread.start()

	sent_flag = request_socket.recv(1024)
	if sent_flag == "File sent." :
		time.sleep(WAIT_INTERVAL)
		NOT_FINISHED = False

	request_socket.close()
	return



def file_recv(recv_socket):
	while NOT_FINISHED:
		recv_data, server_addr = recv_socket.recvfrom(RECV_BUF_SIZE)
		cur_seq = unwrap_slices(recv_data)
		# file_sort(recv_data, cur_seq)
	return



def unwrap_slices(recvmsg):
	seq_num = (ord(recvmsg[2])<<8) + ord(recvmsg[3])
	ts_packets = recvmsg[12:len(recvmsg)]
	ts_packet = []
	for i in range(0, len(ts_packets) / TS_PACKET_LENGTH):
		ts_packet.append(ts_packets[TS_PACKET_LENGTH*i : min((i + 1) * TS_PACKET_LENGTH, len(ts_packets))])
		file_sort(ts_packet[i], cal_order(i, seq_num))
	return seq_num


def cal_order(serial_num, seq_num):
	serial_num1 = PACKETS_PER_MESSAGE * (seq_num - first_seq) + serial_num
	serial_num2 = serial_num1 / 16 + init_order4[serial_num1 % 16]
	return serial_num2


def file_sort(ts_packet, packet_seq):
	# heapq is a module for python but it doesnot provide any approach to limit heap size
	# a naive additional approach to restrict heap size
	if len(buf_heap) <= MAX_HEAP_SIZE:
		heapq.heappush(buf_heap, (packet_seq, ts_packet))
	elif packet_seq < heapq.nsmallest(1, buf_heap)[0][0]:
		pass
	else:
		pause_send()
		'''
		if packet_seq > heapq.nlargest(1, buf_heap)[0][0]:
			heapq.heappop(buf_heap)
			heapq.heappush(buf_heap, (packet_seq, ts_packet))
		'''
	# player: cur_packets = heapq.heappop(buf_heap)
	# player: cur_6packet = cur_packets[1s]


def pause_send():
	SLOW_DOWN = True

def pause_server(tcp_socket):
	while NOT_FINISHED:
		if SLOW_DOWN == True:
			tcp_socket.sendall("Slow down.")
			SLOW_DOWN = False



def test_write(file_slice, file_path):
	filmfile = open(file_path, 'a')
	filmfile.write(file_slice)
	filmfile.close()


def write_ts(file_path):
	while True:
		if len(buf_heap) <= MAX_FIRST_SEQ or heapq.nsmallest(1, buf_heap)[0][0] >= MAX_FIRST_SEQ:
			time.sleep(5)
		else:
			break
	while True:
		if NOT_FINISHED == False and len(buf_heap) == 0:
			break
		test_write(heapq.heappop(buf_heap)[1], file_path)



if __name__ == "__main__":
	film1 = "The Death of Stalin"
	test_path = "./film_test.ts"
	download_thread = threading.Thread(target = request_film, args = (film1,))
	write_thread = threading.Thread(target = write_ts, args = (test_path,))
	download_thread.start()
	write_thread.start()

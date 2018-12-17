import socket
import time
import bitstring
import random
import thread

servefile = {}
UDP_IP_ADDRESS = ""
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
# the SSRC is randomly chosen, don't know about its impact, 
# seems like it would be fine as long as this code is unique.
SSRC = 234567892
SLOW_DOWN = False
FINISHED = False


# add a file to the files database
def addFile(file_path, name):
	file = open("file_path", "rb")
	servefile[name] = file

# reorder the slices so that loss of one packet wouldn't make too much damage
def reorder_slices(slices, unit_size):
	new_slices = []
	start = 0
	for i in range(0, len(slices) - unit_size * unit_size, unit_size * unit_size):
		start = i
		# print(start)
		for j in range(unit_size):
			for k in range(unit_size):
				# print(start + k * unit_size + j)
				new_slices.append(slices[start + k * unit_size + j])
	for i in range(start + unit_size * unit_size, len(slices)):
		new_slices.append(slices[i])
	print("Reordered slices length: ", len(new_slices))
	return new_slices


# warp the slice with an RTP header
def wrap_slices(content, sequence_counter):
	cur_time = time.time()
	cur_time *= 1000
	cur_time = int(cur_time)
	print("time: ", time)
	header = bitstring.BitArray('0b1000000000100010')
	seq = bitstring.BitArray(uint=sequence_counter, length = 16)
	header.append(seq)
	t = bitstring.BitArray(uint=cur_time, length = 32)
	header.append(t)
	ssrc = bitstring.BitArray(uint=SSRC, length = 32)
	header.append(ssrc)
	header_ints = []
	for i in range(3):
		header_ints.append(header[32*i:32*(i+1)].int())
	res = header_ints[0].to_bytes(4, 'big')
	res += (header_ints[1].to_bytes(4, 'big'))
	res += (header_ints[2].to_bytes(4, 'big'))
	res += content
	return content





# slice the TS file into packets
def slice_file(file):
	slices = []
	while True:
		chunk = file.read(TS_PACKET_LENGTH)
		if not chunk:
			break
		assert(len(chunk) == TS_PACKET_LENGTH)
		slices.append(chunk)
	reordered_slices = reorder_slices(slices, 4)
	return reordered_slices



# performs the task as a UDP server
def file_sender(filename, client_ip, client_port, sequence_counter):
	file = servefile[filename]
	send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	file_list = slice_file(file)
	cur_send = b''
	pac_cnter = 0
	for line in file_list:
		if SLOW_DOWN == True:
			time.sleep(5)
			SLOW_DOWN = False
		pac_cnter += 1
		if pac_cnter % PACKETS_PER_MESSAGE == 0:
			wrap_slices(cur_send, sequence_counter)
			send_socket.sendto(cur_send, (client_ip, client_port))
		else:
			cur_send += line
		sequence_counter += 1
	send_socket.sendto(cur_send, (client_ip, client_port))
	send_socket.close()
	FINISHED = True




# clean up the file directories
def clean_up():
	for file in servefile:
		file.close()


# Performs the task as a listener of requests, recursively listens
# for client requests, collect its meta data, and sent the UDP
# server to transmit the video.
def request_listener():
	rqst_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	rqst_socket.bind((RQST_LISTEN_ADDRESS, RQST_LISTEN_PORT))
	rqst_socket.listen()
	while True:
		print("Waiting for connection......")
		conn, addr = rqst_socket.accept()
		with conn:
			data = []
			print("Client from IP address ", addr, " has submitted a request!")
			while True:
				data.append(conn.recv(1024))
			assert(len(data) == RQST_LENGTH)
			requested_filename = data[0]
			client_ip = data[1]
			client_port = data[2]
			assert(client_ip == addr)
			print("requested_filename: ", requested_filename)
			print("client_ip", client_ip)
			print("client_port", client_port)
			sequence_counter = random.randrange(65536)
			conn.send(sequence_counter)
			thread.start_new_thread(file_sender, (requested_filename, client_ip, client_port, sequence_counter))
			while FINISHED == False:
				str = conn.recv(1024)
				if str == "Slow down.":
					SLOW_DOWN = True
				elif str == "Finished.":
					FINISHED = True

			conn.send("File sent.")
			print("File sent!")
			FINISHED = False
	rqst_socket.close()
	return 

def main(args):
	addFile("./the_death_of_stalin.ts", "The Death of Stalin")


	request_listener()
	clean_up()

if __name__ == "__main__":
	main()

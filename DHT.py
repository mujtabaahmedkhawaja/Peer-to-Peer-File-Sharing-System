import socket 
import threading
import os
import time
import hashlib
from json import dumps, loads

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.addr = (self.host, self.port)
		self.successor = self.addr
		self.predecessor = self.addr
		threading.Thread(target = self.pinging).start()
		# additional state variables

	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

	def pinging(self):
		while self.stop == False:
			time.sleep(0.5)
			# For predecessor for successor
			predecessorsocket = socket.socket()
			predecessorsocket.connect(self.successor)
			to_send = {
				"message": "Get Predecessor"
			}
			predecessorsocket.send(dumps(to_send).encode('utf-8'))
			predecessor = predecessorsocket.recv(2048).decode('utf-8')
			data_received = loads(predecessor)
			addr_recv = (data_received["host"], data_received["port"])

			if addr_recv != (self.host, self.port):
				try:
					self.predecessor = addr_recv
					update_successorsocket = socket.socket() # send a message to predecessor so it can update its successor
					update_successorsocket.connect(self.predecessor)
					to_send = {
						"message": "Update Successor",
						"host": self.host,
						"port": self.port
					}
					update_successorsocket.send(dumps(to_send).encode('utf-8'))
					update_successorsocket.close()
					update_predecessorsocket = socket.socket() #send a message to successor so it can update its predecessor
					update_predecessorsocket.connect(self.successor)
					to_send = {
						"message": "Update Predecessor",
						"host": self.host,
						"port": self.port
					}
					update_predecessorsocket.send(dumps(to_send).encode('utf-8')) 
					update_predecessorsocket.close()
				except:
					update_predecessorsocket.close()
					update_successorsocket.close()

	def lookup(self, addr):
		self_key = self.key
		successor_key = self.hasher(self.successor[0]+str(self.successor[1]))
		to_insert = self.hasher(addr[0]+str(addr[1]))

		if (((to_insert < successor_key) and (to_insert < self_key)) and (self_key > successor_key)) or (((to_insert > successor_key) and (to_insert > self_key)) and (self_key > successor_key)) or ((to_insert > self_key) and (to_insert < successor_key)):
			return self.successor
		else:
			temp = socket.socket()
			temp.connect(self.successor)
			to_send = {
				"message": "joining lookup forwarded",
				"host": addr[0],
				"port": addr[1]
			}
			temp.send(dumps(to_send).encode("utf-8"))
			data_received = temp.recv(2048).decode("utf-8")
			data_extracted = loads(data_received)
			temp.close()
			return (data_extracted["host"], data_extracted["port"])

	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		temp = client.recv(2048).decode("utf-8")
		data = loads(temp)
		message = data["message"]
		if(message == "joining request"):
			address = (data["host"], data["port"])
			if(self.successor == (self.host, self.port)):
				self.successor = address
				self.predecessor = address
				to_send = {
					"message": "join corner case",
				}
				client.send(dumps(to_send).encode('utf-8'))
			else:
				return_addr = self.lookup(address)
				to_send = {
					"message": "position found",
					"host": return_addr[0],
					"port": return_addr[1]
				}
				client.send(dumps(to_send).encode("utf-8"))
		elif(message == "joining lookup forwarded"):
			address = (data["host"], data["port"]) 
			if(self.successor == (self.host, self.port)):
				self.successor = address
				self.predecessor = address
				to_send = {
					"message": "join corner case",
				}
				client.send(dumps(to_send).encode('utf-8'))
			else:
				return_addr = self.lookup(address)
				to_send = {
					"message": "forwarded addr",
					"host": return_addr[0],
					"port": return_addr[1]
				}
				client.send(dumps(to_send).encode("utf-8"))
		elif(message == "Get Predecessor"):
			to_send = {
				"host": self.predecessor[0],
				"port": self.predecessor[1]
			}
			client.send(dumps(to_send).encode("utf-8"))
		elif(message == "Update Predecessor"):
			self.predecessor = (data["host"], data["port"])
		elif(message == "Update Successor"):
			self.successor = (data["host"], data["port"])
		client.close()

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		if joiningAddr != "":
			temp = socket.socket()
			temp.connect(joiningAddr)
			to_send = {
				"message": "joining request",
				"host": self.host,
				"port": self.port
			}
			temp.send(dumps(to_send).encode("utf-8"))
			data_received = temp.recv(2048).decode('utf-8')	
			data_extracted = loads(data_received)
			if(data_extracted["message"] == "join corner case"):
				self.successor = joiningAddr
				self.predecessor = joiningAddr
			else:
				self.successor = (data_extracted["host"], data_extracted["port"])
			temp.close()

	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		

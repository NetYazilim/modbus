package modbus

import (
	"fmt"
	"time"
	"net"
	"strings"
	"sync"
)

type ServerConfiguration struct {
	URL		string
	Speed		uint
	DataBits	uint
	Parity		uint
	StopBits	uint
	Timeout		time.Duration
	MaxClients	uint
	AcceptedUnitIds	[]uint8
}

type RequestHandler interface {
	HandleCoils		(unitId uint8, addr uint16, quantity uint16,
				 isWrite bool, args []bool) (
				 res []bool, err error)

	HandleDiscreteInputs	(unitId uint8, addr uint16, quantity uint16) (
				 res []bool, err error)

	HandleHoldingRegisters	(unitId uint8, addr uint16, quantity uint16,
				 isWrite bool, args []uint16) (
				 res []uint16, err error)

	HandleInputRegisters	(unitId uint8, addr uint16, quantity uint16) (
				 res []uint16, err error)
}

type ModbusServer struct {
	conf		ServerConfiguration
	logger		*logger
	lock		sync.Mutex
	started		bool
	handler		RequestHandler
	tcpListener	net.Listener
	tcpClients	[]net.Conn
	transportType	transportType
}

func NewServer(conf *ServerConfiguration, reqHandler RequestHandler) (ms *ModbusServer, err error) {
	ms = &ModbusServer{
		conf:		*conf,
		handler:	reqHandler,
		logger:		newLogger("modbus-server"),
	}

	switch {
	case strings.HasPrefix(ms.conf.URL, "rtu://"):
		ms.conf.URL	= strings.TrimPrefix(ms.conf.URL, "rtu://")

		// set useful defaults
		if ms.conf.Speed == 0 {
			ms.conf.Speed	= 9600
		}

		if ms.conf.DataBits == 0 {
			ms.conf.DataBits = 8
		}

		if ms.conf.StopBits == 0 {
			if ms.conf.Parity == PARITY_NONE {
				ms.conf.StopBits = 2
			} else {
				ms.conf.StopBits = 1
			}
		}

		if ms.conf.Timeout == 0 {
			ms.conf.Timeout = 30 * time.Second
		}

		// ensure we have at least one configured unit ID to tune into
		if len(ms.conf.AcceptedUnitIds) == 0 {
			ms.logger.Errorf("at least 1 unit id must be configured " +
					 "with the RTU transport")
			err = ErrConfigurationError
			return
		}

		ms.transportType	= RTU_TRANSPORT

	case strings.HasPrefix(ms.conf.URL, "tcp://"):
		ms.conf.URL	= strings.TrimPrefix(ms.conf.URL, "tcp://")

		if ms.conf.Timeout == 0 {
			ms.conf.Timeout = 120 * time.Second
		}

		if ms.conf.MaxClients == 0 {
			ms.conf.MaxClients = 10
		}

		ms.transportType	= TCP_TRANSPORT

	default:
		err	= ErrConfigurationError
		return
	}

	ms.logger	= newLogger(fmt.Sprintf("modbus-server(%s)", ms.conf.URL))

	return
}

func (ms *ModbusServer) Start() (err error) {
	var spw		*serialPortWrapper

	ms.lock.Lock()
	defer ms.lock.Unlock()

	if ms.started {
		return
	}

	switch ms.transportType {
	case RTU_TRANSPORT:
		// create a serial port wrapper object
		spw = newSerialPortWrapper(&serialPortConfig{
			Device:		ms.conf.URL,
			Speed:		ms.conf.Speed,
			DataBits:	ms.conf.DataBits,
			Parity:		ms.conf.Parity,
			StopBits:	ms.conf.StopBits,
		})

		// open the serial device
		err = spw.Open()
		if err != nil {
			return
		}

		// discard potentially stale serial data
		discard(spw)

		// create the RTU transport and pass it to the handler goroutine
		go ms.handleTransport(
			newRTUTransport(
				spw, ms.conf.URL, ms.conf.Speed, ms.conf.Timeout))

	case TCP_TRANSPORT:
		// bind to a TCP socket
		ms.tcpListener, err	= net.Listen("tcp", ms.conf.URL)
		if err != nil {
			return
		}

		// accept client connections in a goroutine
		go ms.acceptTCPClients()

	default:
		err = ErrConfigurationError
		return
	}

	ms.started = true

	return
}

func (ms *ModbusServer) Stop() (err error) {
	ms.lock.Lock()
	defer ms.lock.Unlock()

	ms.started = false

	if ms.transportType == TCP_TRANSPORT {
		// close the server socket if we're listening over TCP
		ms.tcpListener.Close()

		// close all active TCP clients
		for _, sock := range ms.tcpClients{
			sock.Close()
		}
	}

	return
}

func (ms *ModbusServer) acceptTCPClients() {
	var sock	net.Conn
	var err		error
	var accepted	bool

	for {
		sock, err = ms.tcpListener.Accept()
		if err != nil {
			// if the server has just been stopped, return here
			if !ms.started {
				return
			}
			ms.logger.Warningf("failed to accept client connection: %v", err)
			continue
		}

		ms.lock.Lock()
		// apply a connection limit
		if uint(len(ms.tcpClients)) < ms.conf.MaxClients {
			accepted	= true
			// add the new client connection to the pool
			ms.tcpClients	= append(ms.tcpClients, sock)
		} else {
			accepted	= false
		}
		ms.lock.Unlock()

		if accepted {
			// spin a client handler goroutine to serve the new client
			go ms.handleTCPClient(sock)
		} else {
			ms.logger.Warningf("max. number of concurrent connections " +
					   "reached, rejecting %v", sock.RemoteAddr())
			// discard the connection
			sock.Close()
		}
	}

	return
}

func (ms *ModbusServer) handleTCPClient(sock net.Conn) {
	var tt	*tcpTransport

	// create a new transport
	tt = newTCPTransport(sock, ms.conf.Timeout)

	ms.handleTransport(tt)

	// once done, remove our connection from the list of active client conns
	ms.lock.Lock()
	for i := range ms.tcpClients {
		if ms.tcpClients[i] == sock {
			ms.tcpClients[i] = ms.tcpClients[len(ms.tcpClients)-1]
			ms.tcpClients	 = ms.tcpClients[:len(ms.tcpClients)-1]
			break
		}
	}
	ms.lock.Unlock()

	// close the connection
	sock.Close()

	return
}

func (ms *ModbusServer) handleTransport(t transport) {
	var req		*pdu
	var res		*pdu
	var err		error
	var found	bool
	var addr	uint16
	var quantity	uint16

	for {
		req, err = t.ReadRequest()
		if err != nil {
			// on RTU links, skip the frame. On TCP links, return to close the
			// connection.
			if ms.transportType == RTU_TRANSPORT {
				continue
			} else {
				return
			}
		}

		// only accept unit IDs of interest on shared RTU links.
		// on TCP links, the endpoint is clearly identified by its IP address and
		// port, so passing all requests regardless of their unit ID to the handler
		// is appropriate.
		if ms.transportType == RTU_TRANSPORT {
			found = false

			// loop through the accepted unit ID list
			for _, uid := range ms.conf.AcceptedUnitIds {
				if uid == req.unitId {
					found = true
					break
				}
			}

			// if we found no match, stay silent as this request wasn't for us
			if !found {
				continue
			}
		}

		switch req.functionCode {
		case FC_READ_COILS, FC_READ_DISCRETE_INPUTS:
			var coils	[]bool
			var resCount	int

			if len(req.payload) != 4 {
				err = ErrProtocolError
				break
			}

			// decode address and quantity fields
			addr		= bytesToUint16(BIG_ENDIAN, req.payload[0:2])
			quantity	= bytesToUint16(BIG_ENDIAN, req.payload[2:4])

			// ensure the reply never exceeds the maximum PDU length and we
			// never read past 0xffff
			if quantity > 2000 || quantity == 0 {
				err	= ErrProtocolError
				break
			}
			if uint32(addr) + uint32(quantity) - 1 > 0xffff {
				err	= ErrIllegalDataAddress
				break
			}

			// invoke the appropriate handler
			if req.functionCode == FC_READ_COILS {
				coils, err	= ms.handler.HandleCoils(
					req.unitId,
					addr, quantity,
					false, nil)
			} else {
				coils, err	= ms.handler.HandleDiscreteInputs(
					req.unitId, addr, quantity)
			}
			resCount	= len(coils)

			// make sure the handler returned the expected number of items
			if err == nil && resCount != int(quantity) {
				ms.logger.Errorf("handler returned %v coils, " +
					         "expected %v", resCount, quantity)
				err = ErrServerDeviceFailure
				break
			}

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
				payload:	[]byte{0},
			}

			// byte count (1 byte for 8 coils)
			res.payload[0]	= uint8(resCount / 8)
			if resCount % 8 != 0 {
				res.payload[0]++
			}

			// coil values
			res.payload	= append(res.payload, encodeBools(coils)...)

		case FC_WRITE_SINGLE_COIL:
			if len(req.payload) != 4 {
				err = ErrProtocolError
				break
			}

			// decode the address field
			addr	= bytesToUint16(BIG_ENDIAN, req.payload[0:2])

			// validate the value field (should be either 0xff00 or 0x0000)
			if ((req.payload[2] != 0xff && req.payload[2] != 0x00) ||
			    req.payload[3] != 0x00) {
				err = ErrProtocolError
				break
			}

			// invoke the coil handler
			_, err	= ms.handler.HandleCoils(
				req.unitId,
				addr, 1,	// quantity is 1
				true,		// this is a write request
				[]bool{(req.payload[2] == 0xff)})

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
			}

			// echo the address and value in the response
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, addr)...)
			res.payload	= append(res.payload,
						 req.payload[2], req.payload[3])

		case FC_WRITE_MULTIPLE_COILS:
			var expectedLen	int

			if len(req.payload) < 6 {
				err = ErrProtocolError
				break
			}

			// decode address and quantity fields
			addr		= bytesToUint16(BIG_ENDIAN, req.payload[0:2])
			quantity	= bytesToUint16(BIG_ENDIAN, req.payload[2:4])

			// ensure the reply never exceeds the maximum PDU length and we
			// never read past 0xffff
			if quantity > 0x7b0 || quantity == 0 {
				err	= ErrProtocolError
				break
			}
			if uint32(addr) + uint32(quantity) - 1 > 0xffff {
				err	= ErrIllegalDataAddress
				break
			}

			// validate the byte count field (1 byte for 8 coils)
			expectedLen	= int(quantity) / 8
			if quantity % 8 != 0 {
				expectedLen++
			}

			if req.payload[4] != uint8(expectedLen) {
				err	= ErrProtocolError
				break
			}

			// make sure we have enough bytes
			if len(req.payload) - 5 != expectedLen {
				err	= ErrProtocolError
				break
			}

			// invoke the coil handler
			_, err		= ms.handler.HandleCoils(
				req.unitId,
				addr, quantity,
				true,		// this is a write request
				decodeBools(quantity, req.payload[5:]))

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
			}

			// echo the address and quantity in the response
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, addr)...)
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, quantity)...)

		case FC_READ_HOLDING_REGISTERS, FC_READ_INPUT_REGISTERS:
			var regs	[]uint16
			var resCount	int

			if len(req.payload) != 4 {
				err = ErrProtocolError
				break
			}

			// decode address and quantity fields
			addr		= bytesToUint16(BIG_ENDIAN, req.payload[0:2])
			quantity	= bytesToUint16(BIG_ENDIAN, req.payload[2:4])

			// ensure the reply never exceeds the maximum PDU length and we
			// never read past 0xffff
			if quantity > 0x007d || quantity == 0 {
				err	= ErrProtocolError
				break
			}
			if uint32(addr) + uint32(quantity) - 1 > 0xffff {
				err	= ErrIllegalDataAddress
				break
			}

			// invoke the appropriate handler
			if req.functionCode == FC_READ_HOLDING_REGISTERS {
				regs, err	= ms.handler.HandleHoldingRegisters(
					req.unitId,
					addr, quantity,
					false, nil)
			} else {
				regs, err	= ms.handler.HandleInputRegisters(
					req.unitId, addr, quantity)
			}
			resCount	= len(regs)

			// make sure the handler returned the expected number of items
			if err == nil && resCount != int(quantity) {
				ms.logger.Errorf("handler returned %v 16-bit values, " +
					         "expected %v", resCount, quantity)
				err = ErrServerDeviceFailure
				break
			}

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
				payload:	[]byte{0},
			}

			// byte count (2 byte per register)
			res.payload[0]	= uint8(resCount * 2)

			// register values
			res.payload	= append(res.payload,
						 uint16sToBytes(BIG_ENDIAN, regs)...)

		case FC_WRITE_SINGLE_REGISTER:
			var value	uint16

			if len(req.payload) != 4 {
				err = ErrProtocolError
				break
			}

			// decode address and value fields
			addr	= bytesToUint16(BIG_ENDIAN, req.payload[0:2])
			value	= bytesToUint16(BIG_ENDIAN, req.payload[2:4])

			// invoke the handler
			_, err	= ms.handler.HandleHoldingRegisters(
				req.unitId,
				addr, 1,	// quantity is 1
				true,		// this is a write request
				[]uint16{value})

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
			}

			// echo the address and value in the response
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, addr)...)
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, value)...)

		case FC_WRITE_MULTIPLE_REGISTERS:
			var expectedLen	int

			if len(req.payload) < 6 {
				err = ErrProtocolError
				break
			}

			// decode address and quantity fields
			addr		= bytesToUint16(BIG_ENDIAN, req.payload[0:2])
			quantity	= bytesToUint16(BIG_ENDIAN, req.payload[2:4])

			// ensure the reply never exceeds the maximum PDU length and we
			// never read past 0xffff
			if quantity > 0x007b || quantity == 0 {
				err	= ErrProtocolError
				break
			}
			if uint32(addr) + uint32(quantity) - 1 > 0xffff {
				err	= ErrIllegalDataAddress
				break
			}

			// validate the byte count field (2 bytes per register)
			expectedLen	= int(quantity) * 2

			if req.payload[4] != uint8(expectedLen) {
				err	= ErrProtocolError
				break
			}

			// make sure we have enough bytes
			if len(req.payload) - 5 != expectedLen {
				err	= ErrProtocolError
				break
			}

			// invoke the holding register handler
			_, err		= ms.handler.HandleHoldingRegisters(
				req.unitId,
				addr, quantity,
				true,		// this is a write request
				bytesToUint16s(BIG_ENDIAN, req.payload[5:]))

			if err != nil {
				break
			}

			// assemble a response PDU
			res = &pdu{
				unitId:		req.unitId,
				functionCode:	req.functionCode,
			}

			// echo the address and quantity in the response
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, addr)...)
			res.payload	= append(res.payload,
						 uint16ToBytes(BIG_ENDIAN, quantity)...)

		default:
			res = &pdu{
				// reply with the request target unit ID
				unitId:		req.unitId,
				// set the error bit
				functionCode:	(0x80 | req.functionCode),
				// set the exception code to illegal function to indicate that
				// the server does not know how to handle this function code.
				payload:	[]byte{EX_ILLEGAL_FUNCTION},
			}
		}

		// if there was no error processing the request but the response is nil
		// (which should never happen), emit a server failure exception code
		// and log an error
		if err == nil && res == nil {
			err = ErrServerDeviceFailure
			ms.logger.Errorf("internal server error (req: %v, res: %v, err: %v)",
					 req, res, err)
		}

		// map go errors to modbus errors, unless the error is a protocol error,
		// in which case close the transport and return.
		if err != nil {
			if err == ErrProtocolError {
				ms.logger.Warningf("protocol error, closing link")
				t.Close()
				return
			} else {
				res = &pdu{
					unitId:		req.unitId,
					functionCode:	(0x80 | req.functionCode),
					payload:	[]byte{mapErrorToExceptionCode(err)},
				}
			}
		}

		// write the response to the transport
		err	= t.WriteResponse(res)
		if err != nil {
			ms.logger.Warningf("failed to write response: %v", err)
		}

		// avoid holding onto stale data
		req	= nil
		res	= nil
	}

	return
}

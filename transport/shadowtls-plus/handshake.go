package shadowtlsplus

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// TLS constants
const (
	RecordTypeHandshake     = 0x16
	TLSVersion10            = 0x0301
	TLSVersion12            = 0x0303
	HandshakeTypeClientHello = 0x01
	HandshakeTypeServerHello = 0x02

	ExtensionServerName       = 0x0000
	ExtensionSupportedGroups  = 0x000A
	ExtensionKeyShare         = 0x0033
	ExtensionSupportedVersion = 0x002B
	ExtensionPadding          = 0x0015

	CipherSuiteAES256GCM = 0x1302
	KeyShareGroupX25519  = 0x001D

	RandomSize         = 32
	SessionIDSize      = 32
	MaxClientHelloSize = 16384
	MinPaddingSize     = AuthDataSize + 32
)

// Handshake errors
var (
	ErrInvalidPassword      = errors.New("handshake: password cannot be empty")
	ErrInvalidRecordType    = errors.New("handshake: invalid TLS record type")
	ErrInvalidHandshakeType = errors.New("handshake: invalid handshake type")
	ErrInvalidKeyShare      = errors.New("handshake: invalid key share data")
	ErrAuthenticationFailed = errors.New("handshake: authentication failed")
	ErrMessageTooShort      = errors.New("handshake: message too short")
	ErrMessageTooLong       = errors.New("handshake: message too long")
	ErrKeyDerivationFailed  = errors.New("handshake: key derivation failed")
)

// ClientHelloInfo contains parsed ClientHello information
type ClientHelloInfo struct {
	Version       uint16
	Random        [RandomSize]byte
	SessionID     []byte
	CipherSuite   uint16
	ServerName    string
	KeyShareGroup uint16
	KeyShareData  []byte
	Padding       []byte
	Raw           []byte
}

// ServerHelloInfo contains parsed ServerHello information
type ServerHelloInfo struct {
	Version       uint16
	Random        [RandomSize]byte
	SessionID     []byte
	CipherSuite   uint16
	KeyShareGroup uint16
	KeyShareData  []byte
	Padding       []byte
}

// HandshakeResult contains the result of a successful handshake
type HandshakeResult struct {
	Keys             *SessionKeys
	ClientInfo       *ClientHelloInfo
	ServerInfo       *ServerHelloInfo
	ClientPrivateKey *ecdh.PrivateKey
	CompletedAt      time.Time
}

// HandshakeConfig contains configuration for handshake
type HandshakeConfig struct {
	Password     string
	SNI          string
	FallbackAddr string
	Timeout      time.Duration
}

// Validate validates the handshake configuration
func (c *HandshakeConfig) Validate() error {
	if c.Password == "" {
		return ErrInvalidPassword
	}
	return nil
}

// ClientHandshaker performs handshake on client side
type ClientHandshaker struct {
	config *HandshakeConfig
}

// NewClientHandshaker creates a new client handshaker
func NewClientHandshaker(config *HandshakeConfig) (*ClientHandshaker, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &ClientHandshaker{config: config}, nil
}

// Handshake performs the client-side handshake
func (h *ClientHandshaker) Handshake(conn net.Conn) (*HandshakeResult, error) {
	if h.config.Timeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(h.config.Timeout)); err != nil {
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}
		defer func() { _ = conn.SetDeadline(time.Time{}) }()
	}

	keyPair, err := GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	authData, err := GenerateAuthData(h.config.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to generate auth data: %w", err)
	}

	clientHello := NewClientHelloBuilder(h.config.SNI, keyPair.PublicKeyBytes(), authData)
	clientHelloBytes := clientHello.Build()

	if _, err := conn.Write(clientHelloBytes); err != nil {
		return nil, fmt.Errorf("failed to send ClientHello: %w", err)
	}

	serverHelloBytes, err := readTLSRecord(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read ServerHello: %w", err)
	}

	serverInfo, err := ParseServerHello(serverHelloBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ServerHello: %w", err)
	}

	if serverInfo.KeyShareGroup != KeyShareGroupX25519 {
		return nil, ErrInvalidKeyShare
	}
	if len(serverInfo.KeyShareData) != 32 {
		return nil, ErrInvalidKeyShare
	}

	serverPubKey, err := PublicKeyFromBytes(serverInfo.KeyShareData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse server public key: %w", err)
	}

	sharedSecret, err := ComputeSharedSecret(keyPair.Private, serverPubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to compute shared secret: %w", err)
	}

	keys, err := DeriveSessionKeys(sharedSecret)
	if err != nil {
		return nil, ErrKeyDerivationFailed
	}

	return &HandshakeResult{
		Keys:             keys,
		ServerInfo:       serverInfo,
		ClientPrivateKey: keyPair.Private,
		CompletedAt:      time.Now(),
	}, nil
}

func readTLSRecord(conn net.Conn) ([]byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	recordLen := int(header[3])<<8 | int(header[4])
	if recordLen > MaxClientHelloSize {
		return nil, ErrMessageTooLong
	}

	record := make([]byte, 5+recordLen)
	copy(record, header)
	if _, err := io.ReadFull(conn, record[5:]); err != nil {
		return nil, err
	}

	return record, nil
}

// ClientHelloBuilder builds a TLS ClientHello message
type ClientHelloBuilder struct {
	serverName string
	sessionID  []byte
	ecdhePub   []byte
	authData   *AuthData
}

// NewClientHelloBuilder creates a new ClientHello builder
func NewClientHelloBuilder(serverName string, ecdhePub []byte, authData *AuthData) *ClientHelloBuilder {
	sessionID := make([]byte, SessionIDSize)
	_, _ = rand.Read(sessionID)

	return &ClientHelloBuilder{
		serverName: serverName,
		sessionID:  sessionID,
		ecdhePub:   ecdhePub,
		authData:   authData,
	}
}

// Build builds the complete TLS ClientHello message
func (b *ClientHelloBuilder) Build() []byte {
	clientHello := b.buildClientHelloContent()

	record := make([]byte, 5+len(clientHello))
	record[0] = RecordTypeHandshake
	binary.BigEndian.PutUint16(record[1:3], TLSVersion10)
	binary.BigEndian.PutUint16(record[3:5], uint16(len(clientHello)))
	copy(record[5:], clientHello)

	return record
}

func (b *ClientHelloBuilder) buildClientHelloContent() []byte {
	extensions := b.buildExtensions()

	cipherSuites := []uint16{
		0x1302, // TLS_AES_256_GCM_SHA384
		0x1303, // TLS_CHACHA20_POLY1305_SHA256
		0x1301, // TLS_AES_128_GCM_SHA256
		0xc02c, // TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
		0xc02b, // TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	}

	bodySize := 2 + RandomSize + 1 + len(b.sessionID) +
		2 + len(cipherSuites)*2 +
		1 + 1 +
		2 + len(extensions)

	msg := make([]byte, 4+bodySize)
	msg[0] = HandshakeTypeClientHello
	msg[1] = byte(bodySize >> 16)
	msg[2] = byte(bodySize >> 8)
	msg[3] = byte(bodySize)

	offset := 4

	binary.BigEndian.PutUint16(msg[offset:], TLSVersion12)
	offset += 2

	_, _ = rand.Read(msg[offset : offset+RandomSize])
	offset += RandomSize

	msg[offset] = byte(len(b.sessionID))
	offset++
	copy(msg[offset:], b.sessionID)
	offset += len(b.sessionID)

	binary.BigEndian.PutUint16(msg[offset:], uint16(len(cipherSuites)*2))
	offset += 2
	for _, suite := range cipherSuites {
		binary.BigEndian.PutUint16(msg[offset:], suite)
		offset += 2
	}

	msg[offset] = 1
	offset++
	msg[offset] = 0
	offset++

	binary.BigEndian.PutUint16(msg[offset:], uint16(len(extensions)))
	offset += 2
	copy(msg[offset:], extensions)

	return msg
}

func (b *ClientHelloBuilder) buildExtensions() []byte {
	var extensions []byte
	extensions = append(extensions, b.buildSNIExtension()...)
	extensions = append(extensions, b.buildSupportedVersionsExtension()...)
	extensions = append(extensions, b.buildSupportedGroupsExtension()...)
	extensions = append(extensions, b.buildKeyShareExtension()...)
	extensions = append(extensions, b.buildPaddingExtension()...)
	return extensions
}

func (b *ClientHelloBuilder) buildSNIExtension() []byte {
	nameBytes := []byte(b.serverName)
	nameLen := len(nameBytes)

	ext := make([]byte, 4+2+1+2+nameLen)
	binary.BigEndian.PutUint16(ext[0:2], ExtensionServerName)
	binary.BigEndian.PutUint16(ext[2:4], uint16(2+1+2+nameLen))
	binary.BigEndian.PutUint16(ext[4:6], uint16(1+2+nameLen))
	ext[6] = 0x00
	binary.BigEndian.PutUint16(ext[7:9], uint16(nameLen))
	copy(ext[9:], nameBytes)

	return ext
}

func (b *ClientHelloBuilder) buildSupportedVersionsExtension() []byte {
	versions := []uint16{0x0304, 0x0303}

	ext := make([]byte, 4+1+len(versions)*2)
	binary.BigEndian.PutUint16(ext[0:2], ExtensionSupportedVersion)
	binary.BigEndian.PutUint16(ext[2:4], uint16(1+len(versions)*2))
	ext[4] = byte(len(versions) * 2)
	offset := 5
	for _, v := range versions {
		binary.BigEndian.PutUint16(ext[offset:], v)
		offset += 2
	}

	return ext
}

func (b *ClientHelloBuilder) buildSupportedGroupsExtension() []byte {
	groups := []uint16{
		0x001d, // x25519
		0x0017, // secp256r1
		0x0018, // secp384r1
	}

	ext := make([]byte, 4+2+len(groups)*2)
	binary.BigEndian.PutUint16(ext[0:2], ExtensionSupportedGroups)
	binary.BigEndian.PutUint16(ext[2:4], uint16(2+len(groups)*2))
	binary.BigEndian.PutUint16(ext[4:6], uint16(len(groups)*2))
	offset := 6
	for _, g := range groups {
		binary.BigEndian.PutUint16(ext[offset:], g)
		offset += 2
	}

	return ext
}

func (b *ClientHelloBuilder) buildKeyShareExtension() []byte {
	keyShareEntry := make([]byte, 4+len(b.ecdhePub))
	binary.BigEndian.PutUint16(keyShareEntry[0:2], KeyShareGroupX25519)
	binary.BigEndian.PutUint16(keyShareEntry[2:4], uint16(len(b.ecdhePub)))
	copy(keyShareEntry[4:], b.ecdhePub)

	ext := make([]byte, 4+2+len(keyShareEntry))
	binary.BigEndian.PutUint16(ext[0:2], ExtensionKeyShare)
	binary.BigEndian.PutUint16(ext[2:4], uint16(2+len(keyShareEntry)))
	binary.BigEndian.PutUint16(ext[4:6], uint16(len(keyShareEntry)))
	copy(ext[6:], keyShareEntry)

	return ext
}

func (b *ClientHelloBuilder) buildPaddingExtension() []byte {
	authBytes := b.authData.Marshal()

	randomPaddingLen := 64 + (int(authBytes[0]) % 128)
	paddingData := make([]byte, len(b.ecdhePub)+len(authBytes)+randomPaddingLen)
	copy(paddingData[0:32], b.ecdhePub)
	copy(paddingData[32:88], authBytes)
	_, _ = rand.Read(paddingData[88:])

	ext := make([]byte, 4+len(paddingData))
	binary.BigEndian.PutUint16(ext[0:2], ExtensionPadding)
	binary.BigEndian.PutUint16(ext[2:4], uint16(len(paddingData)))
	copy(ext[4:], paddingData)

	return ext
}

// ParseServerHello parses a TLS ServerHello message
func ParseServerHello(data []byte) (*ServerHelloInfo, error) {
	if len(data) < 5 {
		return nil, ErrMessageTooShort
	}

	info := &ServerHelloInfo{}

	if data[0] != RecordTypeHandshake {
		return nil, ErrInvalidRecordType
	}

	recordLen := int(binary.BigEndian.Uint16(data[3:5]))
	if len(data) < 5+recordLen {
		return nil, ErrMessageTooShort
	}

	handshake := data[5 : 5+recordLen]
	if len(handshake) < 4 {
		return nil, ErrMessageTooShort
	}

	if handshake[0] != HandshakeTypeServerHello {
		return nil, ErrInvalidHandshakeType
	}

	handshakeLen := int(handshake[1])<<16 | int(handshake[2])<<8 | int(handshake[3])
	if len(handshake) < 4+handshakeLen {
		return nil, ErrMessageTooShort
	}

	msg := handshake[4 : 4+handshakeLen]
	offset := 0

	if len(msg) < offset+2 {
		return nil, ErrMessageTooShort
	}
	info.Version = binary.BigEndian.Uint16(msg[offset : offset+2])
	offset += 2

	if len(msg) < offset+RandomSize {
		return nil, ErrMessageTooShort
	}
	copy(info.Random[:], msg[offset:offset+RandomSize])
	offset += RandomSize

	if len(msg) < offset+1 {
		return nil, ErrMessageTooShort
	}
	sessionIDLen := int(msg[offset])
	offset++
	if len(msg) < offset+sessionIDLen {
		return nil, ErrMessageTooShort
	}
	info.SessionID = make([]byte, sessionIDLen)
	copy(info.SessionID, msg[offset:offset+sessionIDLen])
	offset += sessionIDLen

	if len(msg) < offset+2 {
		return nil, ErrMessageTooShort
	}
	info.CipherSuite = binary.BigEndian.Uint16(msg[offset : offset+2])
	offset += 2

	if len(msg) < offset+1 {
		return nil, ErrMessageTooShort
	}
	offset++

	if len(msg) < offset+2 {
		return info, nil
	}
	extensionsLen := int(binary.BigEndian.Uint16(msg[offset : offset+2]))
	offset += 2
	if len(msg) < offset+extensionsLen {
		return nil, ErrMessageTooShort
	}

	extensionsEnd := offset + extensionsLen
	for offset < extensionsEnd {
		if offset+4 > len(msg) {
			break
		}
		extType := binary.BigEndian.Uint16(msg[offset : offset+2])
		extLen := int(binary.BigEndian.Uint16(msg[offset+2 : offset+4]))
		offset += 4

		if offset+extLen > len(msg) {
			break
		}
		extData := msg[offset : offset+extLen]
		offset += extLen

		switch extType {
		case ExtensionKeyShare:
			group, keyData := parseKeyShareEntry(extData)
			info.KeyShareGroup = group
			info.KeyShareData = keyData
		case ExtensionPadding:
			info.Padding = make([]byte, len(extData))
			copy(info.Padding, extData)
		}
	}

	return info, nil
}

func parseKeyShareEntry(data []byte) (uint16, []byte) {
	if len(data) < 4 {
		return 0, nil
	}

	group := binary.BigEndian.Uint16(data[0:2])
	keyLen := int(binary.BigEndian.Uint16(data[2:4]))

	if len(data) < 4+keyLen {
		return 0, nil
	}

	keyData := make([]byte, keyLen)
	copy(keyData, data[4:4+keyLen])

	return group, keyData
}

package shadowtlsplus

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/hkdf"
)

const (
	// Auth data sizes
	AuthDataSize  = 8 + 16 + 32 // Timestamp + Nonce + HMAC = 56 bytes
	TimestampSize = 8
	AuthNonceSize = 16
	HMACSize      = 32
	TimeWindow    = 30 * time.Second

	// Session key sizes
	SessionKeyInfo   = "stp-keys"
	SessionKeyLength = 96
	AESKeySize       = 32
	IVSize           = 16

	// Nonce sizes
	NonceSize       = 12
	NonceSuffixSize = 8
	FixedPrefixSize = 4

	// GCM sizes
	GCMTagSize         = 16
	EncryptionOverhead = NonceSuffixSize + GCMTagSize
)

// AuthData represents the authentication data embedded in ClientHello padding
type AuthData struct {
	Timestamp int64
	Nonce     [AuthNonceSize]byte
	HMAC      [HMACSize]byte
}

// GenerateAuthData generates authentication data for the handshake
func GenerateAuthData(password string) (*AuthData, error) {
	auth := &AuthData{
		Timestamp: time.Now().Unix(),
	}

	if _, err := rand.Read(auth.Nonce[:]); err != nil {
		return nil, fmt.Errorf("failed to generate auth nonce: %w", err)
	}

	auth.HMAC = computeAuthHMAC(password, auth.Timestamp, auth.Nonce[:])
	return auth, nil
}

func computeAuthHMAC(password string, timestamp int64, nonce []byte) [HMACSize]byte {
	h := hmac.New(sha256.New, []byte(password))

	var tsBytes [8]byte
	binary.BigEndian.PutUint64(tsBytes[:], uint64(timestamp))
	h.Write(tsBytes[:])
	h.Write(nonce)

	var result [HMACSize]byte
	copy(result[:], h.Sum(nil))
	return result
}

// Marshal serializes AuthData to bytes
func (a *AuthData) Marshal() []byte {
	data := make([]byte, AuthDataSize)
	binary.BigEndian.PutUint64(data[0:8], uint64(a.Timestamp))
	copy(data[8:24], a.Nonce[:])
	copy(data[24:56], a.HMAC[:])
	return data
}

// UnmarshalAuthData deserializes AuthData from bytes
func UnmarshalAuthData(data []byte) (*AuthData, error) {
	if len(data) < AuthDataSize {
		return nil, fmt.Errorf("auth data too short: got %d, want %d", len(data), AuthDataSize)
	}

	auth := &AuthData{
		Timestamp: int64(binary.BigEndian.Uint64(data[0:8])),
	}
	copy(auth.Nonce[:], data[8:24])
	copy(auth.HMAC[:], data[24:56])

	return auth, nil
}

// Verify verifies the authentication data
func (a *AuthData) Verify(password string) bool {
	now := time.Now().Unix()
	diff := now - a.Timestamp
	if diff < 0 {
		diff = -diff
	}
	if diff > int64(TimeWindow.Seconds()) {
		return false
	}

	expectedHMAC := computeAuthHMAC(password, a.Timestamp, a.Nonce[:])
	return hmac.Equal(a.HMAC[:], expectedHMAC[:])
}

// NonceBytes returns the nonce as a byte slice
func (a *AuthData) NonceBytes() []byte {
	return a.Nonce[:]
}

// KeyPair represents an ECDHE key pair using X25519 curve
type KeyPair struct {
	Private *ecdh.PrivateKey
	Public  *ecdh.PublicKey
}

// GenerateKeyPair generates a new ephemeral ECDHE key pair using X25519
func GenerateKeyPair() (*KeyPair, error) {
	curve := ecdh.X25519()

	privateKey, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ECDHE key pair: %w", err)
	}

	return &KeyPair{
		Private: privateKey,
		Public:  privateKey.PublicKey(),
	}, nil
}

// PublicKeyFromBytes creates a public key from raw bytes
func PublicKeyFromBytes(data []byte) (*ecdh.PublicKey, error) {
	curve := ecdh.X25519()
	return curve.NewPublicKey(data)
}

// ComputeSharedSecret computes the shared secret using ECDH
func ComputeSharedSecret(myPrivate *ecdh.PrivateKey, peerPublic *ecdh.PublicKey) ([]byte, error) {
	sharedSecret, err := myPrivate.ECDH(peerPublic)
	if err != nil {
		return nil, fmt.Errorf("failed to compute shared secret: %w", err)
	}
	return sharedSecret, nil
}

// PublicKeyBytes returns the raw bytes of the public key
func (kp *KeyPair) PublicKeyBytes() []byte {
	return kp.Public.Bytes()
}

// SessionKeys holds the derived session keys for both directions
type SessionKeys struct {
	ClientWriteKey []byte
	ServerWriteKey []byte
	ClientWriteIV  []byte
	ServerWriteIV  []byte
}

// DeriveSessionKeys derives session keys from the shared secret using HKDF-SHA256
func DeriveSessionKeys(sharedSecret []byte) (*SessionKeys, error) {
	hkdfReader := hkdf.New(sha256.New, sharedSecret, nil, []byte(SessionKeyInfo))

	keyMaterial := make([]byte, SessionKeyLength)
	if _, err := io.ReadFull(hkdfReader, keyMaterial); err != nil {
		return nil, fmt.Errorf("failed to derive session keys: %w", err)
	}

	return &SessionKeys{
		ClientWriteKey: keyMaterial[0:32],
		ServerWriteKey: keyMaterial[32:64],
		ClientWriteIV:  keyMaterial[64:80],
		ServerWriteIV:  keyMaterial[80:96],
	}, nil
}

// ClientFixedPrefix returns the 4-byte fixed prefix for client nonce construction
func (sk *SessionKeys) ClientFixedPrefix() [4]byte {
	var prefix [4]byte
	copy(prefix[:], sk.ClientWriteIV[:4])
	return prefix
}

// ServerFixedPrefix returns the 4-byte fixed prefix for server nonce construction
func (sk *SessionKeys) ServerFixedPrefix() [4]byte {
	var prefix [4]byte
	copy(prefix[:], sk.ServerWriteIV[:4])
	return prefix
}

// NonceGenerator generates unique nonces for AES-GCM encryption
type NonceGenerator struct {
	fixedPrefix [FixedPrefixSize]byte
	counter     atomic.Uint64
}

// NewNonceGenerator creates a new nonce generator with the given fixed prefix
func NewNonceGenerator(fixedPrefix [FixedPrefixSize]byte) *NonceGenerator {
	return &NonceGenerator{
		fixedPrefix: fixedPrefix,
	}
}

// Next generates the next nonce for the given stream ID
func (ng *NonceGenerator) Next(streamID uint16) (nonce [NonceSize]byte, suffix [NonceSuffixSize]byte) {
	counter := ng.counter.Add(1) - 1

	copy(nonce[0:4], ng.fixedPrefix[:])
	binary.BigEndian.PutUint16(nonce[4:6], streamID)
	binary.BigEndian.PutUint16(nonce[6:8], uint16(counter>>32))
	binary.BigEndian.PutUint32(nonce[8:12], uint32(counter))

	copy(suffix[:], nonce[4:12])

	return nonce, suffix
}

// NonceFromSuffix reconstructs the full nonce from the suffix and fixed prefix
func NonceFromSuffix(fixedPrefix [FixedPrefixSize]byte, suffix [NonceSuffixSize]byte) [NonceSize]byte {
	var nonce [NonceSize]byte
	copy(nonce[0:4], fixedPrefix[:])
	copy(nonce[4:12], suffix[:])
	return nonce
}

// CryptoEngine handles encryption/decryption for a session
type CryptoEngine struct {
	sendCipher cipher.AEAD
	recvCipher cipher.AEAD
	sendNonce  *NonceGenerator
	recvPrefix [FixedPrefixSize]byte
	isClient   bool
}

// NewCryptoEngine creates a new crypto engine from session keys
func NewCryptoEngine(keys *SessionKeys, isClient bool) (*CryptoEngine, error) {
	var sendKey, recvKey []byte
	var sendPrefix, recvPrefix [FixedPrefixSize]byte

	if isClient {
		sendKey = keys.ClientWriteKey
		recvKey = keys.ServerWriteKey
		sendPrefix = keys.ClientFixedPrefix()
		recvPrefix = keys.ServerFixedPrefix()
	} else {
		sendKey = keys.ServerWriteKey
		recvKey = keys.ClientWriteKey
		sendPrefix = keys.ServerFixedPrefix()
		recvPrefix = keys.ClientFixedPrefix()
	}

	sendCipher, err := createAEAD(sendKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create send cipher: %w", err)
	}

	recvCipher, err := createAEAD(recvKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create receive cipher: %w", err)
	}

	return &CryptoEngine{
		sendCipher: sendCipher,
		recvCipher: recvCipher,
		sendNonce:  NewNonceGenerator(sendPrefix),
		recvPrefix: recvPrefix,
		isClient:   isClient,
	}, nil
}

func createAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Encrypt encrypts plaintext and returns (nonceSuffix, ciphertext)
func (ce *CryptoEngine) Encrypt(plaintext []byte, streamID uint16) (suffix [NonceSuffixSize]byte, ciphertext []byte) {
	nonce, suffix := ce.sendNonce.Next(streamID)
	ciphertext = ce.sendCipher.Seal(nil, nonce[:], plaintext, nil)
	return suffix, ciphertext
}

// Decrypt decrypts ciphertext using the provided nonce suffix
func (ce *CryptoEngine) Decrypt(suffix [NonceSuffixSize]byte, ciphertext []byte) ([]byte, error) {
	nonce := NonceFromSuffix(ce.recvPrefix, suffix)

	plaintext, err := ce.recvCipher.Open(nil, nonce[:], ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return plaintext, nil
}

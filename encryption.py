import io
import os

from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES, PKCS1_OAEP

from base64 import b64encode, b64decode


class Encryption:
    modulus_length = 2048

    def __init__(self, name):
        d = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(d, 'keys/encryption/{}.pem'.format(name))
        if os.path.isfile(path):
            self.import_key(path)
        else:
            os.makedirs(os.path.join(d, 'keys/encryption'), exist_ok=True)
            self.generate_key()
            self.export_key(path)

    def generate_key(self):
        self.private_key = RSA.generate(self.modulus_length)

    def export_key(self, fn):
        f = open(fn, 'wb')
        f.write(self.private_key.export_key())

    def import_key(self, fn):
        f = open(fn, 'rb')
        self.private_key = RSA.import_key(f.read())

    def get_public_key(self):
        return self.private_key.publickey().export_key()

    def encrypt(self, data, pem_recipient_key):
        recipient_key = RSA.import_key(pem_recipient_key)

        f = io.BytesIO()
        session_key = get_random_bytes(16)

        # Encrypt the session key with the public RSA key
        cipher_rsa = PKCS1_OAEP.new(recipient_key)
        enc_session_key = cipher_rsa.encrypt(session_key)

        # Encrypt the data with the AES session key
        cipher_aes = AES.new(session_key, AES.MODE_EAX)
        ciphertext, tag = cipher_aes.encrypt_and_digest(data)
        [f.write(x) for x in (
            enc_session_key, cipher_aes.nonce, tag, ciphertext
        )]

        f.seek(0)

        return b64encode(f.read())

    def decrypt(self, base64_data):
        f = io.BytesIO(b64decode(base64_data))

        enc_session_key, nonce, tag, ciphertext = [f.read(x) for x in (
            self.private_key.size_in_bytes(), 16, 16, -1)
        ]

        # Decrypt the session key with the private RSA key
        cipher_rsa = PKCS1_OAEP.new(self.private_key)
        session_key = cipher_rsa.decrypt(enc_session_key)

        # Decrypt the data with the AES session key
        cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce)
        data = cipher_aes.decrypt_and_verify(ciphertext, tag)

        return data

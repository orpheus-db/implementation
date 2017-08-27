import hashlib, binascii

class EncryptionTool():

	# salt should have a random state?
	@staticmethod
	def passphrase_hash(raw, salt=b'datahub', method='sha256', iteration=100000):
		return binascii.hexlify(hashlib.pbkdf2_hmac(method, raw, salt, iteration))
from encryption import EncryptionTool

import exceptions as sys_exception
import json

class LocalUserExistError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class InvalidCredentialError(Exception):
    def __str__(self):
        return "credentials does not match records"

class UserManager(object):
	@classmethod
	def config_path(self):
		return ".meta/config"

	@classmethod
	def user_path(self):
		return ".meta/users"

	@classmethod
	def check_user_exist(cls, user):
		from os import listdir
		from os.path import isfile
		return user in [usr for usr in listdir(cls.user_path())] and isfile("/".join([cls.user_path(), usr, 'config']))

	@classmethod
	def create_user(cls, user, password):
		from os import makedirs
		if cls.check_user_exist(user):
			raise LocalUserExistError("username %s exists, try a different one" % user)
			return None
		passphrase = EncryptionTool.passphrase_hash(password)
		user_directory = '/'.join([cls.user_path(),user])
		makedirs(user_directory) # make the directory, need to check if have permission
		user_obj = {
			'user' : user,
			'passphrase' : passphrase
		}
		with open('/'.join([user_directory, 'config']), 'w+') as f:
			f.write(json.dumps(user_obj))
		return 1


	# this method is very dangrous! use caution
	@classmethod
	def delete_user(cls, user, password):
		pass 


	@classmethod
	def get_current_state(cls):
		try:
			with open(cls.config_path(), 'r') as f:
				config_info = json.loads(f.readline())
		except Exception as inst:
			return None
		return config_info

	@classmethod
	def write_current_state(cls, obj):
		with open(cls.config_path(), 'w') as f:
			f.write(json.dumps(obj))


	@classmethod
	def verify_credential(cls, user, raw):
		if cls.check_user_exist(user):
			user_obj = cls.__get_user_config(user)
			if user_obj['passphrase'] == EncryptionTool.passphrase_hash(raw):
				return True
		raise InvalidCredentialError()
		return False

	@classmethod
	def __get_user_config(cls, user):
		with open('/'.join([cls.user_path(), user, 'config']), 'r') as f:
			user_obj = json.loads(f.readline())
		return user_obj

	# for debug purpose
	@classmethod
	def __list_user(cls):
		from os import listdir
		return [usr for usr in listdir(cls.user_path())]

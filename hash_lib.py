from ast import keyword
import hashlib


def hash_code(key_word, ostring):
    '''
    对传入的字符串进行hash加密
    '''
    hash_obj = hashlib.md5(key_word.encode('utf-8'))
    hash_obj.update(ostring.encode('utf-8'))
    result = hash_obj.hexdigest()

    return result
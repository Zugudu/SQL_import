import mysql.connector
from dbconfig import DB
import os
import re
import json


jsn1 = os.path.join(os.getcwd(), 'jsn1')


def check_contains(dict, lang):
	if lang in dict.keys():
		return True
	if 'audio' in dict.keys():
		if lang in dict['audio'].keys():
			return True
	return False


def object_handler(record, file, word):
	apostrophes = ['`', '"', '’', '´']
	w = word.lower()
	for el in apostrophes:
		w = w.replace(el, "'")

	us_lang = None
	gb_lang = None

	if check_contains(record, 'us'):
		if 'us' in record.keys():
			if 'audio' in record.keys():
				if 'us' in record['audio'].keys():
					us_lang = '{"1":{"us":"' + str(record['us']) + '","audio":' + str(
						record['audio']) + '}}'
			else:
				us_lang = '{"1":{"us":"' + str(record['us']) + '"}}'
		else:
			us_lang = '{"1":{"audio":' + str(record['audio']) + '}}'
			
	if check_contains(record, 'gb'):
		if 'gb' in record.keys():
			if 'audio' in record.keys():
				if 'gb' in record['audio'].keys():
					gb_lang = '{"1":{"gb":"' + str(record['gb']) + '","audio":' + str(
						record['audio']) + '}}'
			else:
				gb_lang = '{"1":{"gb":"' + str(record['gb']) + '"}}'
		else:
			gb_lang = '{"1":{"audio":' + str(record['audio']) + '}}'

	origin_lang = re.search('.*v((ru|uk|pl))\.json$', file).group(1)
	parent = record.get('awords', None)
	if parent:
		parent = str(parent)

	print(w)  # TODO REMOVE
	sql_client = mysql.connector.connect(**DB)
	cursor = sql_client.cursor()
	cursor.execute('SELECT COUNT(*) FROM words WHERE word=%s;', (w,))
	if cursor.fetchone()[0] == 0:
		cursor.execute('INSERT INTO words (word, parent, gb, us , reserved) VALUES(%s, %s, %s, %s, %s);',
						(w, parent, gb_lang, us_lang, file))
		sql_client.commit()

	cursor.execute('SELECT idkey FROM words WHERE word=%s;', (w,))
	id = cursor.fetchone()[0]

	cursor.execute('SELECT COUNT(*) FROM translates WHERE idkey=%s AND lang=%s;', (id, origin_lang))
	if cursor.fetchone()[0] == 0:
		cursor.execute('INSERT INTO translates VALUES(NULL, %s, %s, %s, %s, %s, %s);',
					(id, w, origin_lang, record.get('wforms', None), record.get('trans', None), file))
		sql_client.commit()

	sql_client.close()


if __name__ == '__main__':
	if not os.path.exists(jsn1):
		print('Directory jsn1 doesn\'t exist!')
		exit(1)
	if not os.path.isdir(jsn1):
		print('jsn1 is not a directory!')
		exit(1)

	try:
		correct_name = '.*v(ru|uk|pl)\.json$'
		# Get all correct files
		files_list = [f for f in os.listdir(jsn1) if
					os.path.isfile(os.path.join(jsn1, f)) and re.search(correct_name, f)]

		for file in files_list:
			try:
				with open(os.path.join(jsn1, file), encoding='utf-8') as fd:
					data = json.load(fd)
					if isinstance(data, dict):
						for W in data.keys():
							if check_contains(data[W], 'gb') or check_contains(data[W], 'us'):
								object_handler(data[W], file, W)
					else:
						print('W: File', file, "doesn't contains json dictionary!")

			except FileNotFoundError:
				print('E: File', file, "doesn't exist!")
			except json.decoder.JSONDecodeError:
				print('W: File', file, 'is corrupted')
			except Exception as ex:
				print('E: Some unexceptional error in file', file, '-', ex)
	except KeyboardInterrupt:
		print('Stop by user!')
		exit(0)

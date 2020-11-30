import mysql.connector
from mysql.connector import DataError

from dbconfig import DB

from sys import stdout
from datetime import datetime

import import_config
import os
import re
import json


jsn1 = os.path.join(os.getcwd(), import_config.json)
log_file = os.path.join(os.getcwd(), import_config.log_file)
sql_client = mysql.connector.connect(**DB)


def check_contains(dict, lang):
	if lang in dict.keys():
		return True
	if 'audio' in dict.keys():
		if lang in dict['audio'].keys():
			return True
	return False


def log(data):
	try:
		with open(log_file, 'a+') as fd:
			fd.write(data)
			fd.write('\n')
	except Exception as ex:
		print('E: Some unexceptional error in file', file, '-', ex)
	return data


def object_handler(record, file, word):
	def get_lang_data(row, lang):
		a = None
		if check_contains(row, lang):
			if lang in row.keys():
				if 'audio' in row.keys():
					if lang in row['audio'].keys():
						a = {'1': {lang: str(row[lang]), 'audio': str(row['audio'])}}
				else:
					a = {'1': {lang: str(row[lang])}}
			else:
				a = {'1': {'audio': str(row['audio'])}}
			a = json.dumps(a)
		return a

	def reserved_write(row, mes):
		res = row[6]
		res += mes
		cursor.execute(
			'UPDATE {} SET reserved=%s WHERE idkey=%s;'.format(import_config.words),
			(res, row[0]))
		sql_client.commit()
		cursor.execute('SELECT * FROM {} WHERE word=%s;'.format(import_config.words), (w,))
		return cursor.fetchone()

	def lang_mod(row, row_num, lang_id, lang):
		if row[row_num] and row[row_num] != '':
			if row[row_num] != lang:
				lang_data = None
				try:
					lang_data = json.loads(row[row_num])["1"]
				except json.decoder.JSONDecodeError:
					print(log('In word "{}" {} row have corrupted JSON. Spotted when work with file "{}"'.format(w, lang_id, file)))
				except KeyError:
					print(log('In word "{}" {} row have KeyError. Spotted when work with file "{}"'.format(w, lang_id, file)))
				try:
					if (lang and lang != '') and lang_data.get(lang_id, None) != lang:
						row = reserved_write(row, ';{} in {}'.format(lang, file))
						print(log('W: {} in word "{}" are not equals {} from file "{}"'.format(lang_id, w, lang_id, file)))
					else:
						if not re.match('w\/.*', lang_data.get('audio', '')):
							if lang:
								curr_lang = json.loads(lang)['1']
								if re.match('w\/.*', curr_lang.get('audio', '')):
									if os.path.isfile(curr_lang['audio']):
										cursor.execute(
											'UPDATE {} SET {}=%s WHERE idkey=%s;'.format(lang_id, import_config.words),
											(lang, row[0],))
										sql_client.commit()
									else:
										row = reserved_write(row, ';{} in {}'.format(curr_lang['audio'], file))
										print(log('W: MP3 file not found for word "{}" from file "{}"'.format(w, file)))

						elif import_config.test_file_path:
							if not os.path.isfile(lang_data['audio']):
								print(log('Audio file "{}" in word "{}" not found'.format(lang_data['audio'], w)))
				except KeyError:
					print(log('In word "{}" {} row have corrupted JSON in file "{}"'.format(w, lang_id, file)))
		elif lang:
			cursor.execute('UPDATE {} SET {}=%s WHERE idkey=%s;'.format(import_config.words, lang_id), (lang, row[0]))
			sql_client.commit()

	apostrophes = ['`', '"', '’', '´']
	w = word.lower()
	for el in apostrophes:
		w = w.replace(el, "'")

	us_lang = get_lang_data(record, 'us')
	gb_lang = get_lang_data(record, 'gb')

	origin_lang = re.search('.*v((ru|uk|pl))\.json$', file).group(1)
	parent = record.get('awords', None)
	if parent:
		parent = str(parent)

	cursor = sql_client.cursor()
	cursor.execute('SELECT COUNT(*) FROM {} WHERE word=%s;'.format(import_config.words), (w,))
	if cursor.fetchone()[0] == 0:
		# INFO
		print(w, end=' ')
		stdout.flush()

		cursor.execute('INSERT INTO {} (word, parent, gb, us , reserved) VALUES(%s, %s, %s, %s, %s);'
						.format(import_config.words),
						(w, parent, gb_lang, us_lang, file))
		sql_client.commit()
	else:
		cursor.execute('SELECT * FROM {} WHERE word=%s;'.format(import_config.words), (w,))
		row = cursor.fetchone()
		if (row[3] is None or row[3] == '') and (parent and parent != ''):
			reserved = row[6]
			reserved += ';{} in {}'.format(parent, file)
			cursor.execute('UPDATE {} SET parent=%s, reserved=%s WHERE idkey=%s;'.format(import_config.words),
						(parent, reserved, row[0]))
			sql_client.commit()
			cursor.execute('SELECT * FROM {} WHERE word=%s;'.format(import_config.words), (w,))
			row = cursor.fetchone()
			print(log('W: Parent "{}" was added in word "{}" from file "{}"'.format(parent, w, file)))
		elif row[3] != parent:
			row = reserved_write(row, ';{} in {}'.format(parent, file))
			print(log('W: Parent in word "{}" are not equals parent from file "{}"'.format(w, file)))

		if check_contains(record, 'gb'):
			lang_mod(row, 4, 'gb', gb_lang)
			cursor.execute('SELECT * FROM {} WHERE word=%s;'.format(import_config.words), (w,))
			row = cursor.fetchone()
		if check_contains(record, 'us'):
			lang_mod(row, 5, 'us', us_lang)

	cursor.execute('SELECT idkey FROM {} WHERE word=%s;'.format(import_config.words), (w,))
	id = cursor.fetchone()[0]

	cursor.execute('SELECT COUNT(*) FROM {} WHERE idkey=%s AND lang=%s;'.format(import_config.translates),
					(id, origin_lang))
	if cursor.fetchone()[0] == 0:
		# INFO
		print('[{}]'.format(w), end=' ')
		stdout.flush()

		cursor.execute('INSERT INTO {} VALUES(NULL, %s, %s, %s, %s, %s, %s);'.format(import_config.translates),
					(id, w, origin_lang, record.get('wforms', None), record.get('trans', None), file))
		sql_client.commit()


if __name__ == '__main__':
	if not os.path.exists(jsn1):
		print('Directory jsn1 doesn\'t exist!')
		exit(1)
	if not os.path.isdir(jsn1):
		print('jsn1 is not a directory!')
		exit(1)
	if os.path.exists(log_file) and os.path.isdir(log_file):
		print('Log file cannot be a directory!')
		exit(1)

	log('Started in {}'.format(datetime.now().strftime('%x %X')))

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
						# INFO
						print('\r', file, end=': ')
						stdout.flush()

						for W in data.keys():
							if check_contains(data[W], 'gb') or check_contains(data[W], 'us'):
								object_handler(data[W], file, W)
						print('\n')
					else:
						print('W: File', file, "doesn't contains json dictionary!")

			except FileNotFoundError:
				err_msg = "E: File {} doesn't exist!".format(file)
				print(log(err_msg))
			except json.decoder.JSONDecodeError:
				err_msg = 'W: File {} is corrupted'.format(file)
				print(log(err_msg))
			except DataError:
				print('\n')
				pass
			except Exception as ex:
				err_msg = 'E: Some unexceptional error in file {} - {}'.format(file, ex)
				print(log(err_msg))
	except KeyboardInterrupt:
		print('Stop by user!')
	finally:
		sql_client.close()
		exit(0)

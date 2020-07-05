from clickhouse_driver import Client
import datetime as dt
import random
import argparse


class DBEngine:
	def __init__(self):
		self._connection = Client(host='localhost', database='test_task')

	def get_installs(self, start_date, end_date, ref_regexp):
		res = self._connection.execute(
			f"""SELECT count(distinct user_id) FROM test_task.fact_reg 
			WHERE (ts between '{start_date}' and '{end_date}') and match(ref, '{ref_regexp}')""")

		return res[0][0]

	def get_retentions(self, start_date, end_date, ref_regexp):
		res = self._connection.execute(
			f"""SELECT dateDiff('day', toDateTime('{start_date}'), fl.ts) AS day,
			count(distinct fl.user_id) AS retention
			FROM test_task.fact_login fl JOIN test_task.fact_reg fr ON fl.user_id = fr.user_id
			WHERE (fl.ts BETWEEN '{start_date}' AND '{end_date}') AND (match(fr.ref, '{ref_regexp}'))
			GROUP BY day ORDER BY day WITH FILL""")

		return res

	# при условии, что мы учитываем платежи уникальных пользователей
	def get_ltv(self, start_date, end_date, ref_regexp):
		res = self._connection.execute(
			f"""SELECT sum(fp.usd)/(arrayUniq(groupArray(fp.user_id)))
			FROM test_task.fact_reg fr JOIN test_task.fact_payment fp ON fr.user_id = fp.user_id
			WHERE (fp.ts BETWEEN '{start_date}' AND '{end_date}') AND match(ref, '{ref_regexp}')""")

		return res[0][0]

	# рассчитываем как обычное среднее для группы
	def get_ltv_avg(self, start_date, end_date, ref_regexp):
		res = self._connection.execute(
			f"""SELECT avg(fp.usd) FROM test_task.fact_reg fr JOIN test_task.fact_payment fp ON fr.user_id = fp.user_id
			WHERE (fp.ts BETWEEN '{start_date}' AND '{end_date}') AND match(ref, '{ref_regexp}')""")

		return res[0][0]

	def generate_data(self, user_count):
		refs_patterns = ['ads', 'social', 'media']
		_start_date = dt.datetime(2020, 5, 4)
		users = range(user_count)

		pay_values, login_values, reg_values = [], [], []
		for user in users:
			rnd_date = _start_date + dt.timedelta(days=random.randrange(7))
			reg_values.append((rnd_date, user, f'{random.choice(refs_patterns)}{random.randrange(3)}'))
			login_values.append((rnd_date, user))

		for _ in users:
			rnd_date = _start_date + dt.timedelta(days=random.randrange(7), hours=random.randrange(24))
			login_values.append((rnd_date, random.randrange(user_count)))
			pay_values.append((rnd_date, random.randrange(user_count), random.randrange(1, 10)))

		self._connection.execute('INSERT INTO test_task.fact_reg VALUES', reg_values, types_check=True)
		self._connection.execute('INSERT INTO test_task.fact_login VALUES', login_values, types_check=True)
		self._connection.execute('INSERT INTO test_task.fact_payment VALUES', pay_values, types_check=True)


def main(params):
	db = DBEngine()
	if params.users_count:
		db.generate_data(params.users_count)

	elif params.action and params.start_date and params.end_date and params.refs_regex:
		actions_dict = {
			"installs": db.get_installs,
			"retention": db.get_retentions,
			"ltv_avg": db.get_ltv_avg,
			"ltv": db.get_ltv
		}
		print(actions_dict[params.action](params.start_date,
		                                  params.end_date,
		                                  params.refs_regex))
	else:
		print('Missing arguments')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-ts1', action='store', dest='start_date',
	                    type=lambda s: dt.datetime.strptime(s, '%d.%m.%Y'), help='Start date dd.mm.yyyy')
	parser.add_argument('-ts2', action='store', dest='end_date',
	                    type=lambda s: dt.datetime.strptime(s, '%d.%m.%Y'), help='End date dd.mm.yyyy')
	parser.add_argument('-ref', action='store', dest='refs_regex', help='Ref regex')
	parser.add_argument('-act', action='store', dest='action',
	                    choices=['installs', 'retention', 'ltv_avg', 'ltv'],
	                    help='Action')
	parser.add_argument('-gen', action='store', dest='users_count', type=int, help='Number of generated users')
	main(parser.parse_args())

from impala.dbapi import connect
import psycopg2
import matplotlib.pyplot as plt
from matplotlib.pylab import style

style.use('ggplot')
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

if __name__ == "__main__":
  conn = connect(host='192.168.1.133', port=10000, database='province', auth_mechanism='PLAIN')
  cur = conn.cursor()
  cur.execute("select name, min(val) from province_tb group by name")
  lines = cur.fetchall()

  psqlConn = psycopg2.connect(dbname="hive_province", user="postgres", password="123456", host="192.168.1.133",
                              port="5432")
  psqlCur = psqlConn.cursor()
  names = []
  vals = []
  for line in lines:
    names += [line[0]]
    vals += [line[1]]
    psqlCur.execute("insert into min_val_tb(name, min_val) values ('%s', %d)" % (line[0], line[1]))
  psqlConn.commit()
  conn.close()
  psqlConn.close()

  plt.bar(range(len(vals)), vals, color='rgb', tick_label=names)
  plt.show()

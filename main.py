import email
import glob
import imaplib
import os
import sqlite3
import time
from datetime import datetime

import jinja2

import numpy as np
import pandas as pd

WWW = 'WWW'

cnx = sqlite3.connect('ctot.sqlite')
ptime = 0


def exist_db(cur, cs):
    r = cur.execute("SELECT * FROM CTOT WHERE callsign= ?", (cs,))

    if len(r.fetchall()) > 0:
        return True


def push_db(cs, dep, des, ts):
    cur = cnx.cursor()

    if exist_db(cur, cs):
        SQL = f"UPDATE CTOT SET ctot = ?, updated = ? where callsign = '{cs}'"
        # print(SQL)
        cur.execute(SQL, (ts, '1'))
    else:
        SQL = f"INSERT INTO CTOT(callsign,dep,des,ctot,updated) VALUES('{cs}', '{dep}', '{des}', '{ts}', '0')"
        # print(SQL)
        cur.execute(SQL)

    cnx.commit()


def convert(file):
    df = pd.read_excel(file)
    df = df.drop(['EOBT', 'CLDT'], axis=1)
    df['ts'] = df.CTOT.values.astype(np.int64) // 10 ** 9

    global ptime
    ptime = datetime.utcnow().strftime('%H%MZ')

    for i, row in df.iterrows():
        push_db(row['ACID'], row['ADEP'], row['ADES'], row['ts'])
        # print(row['ACID'], row['ADEP'], row['ADES'], row['ts'])


def render(render_vars, input_fn, output_fn):
    template = f"{WWW}/{input_fn}"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('.'))
    html = env.get_template(template).render(render_vars)

    with open(f'{WWW}/{output_fn}', 'w')as f:
        f.write(html)


def db_2html():
    df = pd.read_sql_query("SELECT * FROM ctot", cnx)
    df.sort_values(by=['ctot'], inplace=True)
    print(df)

    key = 0
    tr_html = ''
    for i, row in df.iterrows():
        key += 1
        ctot = datetime.fromtimestamp(row['ctot']).strftime('%H%MZ')
        ctot_update = ''
        remark = ''
        updated = ''

        ctot_minus5 = row['ctot'] - 300  # 5*60
        ctot_minus5_str = datetime.fromtimestamp(ctot_minus5).strftime('%H%MZ')
        ctot_plus10 = row['ctot'] + 600  # 10*60
        ctot_plus10_str = datetime.fromtimestamp(ctot_plus10).strftime('%H%MZ')
        window = f'{ctot_minus5_str}-{ctot_plus10_str}'

        if row['updated']:
            ctot_update = ctot
            ctot = 'XXXX'
            updated = 'color:red'
            remark = 'CTOT AMENDED'

        callsign = row['callsign']
        dep = row['dep']
        des = row['des']
        # print(key, callsign, ctot, ctot_update, window, des)

        tr_html += f"""
            <tr style='text-align:center;{updated}'>
                <td>{key}</td>
                <td>{callsign}</td>
                <td>{dep}</td>
                <td>{ctot}</td>
                <td>{ctot_update}</td>
                <td>{window}</td>
                <td>{des}</td>
                <td>{remark}</td>
            </tr>
        """
        # print(tr_html)

    render_vars = {
        "table": tr_html,
        "update_time": ptime,
    }
    render(render_vars, 'template.html', 'index.html')


def read_email():
    ms = imaplib.IMAP4_SSL('imap.gmail.com', 993)
    ms.login('iitzexself@gmail.com', 'tacc@1234')
    status, count = ms.select('Inbox')
    print(status, count)

    try:
        resp, mails = ms.search(None, 'ALL')
        resp, data = ms.fetch(
            mails[0].split()[len(mails[0].split())-1], '(RFC822)')
        emailbody = data[0][1]
        mail = email.message_from_bytes(emailbody)
        # print(mail)
    except IndexError:
        return

    fn = ''

    for part in mail.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fn = part.get_filename()

        if fn != '':
            fp = os.path.join("source/", fn)

            if not os.path.isfile(fp):
                with open(fp, 'wb') as f:
                    f.write(part.get_payload(decode=True))
                    print("Download:" + fn)

                print(fp)
                if 'TAIPEI' in fp:
                    convert(fp)
        else:
            print("Exist:" + fn)

    ms.close()
    ms.logout()


def test():
    files = glob.glob('source/*.xlsx')
    for fn in sorted(files):
        print(fn)
        convert(fn)
        print('--')

    db_2html()


if __name__ == "__main__":
    # test()
    while True:
        read_email()
        db_2html()

        time.sleep(10)

    cnx.close()

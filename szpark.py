# pylint: disable=C0103,R0912,R0915,C0301,R0914,R0902
# pylint: disable=no-member
"""SZ Parking Service module"""
import socket
import time
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import configparser
from datetime import datetime, timedelta
import threading
import requests
import serial
from flask import Flask, request, redirect
from flask_httpauth import HTTPBasicAuth
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import win32serviceutil
import win32service
import win32event
import servicemanager

# result constants
R_OK = 0
R_FAIL_ONLINE = 1
R_FAIL_TIME = 2
R_FAIL_FN = 4
R_FAIL_TYPE = 8
R_FAIL_MULTI = 16

# flask app for web inerface
app = Flask(__name__)
auth = HTTPBasicAuth()
users = {
}
pc_lock = threading.RLock()

@auth.get_password
def get_pw(username):
    """HTTP basic auth callback (check password)"""
    if username in users:
        return users.get(username)
    return None

# global variable
g_cfg = {}

@app.route('/', methods=['POST', 'GET'])
@auth.login_required
def www_root():
    """Function for Web Interface (status page)"""
    tpl = """
    <html>
    <table>
    <tr><td>watchdog thread:</td><td>{}</td></tr>
    <tr><td>scanning thread:</td><td>{}</td></tr>
    <tr><td>parking counter thread:</td><td>{}</td></tr>
    <tr><td>parking counter value:</td><td>{}</td></tr>
    <tr><td>remaining parking spaces:</td><td>{}</td></tr>
    <tr><td valign="top">new parking counter value:</td>
    <td><form action="/" method="post">
        <input name="count" type="text" size="2">
        <input type="submit" value="ok"/>
    </form></td><tr>
    <tr><td><a href="/base">base page</a></td><td><a href="/log">log page</a></td></tr>
    <tr><td><a href="/opensesame">open parking</a></td><td><a href="/">refresh</a></td></tr>
    </table>
    </html>
    """
    if request.method == 'POST':
        pc_new_value = int(request.form['count'])
        g_cfg['pc'] = pc_new_value
        update_pc()
        pc_reset()
    if g_cfg['watchdog']:
        if g_cfg['th_watch'].is_alive():
            watch_status = '<font color="#00AA00">alive</font>'
        else:
            watch_status = '<font color="#AA0000">dead</font>'
    else:
        watch_status = '<font color="#AAAAAA">disabled</font>'
    if g_cfg['th_scan'].is_alive():
        scan_status = '<font color="#00AA00">alive</font>'
    else:
        scan_status = '<font color="#AA0000">dead</font>'
    if g_cfg['pc_enable']:
        if g_cfg['th_pc'].is_alive():
            pc_status = '<font color="#00AA00">alive</font>'
        else:
            pc_status = '<font color="#AA0000">dead</font>'
        pc_value = g_cfg['pc']
        pc_remaining = g_cfg['pc_capacity'] - pc_value
    else:
        pc_status = '<font color="#AAAAAA">disabled</font>'
        pc_value = 0
        pc_remaining = 0
    return tpl.format(watch_status, scan_status, pc_status, pc_value, pc_remaining)

@app.route('/base', methods=['POST', 'GET'])
@auth.login_required
def www_base():
    """Function for Web Interface (base page)"""
    result_ok = ''
    multiple_use = ''
    time_exceed = ''
    online_failed = ''
    invalid_type = ''
    invalid_fn = ''
    ch_sum = ''
    ch_fn = ''
    ch_fd = ''
    ch_fp = ''
    ch_t = ''
    date1 = ''
    date2 = ''
    if request.method == 'POST':
        if request.form.get('result_ok'):
            result_ok = 'checked'
        if request.form.get('multiple_use'):
            multiple_use = 'checked'
        if request.form.get('time_exceed'):
            time_exceed = 'checked'
        if request.form.get('online_failed'):
            online_failed = 'checked'
        if request.form.get('invalid_type'):
            invalid_type = 'checked'
        if request.form.get('invalid_fn'):
            invalid_fn = 'checked'
        ch_sum = request.form['ch_sum']
        if (ch_sum[-3:-2] != '.') and (ch_sum != ''):
            ch_sum = ch_sum + '.00'
        date1 = request.form['date1']
        date2 = request.form['date2']
        ch_fn = request.form['ch_fn']
        ch_fd = request.form['ch_fd']
        ch_fp = request.form['ch_fp']
        ch_t = request.form['ch_t']
    dt1 = ''
    try:
        dt1 = datetime.strptime(date1 + ' 00:00', '%Y-%m-%d %H:%M')
    except ValueError:
        pass
    dt2 = ''
    try:
        dt2 = datetime.strptime(date2 + ' 23:59', '%Y-%m-%d %H:%M')
    except ValueError:
        pass
    html = """
    <html>
    <table border=1">
    <th>num</th>
    <th>date</th>
    <th>result</th>
    <th>check date</th>
    <th>check sum</th>
    <th>check fn</th>
    <th>check fd</th>
    <th>check fp</th>
    <th>check type</th>
    <tr><td><form action="/base" method="post"></td>
    <td>
    <input type="date" name="date1" size="2" value={}> - <input type="date" name="date2" size="2" value={}>
    </td>
    <td>
    <input type="checkbox" name="result_ok" id="id_result_ok" {}><label for="id_result_ok">ok</label>
    <input type="checkbox" name="multiple_use" id="id_multiple_use" {}><label for="id_multiple_use">multiple use</label>
    <input type="checkbox" name="time_exceed" id="id_time_exceed" {}><label for="id_time_exceed">time exceed</label>    
    <input type="checkbox" name="online_failed" id="id_online_failed" {}><label for="id_online_failed">online failed</label>
    <input type="checkbox" name="invalid_type" id="id_invalid_type" {}><label for="id_invalid_type">invalid type</label>
    <input type="checkbox" name="invalid_fn" id="id_invalid_fn" {}><label for="id_invalid_fn">invalid fn</label>    
    </td>
    <td></td>
    <td><input name="ch_sum" type="text" size="6" value={}></td>
    <td><input name="ch_fn" type="text" value={}></td>
    <td><input name="ch_fd" type="text" size="4" value={}></td>
    <td><input name="ch_fp" type="text" size="7" value={}></td>
    <td><input name="ch_t" type="text" size="1" value={}>
    <input type="submit" value="ok"/></form></td></tr>""".format(
        date1, date2, result_ok, multiple_use, time_exceed, online_failed, invalid_type, invalid_fn, ch_sum, ch_fn, ch_fd, ch_fp, ch_t)
    dbfn = __file__.replace('.py', '.sqlite')
    conn = sqlite3.connect(dbfn)
    cursor = conn.cursor()
    num = 1
    sql = 'SELECT strftime("%d.%m.%Y %H:%M:%S", date) , result, strftime("%d.%m.%Y %H:%M:%S", ch_date), ch_sum, ch_fn, ch_fd, ch_fp, ch_t FROM cache'
    s_and = ''
    if str(dt1) + str(dt2) + result_ok + multiple_use + time_exceed + online_failed + invalid_type + invalid_fn + ch_sum + ch_fn + ch_fd + ch_fp + ch_t != '':
        sql = sql + ' WHERE'
        if result_ok == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_OK, s_and)
        if multiple_use == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_FAIL_MULTI, s_and)
        if time_exceed == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_FAIL_TIME, s_and)
        if online_failed == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_FAIL_ONLINE, s_and)
        if invalid_type == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_FAIL_TYPE, s_and)
        if invalid_fn == 'checked':
            sql, s_and = add_sql_param(sql, 'result', R_FAIL_FN, s_and)
        sql, s_and = add_sql_param(sql, 'ch_sum', ch_sum, s_and)
        sql, s_and = add_sql_param(sql, 'ch_fn', ch_fn, s_and)
        sql, s_and = add_sql_param(sql, 'ch_fd', ch_fd, s_and)
        sql, s_and = add_sql_param(sql, 'ch_fp', ch_fp, s_and)
        sql, s_and = add_sql_param(sql, 'ch_t', ch_t, s_and)
        sql, s_and = add_sql_param(sql, 'date', (dt1, dt2), s_and)

    sql = sql + ' ORDER BY datetime(date) DESC'
    for row in cursor.execute(sql):
        html = html + '<tr><td>'
        html = html + '{}</td><td>'.format(num)
        html = html + '{}</td><td>'.format(row[0])
        html = html + '{}</td><td>'.format(result_decode(row[1], True))
        html = html + '{}</td><td>'.format(row[2])
        html = html + '{}</td><td>'.format(row[3])
        html = html + '{}</td><td>'.format(row[4])
        html = html + '{}</td><td>'.format(row[5])
        html = html + '{}</td><td>'.format(row[6])
        html = html + '{}</td></tr>'.format(row[7])
        num = num + 1
    cursor.close()
    conn.close()
    html = html + '<table><html>'
    return html

def add_sql_param(sql, param_name, param_value, s_and):
    """Add filter by param value to sql"""
    if param_value != '':
        if param_name == 'date':
            if str(param_value[0]) + str(param_value[1]) != '':
                if param_value[0] == '':
                    sql = '{}{} {} <= "{}"'.format(sql, s_and, param_name, param_value[1])
                else:
                    if param_value[1] == '':
                        sql = '{}{} {} >= "{}"'.format(sql, s_and, param_name, param_value[0])
                    else:
                        sql = '{}{} {} BETWEEN "{}" AND "{}"'.format(sql, s_and, param_name, param_value[0], param_value[1])
        else:
            if (param_name == 'result') and (param_value != R_OK):
                sql = '{}{} {} & {} <> 0'.format(sql, s_and, param_name, param_value)
            else:
                sql = '{}{} {} = "{}"'.format(sql, s_and, param_name, param_value)
        if s_and == '':
            s_and = ' AND'
    return sql, s_and

@app.route('/log')
@auth.login_required
def www_log():
    """Function for Web Interface (log page)"""
    html = '<html><pre>'
    f_log = open(__file__.replace('.py', '.log'), mode='r')
    lines = f_log.readlines()
    lines.reverse()
    for line in lines:
        html = html + line
    f_log.close()
    html = html + '</pre></html>'
    return html

@app.route('/opensesame')
@auth.login_required
def www_open():
    """Function for Web Interface (open parking)"""
    logging.info("WWW: opening parking")
    th_open = threading.Thread(target=open_parking, args=())
    th_open.start()
    return redirect('/')

def result_decode(result, html=False):
    """Decoding integer result to description string"""
    if html:
        result_s = '<font color="#00AA00">OK</font>'
    else:
        result_s = 'OK'
    if result != R_OK:
        if html:
            result_s = '<font color="#AA0000">FAIL</font>'
        else:
            result_s = 'FAIL:'
        if result & R_FAIL_MULTI:
            result_s = result_s + ' | multiple use'
        if result & R_FAIL_TIME:
            result_s = result_s + ' | time exceed'
        if result & R_FAIL_ONLINE:
            result_s = result_s + ' | online failed'
        if result & R_FAIL_TYPE:
            result_s = result_s + ' | invalid type'
        if result & R_FAIL_FN:
            result_s = result_s + ' | invalid fn'
    return result_s

def open_com():
    """Open serial port, retrying on failure"""
    nf = 0
    while True:
        try:
            g_cfg['serial'] = serial.Serial(
                g_cfg['com'],
                timeout=None,
                baudrate=g_cfg['speed'],
                xonxoff=False,
                rtscts=False,
                dsrdtr=False)
            g_cfg['serial'].isOpen()
            logging.info('Open ' + g_cfg['com'])
            break
        except serial.SerialException:
            time.sleep(g_cfg['scan_interval'])
            nf = nf + 1
            if nf == 1:
                logging.info('Failed to open ' + g_cfg['com'])
            break

def read_cfg():
    """Read config"""
    config = configparser.ConfigParser()
    config.read(g_cfg['fn'], encoding='utf-8-sig')
    logger = logging.getLogger()
    logger.setLevel(config.getint('szpark', 'log_level'))
    handler = RotatingFileHandler(
        __file__.replace('.py', '.log'),
        maxBytes=config.getint('szpark', 'log_size'),
        backupCount=config.getint('szpark', 'log_num'))
    formatter = logging.Formatter('%(asctime)-15s %(levelname)-7.7s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    g_cfg['com'] = config.get('szpark', 'com')
    g_cfg['speed'] = config.get('szpark', 'speed')
    g_cfg['online'] = config.getboolean('szpark', 'online')
    g_cfg['mbcip'] = config.get('szpark', 'mbcip')
    g_cfg['mbcport'] = config.getint('szpark', 'mbcport')
    g_cfg['mbccoil'] = config.getint('szpark', 'mbccoil')
    g_cfg['mbctime'] = config.getfloat('szpark', 'mbctime')
    g_cfg['onlinefound'] = config.get('szpark', 'onlinefound')
    g_cfg['multiple'] = config.getboolean('szpark', 'multiple')
    g_cfg['interval'] = timedelta(seconds=config.getint('szpark', 'interval'))
    g_cfg['scan_interval'] = config.getint('szpark', 'scan_interval')
    g_cfg['pc_capacity'] = config.getint('szpark', 'pc_capacity')
    g_cfg['fns'] = []
    nfn = 1
    while True:
        try:
            g_cfg['fns'].append(config.get('szpark', 'fn{}'.format(nfn)))
            nfn = nfn + 1
        except configparser.NoOptionError:
            break
    # parking counter options
    g_cfg['pc_enable'] = config.getboolean('szpark', 'pc_enable')
    g_cfg['mbcreg_init_in'] = config.getint('szpark', 'mbcreg_init_in')
    g_cfg['mbcreg_init_out'] = config.getint('szpark', 'mbcreg_init_out')
    g_cfg['mbccoil_save'] = config.getint('szpark', 'mbccoil_save')
    g_cfg['mbccoil_reset_in'] = config.getint('szpark', 'mbccoil_reset_in')
    g_cfg['mbccoil_reset_out'] = config.getint('szpark', 'mbccoil_reset_out')
    g_cfg['mbcreg_in'] = config.getint('szpark', 'mbcreg_in')
    g_cfg['mbcreg_out'] = config.getint('szpark', 'mbcreg_out')
    g_cfg['pc_init'] = config.getint('szpark', 'pc_init')
    g_cfg['pc_interval'] = config.getint('szpark', 'pc_interval')
    # web interface options
    g_cfg['www_addr'] = config.get('szpark', 'www_addr')
    g_cfg['www_port'] = config.getint('szpark', 'www_port')
    g_cfg['www_login'] = config.get('szpark', 'www_login')
    g_cfg['www_pass'] = config.get('szpark', 'www_pass')
    users[g_cfg['www_login']] = g_cfg['www_pass']
    # watchdog options
    g_cfg['watchdog'] = config.getboolean('szpark', 'watchdog')
    g_cfg['watch_interval'] = config.getint('szpark', 'watch_interval')

def pc_reset():
    """Reset parking counters"""
    mbc = ModbusClient(g_cfg['mbcip'], port=g_cfg['mbcport'])
    with pc_lock:
        if mbc.connect():
            mbc.write_register(g_cfg['mbcreg_init_in'], g_cfg['pc'], unit=1)
            mbc.write_register(g_cfg['mbcreg_init_in'] + 1, 0, unit=1)
            mbc.write_register(g_cfg['mbcreg_init_out'], 0, unit=1)
            mbc.write_register(g_cfg['mbcreg_init_out'] + 1, 0, unit=1)
            mbc.write_coil(g_cfg['mbccoil_save'], 1, unit=1)
            mbc.write_coil(g_cfg['mbccoil_reset_in'], 1, unit=1)
            mbc.write_coil(g_cfg['mbccoil_reset_in'] + 1, 1, unit=1)
            mbc.write_coil(g_cfg['mbccoil_reset_out'], 1, unit=1)
            mbc.write_coil(g_cfg['mbccoil_reset_out'] + 1, 1, unit=1)
            mbc.close()

def update_pc():
    """Update parking counter value in db"""
    dbfn = __file__.replace('.py', '.sqlite')
    conn = sqlite3.connect(dbfn)
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM pc')
    result = cursor.fetchone()
    if result:
        pc_o = result[0]
        cursor.execute('UPDATE pc SET value=? WHERE value=?', (g_cfg['pc'], pc_o))
    else:
        cursor.execute('INSERT INTO pc(value) VALUES (?)', (g_cfg['pc'],))
    conn.commit()
    cursor.close()
    conn.close()

def open_parking():
    """Open parking"""
    mbc = ModbusClient(g_cfg['mbcip'], port=g_cfg['mbcport'])
    with pc_lock:
        if mbc.connect():
            mbc.write_coil(g_cfg['mbccoil'], True, unit=1)
            time.sleep(g_cfg['mbctime'])
            mbc.write_coil(g_cfg['mbccoil'], False, unit=1)
            mbc.close()

def watch_th_fn():
    """Watchdog thread function"""
    while True:
        if not g_cfg['th_scan'].is_alive():
            logging.info('Scan thread restarted')
            g_cfg['th_scan'] = threading.Thread(target=scan_th_fn, args=())
            g_cfg['th_scan'].start()
        if g_cfg['pc_enable']:
            if not g_cfg['th_pc'].is_alive():
                logging.info('Parking counter thread restarted')
                g_cfg['th_pc'] = threading.Thread(target=pc_th_fn, args=())
                g_cfg['th_pc'].start()
        time.sleep(g_cfg['watch_interval'])

def pc_th_fn():
    """Thread function for parking counter"""
    pc_reset()
    mbc = ModbusClient(g_cfg['mbcip'], port=g_cfg['mbcport'])
    while True:
        with pc_lock:
            if mbc.connect():
                result = mbc.read_input_registers(g_cfg['mbcreg_in'], 1, unit=1)
                cnt_in = result.getRegister(0)
                result = mbc.read_input_registers(g_cfg['mbcreg_out'], 1, unit=1)
                cnt_out = result.getRegister(0)
                mbc.close()
                if g_cfg['pc'] != cnt_in - cnt_out:
                    g_cfg['pc'] = cnt_in - cnt_out
                    logging.info('Updating parking counter: %d', g_cfg['pc'])
                    update_pc()
        mbc.close()
        time.sleep(g_cfg['pc_interval'])

def scan_th_fn():
    """Thread function for check scan module"""
    open_com()
    dbfn = __file__.replace('.py', '.sqlite')
    conn = sqlite3.connect(dbfn)
    cursor = conn.cursor()
    while True:
        data = ''
        try:
            while g_cfg['serial'].inWaiting() > 0:
                try:
                    data += g_cfg['serial'].read(1).decode("utf-8")
                except UnicodeDecodeError:
                    pass
        except serial.SerialException:
            open_com()
        if data != '':
            data = data.strip()
            logging.info('Reading raw data:%s', data)
            result = R_OK
            # decoding string
            data_s = data.split('&')
            if len(data_s) != 6:
                continue
            try:
                ch_date = datetime.strptime(data_s[0][:15], 't=%Y%m%dT%H%M')
            except ValueError:
                ch_date = ''
            ch_sum = data_s[1].replace('s=', '')
            ch_fn = data_s[2].replace('fn=', '')
            ch_fd = data_s[3].replace('i=', '')
            ch_fp = data_s[4].replace('fp=', '')
            ch_t = int(data_s[5].replace('n=', ''))
            logging.info('Decoding check: date:%s sum:%s fn:%s fd:%s fp:%s type:%s',
                         ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t)
            date = datetime.now()
            # checking multiple use
            if not g_cfg['multiple']:
                cursor.execute('SELECT * FROM cache WHERE data = ?', (data,))
                if cursor.fetchone() != None:
                    result = result + R_FAIL_MULTI
            # checking time interval
            if (date - ch_date) > g_cfg['interval']:
                result = result + R_FAIL_TIME
            # checking online
            if g_cfg['online']:
                payload = {'fp': ch_fp, 's': ch_sum}
                r = requests.get('http://receipt.taxcom.ru/v01/show', params=payload)
                if g_cfg['onlinefound'] in r.text:
                    pass
                else:
                    result = result + R_FAIL_ONLINE
            # checking type
            if ch_t != 1:
                result = result + R_FAIL_TYPE
            # checking fn
            if ch_fn not in g_cfg['fns']:
                result = result + R_FAIL_FN
            # store in base
            cursor.execute('INSERT INTO cache(data, date, result, ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t)\
                VALUES (?,?,?,?,?,?,?,?,?)', (data, date, result, ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t))
            conn.commit()
            # log result
            if result != R_OK:
                logging.info(result_decode(result))
            if result == R_OK:
                logging.info("OK: opening parking")
                th_open = threading.Thread(target=open_parking, args=())
                th_open.start()
        time.sleep(g_cfg['scan_interval'])
    cursor.close()
    conn.close()
    g_cfg['serial'].close()

def init():
    """Init data and main threads"""
    g_cfg['fn'] = __file__.replace('.py', '.ini')
    read_cfg()
    dbfn = __file__.replace('.py', '.sqlite')
    conn = sqlite3.connect(dbfn)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS cache(
        data TEXT,
        date TEXT,
        result INT,
        ch_date TEXT,
        ch_sum TEXT,
        ch_fn TEXT,
        ch_fd TEXT,
        ch_fp TEXT,
        ch_t INT)""")
    cursor.execute('CREATE INDEX IF NOT EXISTS data ON cache (data)')
    cursor.execute('CREATE TABLE IF NOT EXISTS pc(value INT)')
    conn.commit()
    g_cfg['th_scan'] = threading.Thread(target=scan_th_fn, args=())
    g_cfg['th_scan'].start()
    if g_cfg['pc_enable']:
        # init parking counter from ini file if does not exist data in db
        cursor.execute('SELECT value FROM pc')
        result = cursor.fetchone()
        if result:
            g_cfg['pc'] = result[0]
        else:
            g_cfg['pc'] = g_cfg['pc_init']
        g_cfg['th_pc'] = threading.Thread(target=pc_th_fn, args=())
        g_cfg['th_pc'].start()
    if g_cfg['watchdog']:
        g_cfg['th_watch'] = threading.Thread(target=watch_th_fn, args=())
        g_cfg['th_watch'].start()
    cursor.close()
    conn.close()

class SZParkSvc(win32serviceutil.ServiceFramework):
    """SZ Parking Service main class"""
    _svc_name_ = "SZParkSvc"
    _svc_display_name_ = "SZ Parking Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        """Stop service method"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        logging.info('Stopping service')
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        """Main do run service method"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        init()
        logging.info('Starting service')
        app.run(host=g_cfg['www_addr'], port=g_cfg['www_port'])

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SZParkSvc)

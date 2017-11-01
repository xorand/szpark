# pylint: disable=C0103,R0912,R0915,C0301,R0914
# pylint: disable=no-member
"""SZ Parking Service module v0.1a"""
import socket
import time
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import configparser
from datetime import datetime, timedelta
import requests
import serial
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

class SZParkSvc(win32serviceutil.ServiceFramework):
    """SZ Parking Service main class"""
    _svc_name_ = "SZParkSvc"
    _svc_display_name_ = "SZ Parking Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.stop_requested = False
        self.cfg = {}
        self.cfg['fn'] = __file__.replace('.py', '.ini')
        self.read_cfg()
        self.com = None
        dbfn = __file__.replace('.py', '.sqlite')
        self.conn = sqlite3.connect(dbfn)
        cursor = self.conn.cursor()
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
        cursor.close()

    def SvcStop(self):
        """Stop service method"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        logging.info('Stopping service')
        self.stop_requested = True

    def SvcDoRun(self):
        """Main do run service method"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()

    def open_com(self):
        """Open serial port, retrying every 5 seconds on failure"""
        nf = 0
        while True:
            try:
                self.com = serial.Serial(
                    self.cfg['com'],
                    timeout=None,
                    baudrate=self.cfg['speed'],
                    xonxoff=False,
                    rtscts=False,
                    dsrdtr=False)
                self.com.isOpen()
                logging.info('Open ' + self.cfg['com'])
                break
            except serial.SerialException:
                time.sleep(5)
                nf = nf + 1
                if nf == 1:
                    logging.info('Failed to open ' + self.cfg['com'])
                if self.stop_requested:
                    logging.info('Stop requesting')
                break

    def read_cfg(self):
        """Read config"""
        config = configparser.ConfigParser()
        config.read(self.cfg['fn'])
        logger = logging.getLogger()
        logger.setLevel(config.getint('szpark', 'log'))
        handler = RotatingFileHandler(
            __file__.replace('.py', '.log'),
            maxBytes=1048576,
            backupCount=2)
        formatter = logging.Formatter('%(asctime)-15s %(levelname)-7.7s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.cfg = {}
        self.cfg['com'] = config.get('szpark', 'com')
        self.cfg['speed'] = config.get('szpark', 'speed')
        self.cfg['online'] = config.getboolean('szpark', 'online')
        self.cfg['mbcip'] = config.get('szpark', 'mbcip')
        self.cfg['mbcport'] = config.getint('szpark', 'mbcport')
        self.cfg['mbccoil'] = config.getint('szpark', 'mbccoil')
        self.cfg['mbctime'] = config.getfloat('szpark', 'mbctime')
        self.cfg['onlinefound'] = config.get('szpark', 'onlinefound')
        self.cfg['multiple'] = config.getboolean('szpark', 'multiple')
        self.cfg['interval'] = timedelta(seconds=config.getint('szpark', 'interval'))
        self.cfg['fns'] = []
        nfn = 1
        while True:
            try:
                self.cfg['fns'].append(config.get('szpark', 'fn{}'.format(nfn)))
                nfn = nfn + 1
            except configparser.NoOptionError:
                break

    def main(self):
        """Main program"""
        logging.info('Starting service')
        self.open_com()
        time.sleep(1)
        cursor = self.conn.cursor()
        while True:
            if self.stop_requested:
                logging.info('Stop requesting')
                break
            data = ''
            try:
                while self.com.inWaiting() > 0:
                    try:
                        data += self.com.read(1).decode("utf-8")
                    except UnicodeDecodeError:
                        pass
            except serial.SerialException:
                self.open_com()
            if data != '':
                data = data.replace('\n', '')
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
                if not self.cfg['multiple']:
                    cursor.execute('SELECT * FROM cache WHERE data = ?', (data,))
                    if cursor.fetchone() != None:
                        result = result + R_FAIL_MULTI
                # checking time interval
                if (date - ch_date) > self.cfg['interval']:
                    result = result + R_FAIL_TIME
                # checking online
                if self.cfg['online']:
                    payload = {'fp': ch_fp, 's': ch_sum}
                    r = requests.get('http://receipt.taxcom.ru/v01/show', params=payload)
                    if self.cfg['onlinefound'] in r.text:
                        pass
                    else:
                        result = result + R_FAIL_ONLINE
                # checking type
                if ch_t != 1:
                    result = result + R_FAIL_TYPE
                # checking fn
                if ch_fn not in self.cfg['fns']:
                    result = result + R_FAIL_FN
                # store in base
                cursor.execute('INSERT INTO cache(data, date, result, ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t)\
                    VALUES (?,?,?,?,?,?,?,?,?)', (data, date, result, ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t))
                self.conn.commit()
                # log result
                if result != R_OK:
                    result_s = 'FAIL:'
                    if result & R_FAIL_MULTI:
                        result_s = result_s + ' | multiple use'
                    if result & R_FAIL_TIME:
                        result_s = result_s + ' | time interval exceed'
                    if result & R_FAIL_ONLINE:
                        result_s = result_s + ' | online check failed'
                    if result & R_FAIL_TYPE:
                        result_s = result_s + ' | invalid check time'
                    if result & R_FAIL_FN:
                        result_s = result_s + ' | invalid fn'
                    logging.info(result_s)
                if result == R_OK:
                    logging.info("OK: opening parking")
                    mbc = ModbusClient(self.cfg['mbcip'], port=self.cfg['mbcport'])
                    if mbc.connect():
                        mbc.write_coil(self.cfg['mbccoil'], True, unit=1)
                        time.sleep(self.cfg['mbctime'])
                        mbc.write_coil(self.cfg['mbccoil'], False, unit=1)
                        mbc.close()

            time.sleep(1)
        cursor.close()
        self.com.close()
        self.conn.close()
        return

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SZParkSvc)

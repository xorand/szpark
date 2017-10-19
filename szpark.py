import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
import logging
import serial
import configparser
from datetime import datetime
import requests

logging.basicConfig(
    filename = __file__.replace('.py' , '.log'),
    level = logging.DEBUG, 
    format = '%(asctime)-15s %(levelname)-7.7s %(message)s'
)

class SZParkSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "SZParkSvc"
    _svc_display_name_ = "SZ Parking Service"
    
    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.stop_event = win32event.CreateEvent(None,0,0,None)
        socket.setdefaulttimeout(60)
        self.stop_requested = False

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        logging.info('Stopping service')
        self.stop_requested = True

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_,'')
        )
        self.main()

    def open_com(self):
        nf = 0
        while True:
            try:
                self.com = serial.Serial(self.cfg['com'], timeout=None, baudrate=self.cfg['speed'], xonxoff=False, rtscts=False, dsrdtr=False)
                self.com.isOpen()
                logging.info('Open ' + self.cfg['com'])
                break
            except:
                time.sleep(5)
                nf = nf + 1
                if nf==1:
                    logging.info('Failed to open ' + self.cfg['com'])        

    def read_cfg(self):
        logging.info('Reading config from ' + self.cfg['fn'])
        config = configparser.ConfigParser()
        config.read(self.cfg['fn'])
        self.cfg = {}
        self.cfg['com'] = config.get('szpark', 'com')
        self.cfg['speed'] = config.get('szpark', 'speed')
        self.cfg['online'] = config.getboolean('szpark', 'online')
        self.cfg['fns'] = []
        nfn = 1
        while True:
            try:
                self.cfg['fns'].append(config.get('szpark', 'fn{}'.format(nfn)))
                nfn = nfn + 1                
            except:
                break
        
    def main(self):
        logging.info('Starting service')
        self.cfg = {}
        self.cfg['fn'] =__file__.replace('.py' , '.ini')
        self.read_cfg()
        self.open_com()
        time.sleep(1)
        while True:

            if self.stop_requested:
                logging.info('Stop requesting')
                break

            data = ''
            try:
                while self.com.inWaiting() > 0:
                    data += self.com.read(1).decode("utf-8")
            except:                
                self.open_com()

            if data != '':
                logging.info('Reading raw data:{}'.format(data))
                data = data.split('&')
                if len(data)!=6:                    
                    continue
                try:
                    try:
                        ch_date = datetime.strptime(data[0][:15], 't=%Y%m%dT%H%M')
                    except:
                        ch_date = 'not valid date'
                    ch_sum  = data[1].replace('s=', '')
                    ch_fn   = data[2].replace('fn=', '')
                    ch_fd   = data[3].replace('i=', '')
                    ch_fp   = data[4].replace('fp=', '')
                    ch_t    = int(data[5].replace('n=', ''))
                    logging.info('Decoding check: date:{} sum:{} fn:{} fd:{} fp:{} type:{}'.format(ch_date, ch_sum, ch_fn, ch_fd, ch_fp, ch_t))
                    if self.cfg['online']:
                        payload = {'fp': ch_fp, 's': ch_sum}
                        r = requests.get('http://receipt.taxcom.ru/v01/show', params=payload)
                        if 'Такой чек не найден' in r.text:
                            logging.info('Online check not found')
                        else:
                            logging.info('Online check found')
                    if ch_t != 1:
                        logging.info('Invalid check type, check failed')
                        continue
                    if ch_fn not in self.cfg['fns']:
                        logging.info('Not valid fn, check failed')
                        continue                    
                except:                    
                    continue
                
            time.sleep(1)
            
        self.com.close()
        return

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SZParkSvc)

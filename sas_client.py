# This is python wrapper for calc_exposure.sas

import saspy
import concurrent.futures as confu


class Concurrent(object):
    def __init__(self, sas_codes, sas_omruser, sas_omrpw):
        self.sas_codes = sas_codes
        self.sas_omruser = sas_omruser
        self.sas_omrpw = sas_omrpw


    def calc(self):
        with confu.ProcessPoolExecutor() as executor:
        # with confu.ThreadPoolExecutor() as executor:
            futures = []
            for sas_code in self.sas_codes:
                futures.append(executor.submit(execute_sas_code, sas_code, self.sas_omruser, self.sas_omrpw))

            for future in confu.as_completed(futures):
                print(future.result())


#  並行(process based)実行される関数はpickableでなければいけない。moduleのtop-levelであることはpickableの必要条件
def execute_sas_code(sas_code, omruser, omrpw, encoding='utf8'):
    try:
        sas_session = saspy.SASsession(cgfname='winiomlinux', omruser=omruser, omrpw=omrpw)
        sas_session._io.sascfg.encoding = encoding
    except Exception as e:
        print(e)
        raise Exception("connecting to sas server failed. if you fail entering password 3 times, your account will be locked.")
    result = sas_session.submit(sas_code)
    return result['LOG']
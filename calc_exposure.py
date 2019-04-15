# This is python wrapper for calc_exposure.sas

import saspy
import concurrent.futures as confu


class SasConcurrent(object):
    def __init__(self, sas_codes, sas_omruser, sas_omrpw):
        self.sas_codes = sas_codes
        self.sas_omruser = sas_omruser
        self.sas_omrpw = sas_omrpw


    def calc(self):
        with confu.ProcessPoolExecutor() as executor:
            futures = []
            for sas_code in self.sas_codes:
                futures.append(executor.submit(process, sas_code, self.sas_omruser, self.sas_omrpw))

            for future in confu.as_completed(futures):
                print(future.result())


def process(sas_code, omruser, omrpw):
    try:
        sas_session = saspy.SASsession(cgfname='winiomlinux', omruser=omruser, omrpw=omrpw)
    except Exception as e:
        print(e)
        raise Exception("connecting to sas server failed")
    # sas_session._io.sascfg.encoding='utf8'
    result = sas_session.submit(sas_code)
    return result['LOG']
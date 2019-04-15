# This is python wrapper for calc_exposure.sas

"""
# import saspy included in this module
import concurrent.futures as confu
From calc_exposure import CalcExposure

def process(sas_code):
    ce = CalcExposure(sas_code)
    ce.calc()

with confu.ProcessPoolExecutor() as executor:
    # futures = [executor.submit(process, yyyy, f'sas_code_dummy_{yyyy}') for yyyy in range(2010, 2020)]
    futures = []
    for yyyy in range(2010, 2020):
        sas_code = f"""
            {yyyy}
        """
        futures.append(executor.submit(process, yyyy, f'sas_code'))

    for future in confu.as_completed(futures):
        print(future.result())
"""

import saspy

class CalcExposure(object):
    material_path = ''
    output_path = ''
    def __init__(self, name, sas_code):
        self.name = name
        self.sas = saspy.SASsession(cgfname='winiomlinux', omruser='watkoji', omrpw='s4rhfkxm/')
        self.sas_code = sas_code
        with open(f'./sample{name}', mode='a') as f:
            f.write('hello')
    
    def calc(self):
        self.sas.submit(self.sas_code)

def process(name, sas_code):
    ce = CalcExposure(name, sas_code)
    ce.calc()

if __name__=='__main__':
    import concurrent.futures as confu
    with open(f'./start', mode='a') as f:
        f.write('hello')  # check starting

    with confu.ProcessPoolExecutor() as executor:
        # futures = [executor.submit(process, yyyy, f'sas_code_dummy_{yyyy}') for yyyy in range(2010, 2020)]
        futures = []
        for yyyy in range(2010, 2020):
            futures.append(executor.submit(process, yyyy, f'sas_code_dummy_{yyyy}'))

        for future in confu.as_completed(futures):
            print(future.result())
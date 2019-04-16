from sas_client import Concurrent
import getpass
import sys

class CalcExposureConcurrent(object):
    def __init__(self):
        self.start_month = ''
        self.stop_month = ''
        self.input_table = 'egp.exposure_material'
        self.output_table = 'egp.exposure'
        self.exps_start_date = 'exps_start_date'
        self.exps_end_date = 'exps_end_date'
        self.pol_start_dt = 'hist_first_issue_date'
        self.extended_end_date = 'extended_end_date'
        self.exps_amount = ''
        self.exps_amount2 = ''
        self.ex_from_count = ''
        self.obs_start_dt = ''
        self.obs_end_dt = ''
        self.event_dt = ''
        self.event_amount = ''
        self.event_amount2 = ''
        self.computed = ''
        self.computed_fields_used_by_grouping = ''
        self.properties = ''  # TODO: listで指定出来るようにする。


    def calc_exposure_parallel(
        self,
        egp_work_path:str,
        process_count = 10,
        ) -> None:

        # set base sas code
        with open('./calc_exposure.sas', mode='r', encoding='utf-8') as f:
            calc_exposure_code = f.read() + '\n'

        lib_setting_code = f"""
        libname egp base "{egp_work_path}";
        """

        # parameterize sas code
        sas_codes = []
        loop_count = 0
        for start_month, stop_month in self.__split(int(self.start_month), int(self.stop_month), process_count):
            output_table = f"{self.output_table}_{loop_count}"

            sas_code = calc_exposure_code + lib_setting_code
            sas_code += f"""
            %let input = {self.input_table};
            %let exps_start_dt = {self.exps_start_date};
            %let exps_end_dt = {self.exps_end_date};
            %let pol_start_dt = {self.pol_start_dt};
            %let extended_end_dt = {self.extended_end_date};
            %let exps_amount =  {self.exps_amount};
            %let exps_amount2 = {self.exps_amount2};
            %let ex_from_count = {self.ex_from_count};
            %let obs_start_dt = {self.obs_start_dt};
            %let obs_end_dt = {self.obs_end_dt};
            %let event_dt = {self.event_dt};
            %let event_amount = {self.event_amount};
            %let event_amount2 = {self.event_amount2};
            %let computed = {self.computed};
            %let computed_fields_used_by_grouping = {self.computed_fields_used_by_grouping};
            %let properties = {self.properties};
            %make_exposure_table(start_month = {start_month}, stop_month = {stop_month}, output={output_table})
            """

            sas_codes.append(sas_code)
            loop_count += 1
        assert(len(sas_codes)==process_count)

        sas_omruser = 'watkoji'
        sas_omrpw = getpass.getpass()
        ce = Concurrent(sas_codes, sas_omruser, sas_omrpw)
        ce.calc()
    

    def __complete_parameters(self):
        """
        userが設定したpropertyから指定されていないpropertyeを埋める。
        """
        pass


    def __split(self, start_month:int, stop_month:int, process_count): # List[(int, int)]: # List of [start_month, stop_month]
        all_months = []
        year = start_month//100
        month = start_month%100
        e_year = stop_month//100
        e_month = stop_month%100
        while(True):
            all_months.append((year*100 + month))
            if (year==e_year and month==e_month):
                break
            else:
                if (month!=12):
                    month += 1
                else:
                    year += 1
                    month = 1
        unit_size = len(all_months) // process_count
        remained = len(all_months) % process_count

        idx = 0
        res = []
        while(True):
            s = all_months[idx]
            eidx = idx+unit_size-1
            eidx += (1 if remained>0 else 0)
            if (eidx<len(all_months)):
                res.append((s, all_months[eidx]))
                idx += unit_size
                idx += (1 if remained>0 else 0)
                if (idx==len(all_months)):
                    break
            else:
                res.append((s, all_months[-1]))
                break
            remained -= 1
        print(res)
        return res

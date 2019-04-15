from calc_exposure import SasConcurrent
import getpass

if __name__ == "__main__":
    egp_work_path = ""

    with open('./calc_exposure.sas', mode='r', encoding='utf-8') as f:
        sas_code = f.read() + '\n'

    sas_code += f"""
    libname egp base "{egp_work_path}";
    """

    sas_code  = """
    /* %(year)s */
    /* %(output_name)s */

    /* for your main code */
    proc setinit;run;
    """

    sas_codes = []
    for year in range(2015, 2020):
        output_name = f'exposure_{year}'
        sas_codes.append(sas_code%{'year':year, 'output_name':output_name})

    sas_omruser = 'watkoji'
    sas_omrpw = getpass.getpass()
    ce = SasConcurrent(sas_codes, sas_omruser, sas_omrpw)
    ce.calc()
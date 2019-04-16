from calc_exposure_parallel import CalcExposureConcurrent


def main():  # 関数で定義して実行しないとエラーになる。(並行処理の制約)
    egp_work_path = '/saswork/default/SAS_workBE2C000191C5_azalvimpsasp03/SAS_workA36B000191C5_azalvimpsasp03'
    cec = CalcExposureConcurrent()

    cec.start_month = '201701'
    cec.stop_month = '201903'
    cec.input_table = 'egp.exp_material_prod'
    # cec.output_table = ''
    # cec.exps_start_date = ''
    # cec.exps_end_date = ''
    # cec.pol_start_dt = ''
    # cec.extended_end_date = ''
    cec.exps_amount = 'ifn(t1.dec_fa_date is not missing, t1.dec_ap, t1.annual_prem)'
    cec.exps_amount2 = 'ifn(t1.dec_fa_date is not missing, t1.normrized_dec_face_amt, t1.normrized_face_amt)'
    cec.ex_from_count = 'ifn(t1.dec_fa_date is not missing, 1, 0)'
    cec.obs_start_dt = 'mdy(1,1,2017)'
    cec.obs_end_dt = 'mdy(3,31,2019)'
    cec.event_dt = 'lapse_date'
    cec.event_amount = cec.exps_amount
    cec.event_amount2 = cec.exps_amount2
    cec.computed = """
    (year(term_start) - year(&exps_start_dt))*12 + (month(term_start) - month(&exps_start_dt)) + ifn(baflag='aa',1,0) as rpm /* record pm*/
    ,sum(&exps_amount2 * t1.exps_dur) as exps_dur_amount2
    ,sum(&exps_amount2 * t1.extended_dur) as extended_dur_amount2
    ,sum(ifn(t1.event_bit_in_term=1, &event_amount2, 0)) as event_amount2
    ,sum(ifn(t1.dec_fa_date is not missing, t1.event_bit_in_term, 0)) as partial_lapse_count
    ,sum(ifn(t1.dec_fa_date is not missing, t1.event_bit_in_term * &event_amount, 0)) as partial_lapse_amount
    ,sum(ifn(t1.dec_fa_date is not missing, t1.event_bit_in_term * &event_amount2, 0)) as partial_lapse_amount2
    ;
    """
    cec.computed_fields_used_by_grouping = 'rpm'
    cec.properties = """
        cy, cm, baflag, 
        t1.sex_code, t1.plan_code, t1.assumption_class, t1.study_class, t1.paid_up_age, t1.him, t1.is_hist_last, t1.sub_p_flag, t1.renewal_limit_age,
        t1.yrt_flag, t1.cov_term, t1.pre_cov_term, t1.hist_issue_age, t1.issue_age , t1.plan_term, t1.ip_ind, pm, inner_pm, py, py_start_dt, py_end_dt
    """

    cec.calc_exposure_parallel(
        egp_work_path=egp_work_path,
        process_count=12
    )

if __name__ == '__main__':
    import time
    start_time = time.perf_counter()

    main()

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    print(execution_time)
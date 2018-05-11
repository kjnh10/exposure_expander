exposure_expander
===============
make exposure-time series table from the latest snapshot

# default calculated fields
You can access to the calculated fields below which vary depending on specific terms.
* &c_year
* &c_month
* &CM
* &TERM_START
* &TERM_END
* &BAFLAG
* &MONTHIV_DT
* &EXP_DUR
* &PM
* &PY
* &FULLY_OBSERVED_PY

# Sample Setting
```SQL
%include "/mul/warehouse/jpnops/Experience_Analysis/library/2018/macros/make_exposure_table/ver0.10/calc_exposure_monthly.sas" /SOURCE2;

%let input = cache.EXP_MATERIAL;
%let EXPS_START_DT = t1.EXPS_START_DATE;
%let EXPS_END_DT = t1.EXPS_END_DATE;
%let EXTENDED_END_DT = t1.EXTENDED_END_DATE;  /* 観測対象のイベントが発生しているデータの観測期間単位の後ろまで伸ばした日 */
%let POL_START_DT = t1.HIST_FIRST_ISSUE_DATE;
%let OBS_START_DT = MDY(1, 1, 2015);
%let OBS_END_DT = MDY(9, 30, 2017);
%let EXPS_AMOUNT = IFN(t1.DEC_FA_DATE is not missing, t1.DEC_AP, t1.ANNUAL_PREM);
%let EX_FROM_COUNT = ifn(t1.DEC_FA_DATE is not missing, 1, 0);
%let EVENT_DT = t1.LAPSE_DATE;
%let EVENT_AMOUNT = &EXPS_AMOUNT;
%let computed = (year(&TERM_START) - year(&EXPS_START_DT))*12 + (month(&TERM_START) - month(&EXPS_START_DT)) + ifn(&baflag='AA',1,0) as RPM /* record pm*/;
%let computed_fields_used_by_grouping = RPM;
%let properties = /* t1.POLICY_ID, t1.COV_NO, */
                  &CM, &BAFLAG, t1.SEX_CODE, t1.PLAN_CODE, t1.assumption_class, t1.study_class, t1.paid_up_age,
                  t1.YRT_FLAG, t1.COV_TERM, t1.PRE_COV_TERM, t1.ISSUE_AGE , t1.PLAN_TERM, t1.IP_ind, &PM, &INNER_PM, &PY, &FULLY_OBSERVED_PY
                  ;

/* main part*/
  %make_exposure_table(start_month = 201501, stop_month = 201709, output=cache.EXPOSURE_MONTHLY_2015_2017)
```

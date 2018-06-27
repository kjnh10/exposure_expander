exposure_expander
===============
make exposure-time series table from the latest snapshot

# default calculated fields
You can access to the calculated fields below which vary depending on specific terms.
* &CM
* &CY
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
%include "<script directory>/calc_exposure.sas" /SOURCE2;

/* user parameter setting */
%let input = WORK.QUERY_FOR_TIME_SERIES_SAS7B_0001;
%let input = WORK.dev;
%let EXPS_START_DT = t1.ORIG_ISSUE_DATE;
%let EXPS_END_DT = t1.EXP_END_DATE;
%let EXTENDED_END_DT = t1.EXP_END_DATE; /* 観測対象のイベントが発生しているデータの観測期間単位の後ろまで伸ばした日 */
%let OBS_START_DT = MDY(1, 1, 2010);  /* fully observed pyの計算に使用される*/
%let OBS_END_DT = MDY(3, 31, 2018);  /* fully observed pyの計算に使用される*/
%let POL_START_DT = t1.ORIG_ISSUE_DATE;  /* for PY calculation */
%let EVENT_DT = IFN(t1.DEATH_DATE is not missing, t1.DEATH_DATE, .);
%let EVENT_AMOUNT = t1.DEATH_CLAIM_AMOUNT;
%let EX_FROM_COUNT = ifn(t1.TYPE= "DECREASE", 1, 0);
%let EXPS_AMOUNT = IFN(t1.TYPE= "DECREASE", t1.DEC_AMT, t1.AMT_FOR_EXPS)
                     * IFN(t1.IP_ind = 0, 1, present_value(t1.PREM_PMT_PERIOD*12 -  passedTime("month", &POL_START_DT, &TERM_START), IFN(t1.'Main Product'n = "UL", 5, 2), 0.0185, 0.01));

%let computed = IFN(0 < t1.FIRST_RENEW_DT <= t1.TERM_START and t1.YRT = 0, 1, 0) AS AFTER_RENEWAL;
%let computed_fields_used_by_grouping = AFTER_RENEWAL;
%let properties = /* t1.POLICY_ID, t1.COV_NO, */
                  &CY, &CM, t1.BAFLAG, t1.IM, t1.'UW-typ-CD'n,
                  t1.SEX_CODE, t1.PLAN_CODE, t1.'Main Product'n, t1.ISSUE_AGE, t1.CHANNEL_CODE, t1.PREM_PMT_PERIOD, t1.PLAN_TERM, t1.ORIG_TERM, t1.COV_TERM
                  ,t1.MULT_RATING_FCT
                  ;

/* main part*/
  %make_exposure_table(start_month = 201001, stop_month = 201709, output=WORK.EXP_MONTHLY)
  /* %make_exposure_table(start_month = 201001, stop_month = 201709, output=WORK.EXP_YEARLY, span=yearly) */
```

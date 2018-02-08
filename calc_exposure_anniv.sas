/* SET UP FUNCTIONS LIBRARY*/
  libname sasfunc "/mul/warehouse/jpnops/Experience_Analysis/library/2017/function/ver1.2";
  options cmplib = (sasfunc.misc sasfunc.IP sasfunc.time);

/* 固定した年に対してExposureを計算してsummaryを行う */
%macro Calc_Exposure(c_year, baflag);
    SELECT
    &properties , /* 分析属性 */

    /* 集計値 */
    sum(IFN(&EX_FROM_COUNT, 0, t1.EXPS_DUR)) AS EXPS_DUR,
    sum(t1.EXPS_DUR * &EXPS_AMOUNT) AS EXPS_AMOUNT,
    sum(t1.EVENT_BIT) AS EVENT_COUNT,
    sum(ifn(t1.TERM_START <= &EVENT_DT <= t1.TERM_END, &EVENT_AMOUNT, 0)) AS EVENT_AMOUNT
    
  /* 使いまわされそうな式をサブクエリで先に計算しておく。 */
    FROM (SELECT 
            *,
            "&baflag" AS BAFLAG,
            &c_year AS CY,
            COALESCE(MDY(MONTH(&POL_START_DT), DAY(&POL_START_DT), &c_year), MDY(2, 28, &c_year)) AS ANNIV_DT,
            IFN("&baflag" = "BA", MDY(1, 1, &c_year), (Calculated ANNIV_DT)) AS TERM_START,
            IFN("&baflag" = "BA", (Calculated ANNIV_DT) - 1, MDY(12, 31, &c_year)) AS TERM_END,
            (Calculated CY) - YEAR(&POL_START_DT) + IFN("&baflag" = "BA", 0, 1) AS PY,
            IFN((Calculated TERM_START) <= &EVENT_DT <= (Calculated TERM_END), 1, 0) AS EVENT_BIT,
            max(0, min(&EXPS_END_DT, (Calculated TERM_END)) - max(&EXPS_START_DT, (Calculated TERM_START)) + 1)/365.25 AS EXPS_DUR
            &computed
          FROM &input as t1
          WHERE ((&EXPS_START_DT <= MDY(12, 31, &c_year)) AND (MDY(1, 1, &c_year) <= &EXPS_END_DT))
                       or
                (&EVENT_DT is not missing))
    as t1
    GROUP BY CY, &properties
    HAVING MAX(EXPS_AMOUNT, EVENT_AMOUNT) > 0
%mend Calc_Exposure;

/* userからマクロ変数で呼び出せるようにailiasを貼る。 */
  %let BAFLAG = t1.BAFLAG;
  %let MONTHIV_DT = t1.MONTHIV_DT;
  %let TERM_START = t1.TERM_START;
  %let TERM_END = t1.TERM_END;
  %let EXPS_DUR = t1.EXPS_DUR;
  %let CY = t1.CY;
  %let PY = t1.PY;

/* loop part */
%macro yearly_loop(start_year, stop_year);
  %local yyyy;
  %do yyyy=&start_year %to &stop_year;
    insert into &output
    %Calc_Exposure(c_year=&yyyy, baflag=BA);

    insert into &output
    %Calc_Exposure(c_year=&yyyy, baflag=AA);
  %end;
%mend yearly_loop;

/* main part*/
%macro make_exposure_table(start_year, stop_year, output);
  %_eg_conditional_dropds(&output,tmp);

  PROC SQL;
    create table tmp as %Calc_Exposure(c_year=2010, baflag=BA); /* templateの作成。いつ時点を使ってもよい。 */
	  create table &output like tmp;
	  drop table tmp;
	  %yearly_loop(start_year=&start_year, stop_year=&stop_year)
  QUIT;
%mend make_exposure_table;


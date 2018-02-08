/* SET UP FUNCTIONS LIBRARY*/
libname sasfunc "/mul/warehouse/jpnops/Experience_Analysis/library/2017/function/ver1.2";
options cmplib = (sasfunc.misc sasfunc.IP sasfunc.time);

/* c_year, c_month, baflagに対してExposureを計算してsummaryを行うマクロ */
%macro Calc_Exposure(c_year, c_month, baflag);
   SELECT
    /* 分析属性 */
    &properties,
    &computed %if not %sysevalf(%superq(computed)=,boolean) %then , ;  /* ifはカンマのため */

    /* 集計値 */
    sum(IFN(&EX_FROM_COUNT, 0, 1)) AS EXPS_COUNT,
    sum(IFN(&EX_FROM_COUNT, 0, t1.EXPS_DUR)) AS EXPS_DUR,
    sum(&EXPS_AMOUNT) AS EXPS_AMOUNT,
    sum(&EXPS_AMOUNT * t1.EXPS_DUR) AS EXPS_DUR_AMOUNT,
    sum(t1.EVENT_BIT) AS EVENT_COUNT,
    sum(t1.EVENT_AMOUNT) AS EVENT_AMOUNT
    
    /* 使いまわされそうな式をサブクエリで先に計算しておく。Userはこのサブクエリの結果に&をつけてアクセス出来るようにする */
    FROM (SELECT 
            *,
            "&baflag" AS BAFLAG,
            (&c_year * 100 + &c_month) AS CM,
            COALESCE(MDY(&c_month, DAY(&POL_START_DT), &c_year), INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)-1) AS MONTHIV_DT,
            IFN("&baflag" = "BA", MDY(&c_month, 1, &c_year), (Calculated MONTHIV_DT)) AS TERM_START,
            IFN("&baflag" = "BA", (Calculated MONTHIV_DT) - 1, INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)-1) AS TERM_END,
            IFN((Calculated TERM_START) <= &EVENT_DT <= (Calculated TERM_END), 1, 0) AS EVENT_BIT,
            IFN((Calculated TERM_START) <= &EVENT_DT <= (Calculated TERM_END), &EVENT_AMOUNT, 0) AS EVENT_AMOUNT,
            max(0, min(&EXPS_END_DT, (Calculated TERM_END)) - max(&EXPS_START_DT, (Calculated TERM_START)) + 1)/365.25 AS EXPS_DUR
          FROM &input as t1
          WHERE ((&EXPS_START_DT <= INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)) AND (MDY(&c_month, 1, &c_year) <= &EXPS_END_DT))
                       or
                (&EVENT_DT is not missing))
    as t1

    where (EXPS_DUR * &EXPS_AMOUNT > 0) or (EVENT_AMOUNT > 0)
    GROUP BY  &properties
              %if not %sysevalf(%superq(computed_fields_used_by_grouping)=,boolean) %then , ; &computed_fields_used_by_grouping
%mend Calc_Exposure;


/* userからマクロ変数で呼び出せるようにailiasを貼る。 */
%let BAFLAG = t1.BAFLAG;
%let MONTHIV_DT = t1.MONTHIV_DT;
%let TERM_START = t1.TERM_START;
%let TERM_END = t1.TERM_END;
%let EXPS_DUR = t1.EXPS_DUR;
%let CM = t1.CM;

%macro monthly_loop(start_month, stop_month);
  %local yyyymm;
  %do yyyymm=&start_month %to &stop_month;
    %let year = %substr(&yyyymm, 1, 4);
    %let month = %substr(&yyyymm, 5, 2);

    insert into &output
    %Calc_Exposure(c_year=&year, c_month=&month, baflag=BA);
    insert into &output
    %Calc_Exposure(c_year=&year, c_month=&month, baflag=AA);

    /* SPECIAL CASE WHEN WE REACH END OF A YEAR*/
    %if &month = 12 %then %let yyyymm = %eval(&yyyymm + 88);     
  %end;
%mend monthly_loop;


/* main part*/
%macro make_exposure_table(start_month, stop_month, output);
  %_eg_conditional_dropds(&output,tmp);

  PROC SQL;
    create table tmp as %Calc_Exposure(c_year=2010, c_month=12, baflag=BA); /* templateの作成。いつ時点を使ってもよい。 */
    create table &output like tmp;
    drop table tmp;
    %monthly_loop(start_month=&start_month, stop_month=&stop_month)
  QUIT;
%mend make_exposure_table;



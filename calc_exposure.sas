%macro Calc_Exposure(c_year, c_month, baflag);
   /* c_year, c_month, baflagに対してExposureを計算してsummaryを行う関数 */

   SELECT
    /* 分析属性 */
    &properties,

    /* custom field: 分析属性の追加にも集計値の追加にも利用できる */
    &computed %if not %sysevalf(%superq(computed)=,boolean) %then , ;

    /* 集計値 */
    sum(IFN(&EX_FROM_COUNT, 0, t1.EXPS_DUR)) AS EXPS_DUR,
    sum(&EXPS_AMOUNT * t1.EXPS_DUR) AS EXPS_DUR_AMOUNT,
    sum(IFN(&EX_FROM_COUNT, 0, IFN(t1.EXPS_DUR>0, 1, 0))) AS EXPS_COUNT,  /* 内部のifはextendedされた部分ではcountが立つのを避けるため。 */
    sum(IFN(t1.EXPS_DUR>0, &EXPS_AMOUNT, 0)) AS EXPS_AMOUNT,
    sum(IFN(&EX_FROM_COUNT, 0, t1.EXTENDED_DUR)) AS EXTENDED_DUR,
    sum(&EXPS_AMOUNT * t1.EXTENDED_DUR) AS EXTENDED_DUR_AMOUNT,
    sum(t1.EVENT_BIT_IN_TERM) AS EVENT_COUNT,
    sum(IFN(t1.EVENT_BIT_IN_TERM=1, &EVENT_AMOUNT, 0)) AS EVENT_AMOUNT
    /*
    下記の使いまわされそうな式をサブクエリで先に計算しておく。
    Userは&properties, &computedの中でこのサブクエリの結果にアクセス出来る。
      input fieldのすべてのフィールド
      BAFLAG
      CY
      CM
      ANNIV_DT
      TERM_START
      TERM_END
      PM
      INNER_PM
      PY
      EVENT_BIT_IN_TERM,
      EVENT_AMOUNT_IN_TERM,
      EXPS_DUR,
      EXTENDED_DUR,
      PY_START_DT,
      PY_END_DT,
      FULLY_OBSERVED_PY

    Userはこのサブクエリ内で計算される列の挙動を下記のマクロ変数によりコントロール出来る。
      &POL_START_DT    : PY, PM計算の際の始点になる。
      &EXPS_START_DT   :
      &EXPS_END_DT     :
      &EXTENDED_END_DT : 観測対象のイベントが発生しているデータの観測期間単位の後ろまで伸ばした日。t1.EXTENDED_DUR, t1.EXTENDED_DUR_AMOUNTの計算に使用される。
      &EVENT_DT        :
      &EVENT_AMOUNT    :
      &EXPS_AMOUNT     :
      &OBS_START_DT    : t1.fully_observed_pyの計算に使用される
      &OBS_END_DT      : t1.fully_observed_pyの計算に使用される
      &EX_FROM_COUNT   : 減額レコードのようにexposureとしてamountは計上したいが、countは計上したくないものには1をたてる。
    */
    FROM (SELECT
            *,
            "&baflag" AS BAFLAG,
            &c_year AS CY,
            &c_month AS CM,

            /* monthlyかyearlyかによりロジックが変わる部分 */
            %if &g_span=monthly %then
              COALESCE(MDY(&c_month, DAY(&POL_START_DT), &c_year), INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)-1) AS ANNIV_DT,
              IFN("&baflag" = "BA", MDY(&c_month, 1, &c_year), (Calculated ANNIV_DT)) AS TERM_START,
              IFN("&baflag" = "BA", (Calculated ANNIV_DT) - 1, INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)-1) AS TERM_END,
              (year(Calculated TERM_START) - year(&POL_START_DT))*12 + (month(Calculated TERM_START) - month(&POL_START_DT)) + ifn((Calculated BAFLAG)='AA',1,0) as PM,
              mod((Calculated PM) - 1, 12) + 1 as INNER_PM,
              ceil((Calculated PM) / 12) as PY,
            ;

            %if &g_span=yearly %then
              COALESCE(MDY(MONTH(&POL_START_DT), DAY(&POL_START_DT), &c_year), MDY(2, 28, &c_year)) AS ANNIV_DT,
              IFN("&baflag" = "BA", MDY(1, 1, &c_year), (Calculated ANNIV_DT)) AS TERM_START,
              IFN("&baflag" = "BA", (Calculated ANNIV_DT) - 1, MDY(12, 31, &c_year)) AS TERM_END,
              -1 as PM,
              -1 as INNER_PM,
              (year(Calculated TERM_START) - year(&POL_START_DT)) + ifn((Calculated BAFLAG)='AA',1,0) as PY,
            ;
            %if &g_span=FY_yearly %then
              COALESCE(MDY(MONTH(&POL_START_DT), DAY(&POL_START_DT), &c_year + IFN(month(&POL_START_DT) in (1,2,3), 1, 0)), MDY(2, 28, &c_year+IFN(month(&POL_START_DT) in (1,2,3), 1, 0))) AS ANNIV_DT,
              IFN("&baflag" = "BA", MDY(4, 1, &c_year), (Calculated ANNIV_DT)) AS TERM_START,
              IFN("&baflag" = "BA", (Calculated ANNIV_DT) - 1, MDY(3, 31, &c_year+1)) AS TERM_END,
              -1 as PM,
              -1 as INNER_PM,
              (year(Calculated TERM_START) - year(&POL_START_DT)) + ifn((Calculated BAFLAG)='AA',1,0) as PY,
            ;
            /* */
            IFN((Calculated TERM_START) <= &g_EVENT_DT <= (Calculated TERM_END), IFN(&EVENT_AMOUNT>0, 1, 0), 0) AS EVENT_BIT_IN_TERM,
            max(0, min(&g_EXPS_END_DT, (Calculated TERM_END)) - max(&g_EXPS_START_DT, (Calculated TERM_START)) + 1)/365.25 AS EXPS_DUR,
            max(0, min(&g_EXTENDED_END_DT, (Calculated TERM_END)) - max(&g_EXPS_START_DT, (Calculated TERM_START)) + 1)/365.25 AS EXTENDED_DUR,
            INTNX("YEAR", &POL_START_DT, (Calculated PY)-1, "sameday") AS PY_START_DT,
            INTNX("YEAR", &POL_START_DT, (Calculated PY), "sameday")-1 AS PY_END_DT,
            IFN((&OBS_START_DT <= (Calculated PY_START_DT) and (Calculated PY_END_DT) <= &OBS_END_DT), 1, 0) AS FULLY_OBSERVED_PY
          FROM &input as t1

          WHERE
            (&g_EVENT_DT is not missing)
            %if &g_span=monthly %then
              or ((&g_EXPS_START_DT <= INTNX("MONTH", MDY(&c_month, 1, &c_year), 1)) AND (MDY(&c_month, 1, &c_year) <= &g_EXPS_END_DT))
            ;%else
              or ((&g_EXPS_START_DT <= MDY(1, 1, &c_year+1)) AND (MDY(1, 1, &c_year) <= &g_EXPS_END_DT))
            ;
          )
    as t1

    where (EXPS_DUR>0 and &EXPS_AMOUNT<>0) or (EVENT_BIT_IN_TERM > 0) or (EXTENDED_DUR>0 and &EXPS_AMOUNT<>0)
    GROUP BY  &properties
              %if not %sysevalf(%superq(computed_fields_used_by_grouping)=,boolean) %then , ; &computed_fields_used_by_grouping
%mend Calc_Exposure;


%macro monthly_loop(start_month, stop_month);
  %local yyyymm;
  %do yyyymm=&start_month %to &stop_month;
    %let year = %substr(&yyyymm, 1, 4);
    %let month = %substr(&yyyymm, 5, 2);

    insert into &output
    %Calc_Exposure(c_year=&year, c_month=&month, baflag=BA);
    insert into &output
    %Calc_Exposure(c_year=&year, c_month=&month, baflag=AA);

    /* Special case when we reach end of a year*/
    %if &month = 12 %then %let yyyymm = %eval(&yyyymm + 88);
  %end;
%mend monthly_loop;


%macro yearly_loop(start_year, stop_year);
  %do year=&start_year %to &stop_year;
    insert into &output
    %Calc_Exposure(c_year=&year, c_month=-1, baflag=BA);
    insert into &output
    %Calc_Exposure(c_year=&year, c_month=-1, baflag=AA);
  %end;
%mend yearly_loop;


/* main part: userはこのマクロ関数を呼び出す */
%macro make_exposure_table(start_month, stop_month, output, span=monthly);
  %_eg_conditional_dropds(&output,tmp);

  %let start_dt = mdy(mod(&start_month,100), 1, &start_month/100);
  %let end_dt = intnx('MONTH', mdy(mod(&stop_month,100), 1, &stop_month/100), 1)-1;
  %global g_span;
  %let g_span=&span;

  %global g_EXPS_START_DT;
  %let g_EXPS_START_DT = ifn(
                              &EXPS_START_DT is missing,
                              0, /* 欠損値の場合にexps_dur, exps_dur_amountを0にするため。exps_durの計算式内にてmin, maxが使用されているがこれらが欠損値を無視するためこの対応をしておく。*/
                              max(&EXPS_START_DT, &start_dt)  /* span = yeary を指定した場合は余計な期間が入らないように端をカットしておく */
                            );

  %global g_EXPS_END_DT;
  %let g_EXPS_END_DT = ifn(
                            &EXPS_END_DT is missing,
                            0,
                            min(&EXPS_END_DT, &end_dt)
                          );

  %global g_EXTENDED_END_DT;
  %let g_EXTENDED_END_DT = ifn(
                            &EXTENDED_END_DT is missing,
                            0,
                            min(&EXTENDED_END_DT, &end_dt)
                          );

  %global g_EVENT_DT;
  %let g_EVENT_DT = ifn(&start_dt <= &EVENT_DT <= &end_dt, &EVENT_DT, .);

  PROC SQL;
    %if &g_span=monthly %then
      create table tmp as %Calc_Exposure(c_year=2010, c_month=12, baflag=BA);
    %else
      create table tmp as %Calc_Exposure(c_year=2010, c_month=-1, baflag=BA);  /* yearlyとmonthlyで現在は同一のテーブルが生成されるはずだが念のため分けておく。*/
    ;
    create table &output like tmp;
    drop table tmp;

    %if &g_span=monthly %then
      %monthly_loop(start_month=&start_month, stop_month=&stop_month);
    %else
      %yearly_loop(start_year=%substr(&start_month, 1, 4), stop_year=%substr(&stop_month, 1, 4));
  QUIT;
%mend make_exposure_table;

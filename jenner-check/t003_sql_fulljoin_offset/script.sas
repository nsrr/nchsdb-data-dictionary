*******************************************************************************;
* Derived from scripts/prepare-nchsdb-for-nsrr.sas (nsrr/nchsdb-data-dictionary).
* Stage: full-join the BMI measurements to the sleep studies by patient, then
*        compute the day offset between each measurement and the sleep study.
*
* The two source tables are built inline here. The PROC SQL FULL JOIN and the
* offset arithmetic (abs of the datetime difference scaled to days) are
* unchanged from upstream.
*******************************************************************************;

  data measurement_bmi;
    infile datalines dsd;
    length MEAS_SOURCE $8;
    input STUDY_PAT_ID MEAS_RECORDED_DATETIME MEAS_SOURCE $ BMI;
    datalines;
1,1930608000,EHR,17.2
2,1929000000,EHR,21.5
3,1931400000,EHR,15.8
4,1930100000,EHR,19.1
;
  run;

  data sleep_study_in;
    infile datalines dsd;
    input STUDY_PAT_ID SLEEP_STUDY_ID AGE_AT_SLEEP_STUDY_DAYS
          SLEEP_STUDY_START_DATETIME;
    datalines;
1,101,3650,1930694400
2,102,5475,1929052800
3,103,2920,1931472000
4,104,4380,1930176000
5,105,3285,1930262400
;
  run;

  *making merge dataset of study id and sleep start;
  *bmi;
  proc sql;
    create table measurement_bmi_sleep_in as
    select * from measurement_bmi a full join sleep_study_in b
    on a.STUDY_PAT_ID = b.STUDY_PAT_ID;
  quit;

  data measurement_bmi_sleep;
    set measurement_bmi_sleep_in;

    *compute the day offset between sleep study date and date of measurement;
    bmi_offset_sort = abs(MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME);
    bmi_offset_sec = MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME;
    bmi_offset_min = bmi_offset_sec/60;
    bmi_offset_hour = bmi_offset_min/60;
    bmi_offset_days = bmi_offset_hour/24;
    bmi_offset = bmi_offset_days;

    *remove sleep studies without BMI;
    if BMI = . then delete;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      BMI
      MEAS_RECORDED_DATETIME
      SLEEP_STUDY_START_DATETIME
      MEAS_SOURCE
      bmi_offset_sort
      bmi_offset
      ;
  run;

  proc print data=measurement_bmi_sleep;
  run;

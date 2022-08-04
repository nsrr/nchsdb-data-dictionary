*******************************************************************************;
* Program           : prepare-nchsdb-for-nsrr.sas
* Project           : National Sleep Research Resource (sleepdata.org)
* Author            : Michael Rueschman (mnr)
* Date Created      : 20210701
* Purpose           : Prepare NCHSDB data for posting on NSRR.
*******************************************************************************;

*******************************************************************************;
* establish options and libnames ;
*******************************************************************************;
  options nofmterr;
  data _null_;
    call symput("sasfiledate",put(year("&sysdate"d),4.)||put(month("&sysdate"d),z2.)||put(day("&sysdate"d),z2.));
  run;

  *project source datasets;
  libname nchsdbs "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source";

  *output location for nsrr sas datasets;
  libname nchsdbd "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_datasets";
  libname nchsdba "\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_archive";

  *set data dictionary version;
  %let version = 0.3.0.pre;

  *set nsrr csv release path;
  %let releasepath = \\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_releases;

*******************************************************************************;
* create core dataset ;
*******************************************************************************;
  proc import datafile="\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source\DEMOGRAPHIC.csv"
    out=demographic_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  data demographic;
    set demographic_in;
  run;

  proc sort data=demographic;
    by STUDY_PAT_ID;
  run;

  proc import datafile="\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source\SLEEP_STUDY.CSV"
    out=sleep_study_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

  data sleep_study;
    set sleep_study_in;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      AGE_AT_SLEEP_STUDY_DAYS
      ;
  run;

  proc sort data=sleep_study;
    by STUDY_PAT_ID SLEEP_STUDY_ID AGE_AT_SLEEP_STUDY_DAYS;
  run;

  proc sort data=sleep_study nodupkey;
    by STUDY_PAT_ID SLEEP_STUDY_ID;
  run;

  proc import datafile="\\rfawin\BWH-SLEEPEPI-NSRR-STAGING\20210809-nchsdb\nsrr-prep\_source\MEASUREMENT.CSV"
    out=measurement_in
    dbms=csv
    replace;
    guessingrows=1000;
  run;

   data measurement;
    set measurement_in;
  run;

  proc sort data=measurement;
    by STUDY_PAT_ID;
  run;
  /*
  proc freq data=demographic;
    table pcori_hispanic_cd;
  run;
  */

  ***********************************************************************
* parsing measurement file and getting measurements closest to sleep data *
  ************************************************************************
* parsing out the measurement.csv file into smaller BMI, BMIPCT, and BP datasets. 
  In BP dataset, separating out systolic (top) and diastolic (bottom) BP;

  *bmi dataset;
  data measurement_bmi;
    set measurement; 
    where MEAS_TYPE="BMI";
    BMI = MEAS_VALUE_NUMBER;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BMI
      ;
  run;

  *bmipct dataset;
  data measurement_bmipct;
    set measurement; 
    where MEAS_TYPE="BMIPCT";
    BMIPCT = MEAS_VALUE_NUMBER;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BMIPCT
      ;
  run;

  *bp dataset;
  data measurement_bp;
    set measurement; 
    where MEAS_TYPE="BP";
    BP = MEAS_VALUE_TEXT;

    keep
      STUDY_PAT_ID
      MEAS_RECORDED_DATETIME
      MEAS_SOURCE
      BP
      ;
  run;

  *seperate bp bp_diastolic and bp_systolic;
  data measurement_bp_sep;
    set measurement_bp;
    bp_systolic = scan(BP,1,'/');
    bp_diastolic = scan(BP,2,'/');
  run;

  /* checking
  proc print data=measurement_bp_sep(obs=10) label;
  run;
  */

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

  *bmipct;
  proc sql;
    create table measurement_bmipct_sleep_in as
    select * from measurement_bmipct a full join sleep_study_in b
    on a.STUDY_PAT_ID = b.STUDY_PAT_ID;
  quit;

  data measurement_bmipct_sleep;
    set measurement_bmipct_sleep_in;

    bmipct_offset_sort = abs(MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME);
    bmipct_offset_sec = MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME;
    bmipct_offset_min = bmipct_offset_sec/60;
    bmipct_offset_hour = bmipct_offset_min/60;
    bmipct_offset_days = bmipct_offset_hour/24;
    bmipct_offset = bmipct_offset_days;

    *remove sleep studies without BMIPCT;
    if BMIPCT = . then delete;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      BMIPCT
      MEAS_RECORDED_DATETIME
      SLEEP_STUDY_START_DATETIME
      MEAS_SOURCE
      bmipct_offset_sort
      bmipct_offset
      ;
  run;

  *bp;
  proc sql;
    create table measurement_bp_sleep_in as
    select * from measurement_bp_sep a full join sleep_study_in b
    on a.STUDY_PAT_ID = b.STUDY_PAT_ID;
  quit;

  data measurement_bp_sleep;
    set measurement_bp_sleep_in;

    bp_offset_sort = abs(MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME);
    bp_offset_sec = MEAS_RECORDED_DATETIME - SLEEP_STUDY_START_DATETIME;
    bp_offset_min = bp_offset_sec/60;
    bp_offset_hour = bp_offset_min/60;
    bp_offset_days = bp_offset_hour/24;
    bp_offset = bp_offset_days;

    *remove sleep studies without BP;
    if BP = '' or bp_systolic = 0 or bp_diastolic = 0 then delete;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      bp_systolic
      bp_diastolic
      MEAS_RECORDED_DATETIME
      SLEEP_STUDY_START_DATETIME
      MEAS_SOURCE
      bp_offset_sort
      bp_offset
    ;
  run;
  
  *sort to select measure for each individual with smallest date offset from sleep study date;
  *bmi;
  proc sort data=measurement_bmi_sleep;
    by SLEEP_STUDY_ID bmi_offset_sort;
  run;

  *bmipct;
  proc sort data=measurement_bmipct_sleep;
    by SLEEP_STUDY_ID bmipct_offset_sort;
  run;

  *bp;
  proc sort data=measurement_bp_sleep;
    by SLEEP_STUDY_ID bp_offset_sort;
  run;

  /* checking
  proc print data=measurement_bmi_sleep(obs=10) label;
  run;

  proc print data=measurement_bmipct_sleep(obs=10) label;
  run;

  proc print data=measurement_bp_sleep(obs=10) label;
  run;
  */

  *only keep first row of each individual after sort;
  *bmi;
  data measurement_bmi_sleep_subset;
    set measurement_bmi_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bmi_offset_sort;
  run;

  *bmipct;
  data measurement_bmipct_sleep_subset;
    set measurement_bmipct_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bmipct_offset_sort;
  run;

  *bp;
  data measurement_bp_sleep_subset;
    set measurement_bp_sleep;
    by SLEEP_STUDY_ID;
    if first.SLEEP_STUDY_ID then output;
    drop bp_offset_sort;
  run;

  /* checking
  proc print data=measurement_bmi_sleep_subset(obs=10) label;
  run;

  proc print data=measurement_bmipct_sleep_subset(obs=10) label;
  run;

  proc print data=measurement_bp_sleep_subset(obs=10) label;
  run;

  *note: some IDs (i.e ID 28) have no BP data, so it is missing in final;
  */

  *re-combine the 3 datasets with 7 columns;
  *keeping MEAS_SOURCE in- may remove later; 
  data measurement_new;
    merge 
      measurement_bmi_sleep_subset
      measurement_bmipct_sleep_subset
      measurement_bp_sleep_subset
      ;
    by SLEEP_STUDY_ID;

    keep
      STUDY_PAT_ID
      SLEEP_STUDY_ID
      BMI
      bmi_offset
      BMIPCT
      bmipct_offset
      bp_systolic
      bp_diastolic
      bp_offset
    ;
  run;

  proc sort data=measurement_new;
    by STUDY_PAT_ID SLEEP_STUDY_ID;
  run;

  /*
  proc print data=measurement_new(obs=10) label;
  run;
  */

  *merge these into the 'nchsdb_nsrr' dataset;
  data nchsdb_nsrr_in;
    merge
      sleep_study
      measurement_new
      ;
    by study_pat_id sleep_study_id;
  run;

  proc sort data=nchsdb_nsrr_in;
    by study_pat_id age_at_sleep_study_days;
  run;

  data nchsdb_nsrr;
    length study_pat_id sleep_study_id 8. filename_id $20.;
    merge
      demographic
      nchsdb_nsrr_in
      ;
    by study_pat_id;

    *create filename indicator;
    filename_id = compress(put(study_pat_id,8.)) || "_" || compress(put(sleep_study_id,8.));

    *create encounter variable for Spout to use for graph generation;
    retain encounter;
    if first.study_pat_id then encounter = 1;
    else encounter = encounter + 1;
  run;

  /*

  proc freq data=nchsdb_nsrr;
    table encounter;
  run;

  */

*******************************************************************************;
* create harmonized dataset ;
*******************************************************************************;

  data nchsdb_harmonized;
    set nchsdb_nsrr;

    *demographics;
    *age;
    *use age_at_sleep_study_days and divide by 365.25;
    format nsrr_age 8.2;
    nsrr_age = age_at_sleep_study_days / 365.25; 

    *age_gt89;
    *use age_at_sleep_study_days;
    format nsrr_age_gt89 $10.; 
    if age_at_sleep_study_days / 365.25 gt 89 then nsrr_age_gt89='yes';
    else if age_at_sleep_study_days / 365.25 le 89 then nsrr_age_gt89='no';

    *sex;
    *use pcori_gender_cd;
    format nsrr_sex $15.;
    if pcori_gender_cd = 'F' then nsrr_sex = 'female';
    else if pcori_gender_cd = 'M' then nsrr_sex = 'male';
    else if pcori_gender_cd = 'UN' then nsrr_sex = 'not reported';

    *race;
    *use pcori_race_cd;
    format nsrr_race $100.;
    if pcori_race_cd = '01' then nsrr_race = 'american indian or alaska native';
    else if pcori_race_cd = '02' then nsrr_race = 'asian';
    else if pcori_race_cd = '03' then nsrr_race = 'black or african american';
    else if pcori_race_cd = '04' then nsrr_race = 'native hawaiian or other pacific islander'; 
    else if pcori_race_cd = '05' then nsrr_race = 'white';
    else if pcori_race_cd = '06' then nsrr_race = 'multiple';
    else if pcori_race_cd = '07' then nsrr_race = 'not reported';
    else if pcori_race_cd = 'UN' then nsrr_race = 'unknown';

    *ethnicity;
    *use pcori_hispanic_cd;
    format nsrr_ethnicity $100.;
    if pcori_hispanic_cd = 'N' then nsrr_ethnicity = 'not hispanic or latino';
    else if pcori_hispanic_cd = 'Y' then nsrr_ethnicity = 'hispanic or latino';
    else if pcori_hispanic_cd = 'NI' then nsrr_ethnicity = 'not reported';
    else if pcori_hispanic_cd = 'UN' then nsrr_ethnicity = 'unknown';

    *anthropometry
    *bmi;
    *use BMI;
    format nsrr_bmi 10.9;
    nsrr_bmi = BMI;
  
    *bmipct;
    *use BMIPCT;
    format nsrr_bmipct 10.9;
    nsrr_bmipct = BMIPCT;

    *clinical data/vital signs
    *bp_systolic;
    *use bp_systolic;
    format nsrr_bp_systolic 10.9;
    nsrr_bp_systolic = bp_systolic;

    *bp_diastolic;
    *use bp_diastolic;
    format nsrr_bp_diastolic 10.9;
    nsrr_bp_diastolic = bp_diastolic;
  
    *lifestyle and behavioral health
    *none;
    keep
      study_pat_id
      sleep_study_id
      filename_id
      encounter
      nsrr_age 
      nsrr_age_gt89
      nsrr_sex 
      nsrr_ethnicity
      nsrr_race
      nsrr_bmi
      nsrr_bmipct
      nsrr_bp_systolic
      nsrr_bp_diastolic
      ;
  run;

*******************************************************************************;
* checking harmonized datasets ;
*******************************************************************************;

  /* Checking for extreme values for continuous variables */

  proc means data=nchsdb_harmonized;
    VAR   
    nsrr_age
    nsrr_bmi
    nsrr_bp_systolic
    nsrr_bp_diastolic;
  run;

  /* Checking categorical variables */
  
  proc freq data=nchsdb_harmonized;
    table   
    nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_ethnicity;
  run;

  /* checking
  proc print data=nchsdb_harmonized(obs=10) label;
  run;

  proc print data=nchsdb_harmonized(obs=10) label;
    where nsrr_bp_systolic = .;
  run;
  */

*******************************************************************************;
* make all variable names lowercase ;
*******************************************************************************;
  options mprint;
  %macro lowcase(dsn);
       %let dsid=%sysfunc(open(&dsn));
       %let num=%sysfunc(attrn(&dsid,nvars));
       %put &num;
       data &dsn;
             set &dsn(rename=(
          %do i = 1 %to &num;
          %let var&i=%sysfunc(varname(&dsid,&i));    /*function of varname returns the name of a SAS data set variable*/
          &&var&i=%sysfunc(lowcase(&&var&i))         /*rename all variables*/
          %end;));
          %let close=%sysfunc(close(&dsid));
    run;
  %mend lowcase;

  %lowcase(nchsdb_nsrr);
  %lowcase(nchsdb_harmonized);

  /*

  proc contents data=nchsdb_nsrr out=nchsdb_nsrr_contents;
  run;

  */

*******************************************************************************;
* create permanent sas datasets ;
*******************************************************************************;
  data nchsdbd.nchsdb_nsrr nchsdba.nchsdb_nsrr_&sasfiledate;
    set nchsdb_nsrr;
  run;

  data nchsdbd.nchsdb_harmonized nchsdba.nchsdb_harmonized_&sasfiledate;
    set nchsdb_harmonized;
  run;

*******************************************************************************;
* export nsrr csv datasets ;
*******************************************************************************;
  proc export data=nchsdb_nsrr
    outfile="&releasepath\&version\nchsdb-dataset-&version..csv"
    dbms=csv
    replace;
  run;

  proc export data=nchsdb_harmonized
    outfile="&releasepath\&version\nchsdb-dataset-harmonized-&version..csv"
    dbms=csv
    replace;
  run;
